use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh::protocol::Router;
use iroh::{Endpoint, NodeId};
use iroh_blobs::{
    net_protocol::Blobs,
    protocol::ALPN as BLOBS_ALPN,
    rpc::client::blobs::WrapOption,
    store::{ExportFormat, ExportMode},
    ticket::BlobTicket,
    util::SetTagOption,
    BlobFormat,
};
use iroh_gossip::{
    net::{Event, Gossip, GossipEvent, GOSSIP_ALPN},
    proto::TopicId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::signal;

// ---------------------------------------------------------------------------
// Fixed registry topic — shared by all Thorium nodes regardless of which
// files they hold.  Any node on the network can bootstrap any other node
// using this topic, without needing to know any specific file hash.
//
// Topic = SHA-256("thorium-iroh-registry-v1") — same derivation in TypeScript.
// ---------------------------------------------------------------------------

fn registry_topic() -> TopicId {
    let hash = Sha256::digest(b"thorium-iroh-registry-v1");
    TopicId::from_bytes(hash.into())
}

// ---------------------------------------------------------------------------
// Gossip wire protocol — must match gossip.ts protocol v1
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum GossipMsg {
    /// "I hold this blob at this address."
    #[serde(rename = "HAVE")]
    Have {
        v: u8,
        #[serde(rename = "nodeId")]
        node_id: String,
        hash: String,
        #[serde(rename = "relayUrl")]
        relay_url: Option<String>,
    },
    /// "Who has this blob?"
    #[serde(rename = "WANT")]
    Want { v: u8, hash: String },
}

impl GossipMsg {
    fn have(node_id: &str, hash: &str, relay_url: Option<String>) -> Bytes {
        let msg = GossipMsg::Have {
            v: 1,
            node_id: node_id.to_string(),
            hash: hash.to_string(),
            relay_url,
        };
        serde_json::to_vec(&msg).unwrap_or_default().into()
    }

    fn hash_str(&self) -> Option<&str> {
        match self {
            GossipMsg::Have { hash, .. } => Some(hash),
            GossipMsg::Want { hash, .. } => Some(hash),
        }
    }
}

/// Expose a local file on the IROH P2P network.
///
/// Usage:
///   provide <path/to/file>    — shares the file and prints a ticket
///   get <ticket> [output]     — downloads a file using a ticket

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("thorium_iroh_p2p=info".parse()?)
                .add_directive("iroh=warn".parse()?)
                .add_directive("iroh_gossip=warn".parse()?),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("provide") => {
            let path = args.get(2).context("Usage: provide <path/to/file>")?;
            provide(PathBuf::from(path)).await
        }
        Some("get") => {
            let ticket_str = args.get(2).context("Usage: get <ticket> [output]")?;
            let output = args
                .get(3)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("output"));
            get(ticket_str, output).await
        }
        _ => {
            eprintln!("Thorium IROH P2P — share files over the IROH network");
            eprintln!();
            eprintln!("Commands:");
            eprintln!("  provide <path/to/file>       Share a file and print a download ticket");
            eprintln!("  get <ticket> [output]        Download a file using a ticket");
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// provide
// ---------------------------------------------------------------------------

/// Shares a file and participates in TWO gossip topics:
///
///   1. Registry topic (fixed, SHA-256("thorium-iroh-registry-v1"))
///      ─ All Thorium nodes subscribe to this on startup.
///      ─ Used for peer discovery: any node can find who has any file
///        just by joining the registry via any known Thorium node.
///      ─ This node broadcasts HAVE {hash} here so any TypeScript node
///        that joins the registry learns about this file.
///
///   2. Per-file topic (hash bytes of the blob)
///      ─ Nodes that have or want this specific file join here.
///      ─ Used as fast path once peers already know each other.
///      ─ When both M1 and M2 join via this node, they become direct
///        gossip neighbours and survive this node going offline.
async fn provide(path: PathBuf) -> Result<()> {
    if !path.exists() {
        bail!("File not found: {}", path.display());
    }

    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();

    println!("Starting IROH node...");

    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let gossip = Gossip::builder().spawn(endpoint.clone()).await?;
    let blobs = Blobs::memory().build(&endpoint);

    let _router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .accept(BLOBS_ALPN, blobs.clone())
        .spawn();

    let client = blobs.client();

    println!("Adding file: {}", path.display());

    let outcome = client
        .add_from_path(path, false, SetTagOption::Auto, WrapOption::NoWrap)
        .await?
        .finish()
        .await?;

    let node_addr = endpoint.node_addr().await?;
    let node_id_str = node_addr.node_id.to_string();
    let relay_url = node_addr.relay_url().map(|u| u.to_string());
    let hash_str = outcome.hash.to_string();
    let ticket = BlobTicket::new(node_addr.clone(), outcome.hash, BlobFormat::Raw)?;

    // Pre-build our HAVE message (reused on both topics and for WANT replies).
    let have_bytes = GossipMsg::have(&node_id_str, &hash_str, relay_url);

    // ── Topic 1: Registry (fixed, always-on) ────────────────────────────────
    let registry = registry_topic();
    let mut reg_sub = gossip.subscribe(registry, vec![])?;
    reg_sub.broadcast(have_bytes.clone()).await?;
    println!("[registry] joined — broadcasting HAVE for {}", &hash_str[..12]);

    // ── Topic 2: Per-file (blob hash bytes) ──────────────────────────────────
    let blob_topic = TopicId::from_bytes(*outcome.hash.as_bytes());
    let mut blob_sub = gossip.subscribe(blob_topic, vec![])?;
    blob_sub.broadcast(have_bytes.clone()).await?;
    println!("[blob]     joined topic for {}", &hash_str[..12]);

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  {} is live on IROH", file_name);
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("Hash:     {}", hash_str);
    println!("Ticket:   {}", ticket);
    println!("Node ID:  {}", node_id_str);
    println!();
    println!("Bootstrap this node's ID in any Thorium peer to make it");
    println!("the permanent rendezvous point for this network.");
    println!("Press Ctrl+C to stop serving.");
    println!();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("Shutting down...");
                break;
            }

            // ── Registry events ──────────────────────────────────────────────
            event = reg_sub.try_next() => {
                match event? {
                    Some(Event::Gossip(GossipEvent::NeighborUp(peer))) => {
                        println!("[registry] peer joined: {}", fmt_short(&peer));
                        // Re-announce so the new peer knows about this file.
                        reg_sub.broadcast(have_bytes.clone()).await?;
                    }
                    Some(Event::Gossip(GossipEvent::NeighborDown(peer))) => {
                        println!("[registry] peer left: {}", fmt_short(&peer));
                    }
                    Some(Event::Gossip(GossipEvent::Received(msg))) => {
                        if let Ok(parsed) = serde_json::from_slice::<GossipMsg>(&msg.content) {
                            if let GossipMsg::Want { hash, .. } = &parsed {
                                if *hash == hash_str {
                                    println!("[registry] WANT received — replying HAVE");
                                    reg_sub.broadcast(have_bytes.clone()).await?;
                                }
                            }
                        }
                    }
                    Some(_) | None => {}
                }
            }

            // ── Per-file topic events ────────────────────────────────────────
            event = blob_sub.try_next() => {
                match event? {
                    Some(Event::Gossip(GossipEvent::NeighborUp(peer))) => {
                        println!("[blob] peer joined swarm: {}", fmt_short(&peer));
                        blob_sub.broadcast(have_bytes.clone()).await?;
                    }
                    Some(Event::Gossip(GossipEvent::NeighborDown(peer))) => {
                        println!("[blob] peer left swarm: {}", fmt_short(&peer));
                    }
                    Some(Event::Gossip(GossipEvent::Received(msg))) => {
                        if let Ok(parsed) = serde_json::from_slice::<GossipMsg>(&msg.content) {
                            if parsed.hash_str() == Some(&hash_str) {
                                if let GossipMsg::Want { .. } = parsed {
                                    println!("[blob] WANT received — replying HAVE");
                                    blob_sub.broadcast(have_bytes.clone()).await?;
                                }
                            }
                        }
                    }
                    Some(_) | None => {}
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

async fn get(ticket_str: &str, output: PathBuf) -> Result<()> {
    let ticket: BlobTicket = ticket_str
        .parse()
        .context("Invalid ticket — paste the full ticket string printed by 'provide'")?;

    println!("Starting IROH node...");

    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = Blobs::memory().build(&endpoint);
    let _router = Router::builder(endpoint)
        .accept(BLOBS_ALPN, blobs.clone())
        .spawn();

    let client = blobs.client();

    println!("Connecting to peer: {}", ticket.node_addr().node_id);
    println!("Downloading...");

    client
        .download(ticket.hash(), ticket.node_addr().clone())
        .await?
        .finish()
        .await?;

    println!("Saving to: {}", output.display());

    client
        .export(ticket.hash(), output.clone(), ExportFormat::Blob, ExportMode::Copy)
        .await?
        .finish()
        .await?;

    println!("Done! Saved to: {}", output.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn fmt_short(node_id: &NodeId) -> String {
    node_id.to_string()[..12].to_string()
}
