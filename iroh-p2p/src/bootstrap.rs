/// Thorium IROH Bootstrap Node
///
/// A minimal always-on gossip rendezvous node.  It holds NO files — it is
/// purely a stable meeting point so that any Thorium peer on the network can
/// find every other peer regardless of which blob seeder is currently online.
///
/// The node's identity (nodeId) is derived from a secret key that is saved to
/// disk on first run and reloaded on every subsequent run, giving the node a
/// STABLE nodeId across restarts.
///
/// Usage:
///   thorium-iroh-bootstrap [--key-file <path>]
///
///   --key-file   Path to the persistent secret-key file.
///                Default: ./thorium-bootstrap.key
///                The file is created automatically on first run.
use anyhow::{Context, Result};
use futures_lite::StreamExt;
use iroh::{Endpoint, SecretKey};
use iroh_gossip::{
    net::{Event, Gossip, GossipEvent, GOSSIP_ALPN},
    proto::TopicId,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::signal;

// ---------------------------------------------------------------------------
// Registry topic — must match gossip.ts and main.rs
// ---------------------------------------------------------------------------

fn registry_topic() -> TopicId {
    let hash = Sha256::digest(b"thorium-iroh-registry-v1");
    TopicId::from_bytes(hash.into())
}

// ---------------------------------------------------------------------------
// Persistent secret key
// ---------------------------------------------------------------------------

/// Load an existing secret key from `path`, or generate and save a new one.
///
/// The key is stored as a 64-character lowercase hex string (32 raw bytes).
/// Keeping it as a plain text file makes it easy to back up or copy.
fn load_or_create_key(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let hex_str = std::fs::read_to_string(path)
            .with_context(|| format!("reading key file {}", path.display()))?;
        let bytes = hex::decode(hex_str.trim())
            .with_context(|| "decoding key file — expected 64 hex characters")?;
        let arr: [u8; 32] = bytes.try_into().map_err(|_| {
            anyhow::anyhow!("key file must contain exactly 32 bytes (64 hex chars)")
        })?;
        println!("Loaded existing key from {}", path.display());
        Ok(SecretKey::from_bytes(&arr))
    } else {
        let key = SecretKey::generate(rand::rngs::OsRng);
        let hex_str = hex::encode(key.to_bytes());
        std::fs::write(path, &hex_str)
            .with_context(|| format!("writing new key to {}", path.display()))?;
        println!("Generated new key, saved to {}", path.display());
        Ok(key)
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("thorium_iroh_bootstrap=info".parse()?)
                .add_directive("iroh=warn".parse()?)
                .add_directive("iroh_gossip=warn".parse()?),
        )
        .init();

    // Parse --key-file argument (default: ./thorium-bootstrap.key).
    let args: Vec<String> = std::env::args().collect();
    let key_file = args
        .windows(2)
        .find(|w| w[0] == "--key-file")
        .map(|w| PathBuf::from(&w[1]))
        .unwrap_or_else(|| PathBuf::from("thorium-bootstrap.key"));

    // Load or generate the persistent secret key.
    let secret_key = load_or_create_key(&key_file)?;

    println!("Starting Thorium IROH Bootstrap Node...");

    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery_n0()
        .bind()
        .await?;

    let gossip = Gossip::builder().spawn(endpoint.clone()).await?;

    // We do not serve blobs — register gossip only.
    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let node_addr = endpoint.node_addr().await?;

    // ── Print identity ────────────────────────────────────────────────────────
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Thorium Bootstrap Node — STABLE RENDEZVOUS                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Node ID : {}", node_addr.node_id);
    if let Some(relay) = node_addr.relay_url() {
        println!("Relay   : {relay}");
    }
    println!();
    println!("Set this in Thorium:");
    println!("  THORIUM_IROH_BOOTSTRAP_NODE_IDS={}", node_addr.node_id);
    println!();
    println!("The Node ID above is STABLE — it will be the same on every restart");
    println!("as long as '{}' is not deleted.", key_file.display());
    println!();
    println!("Press Ctrl+C to stop.");
    println!();

    // ── Registry subscription ─────────────────────────────────────────────────
    let topic = registry_topic();
    let mut sub = gossip.subscribe(topic, vec![])?;

    println!("[registry] subscribed to fixed topic — waiting for peers...");

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("Shutting down...");
                break;
            }
            event = sub.try_next() => {
                match event? {
                    Some(Event::Gossip(GossipEvent::NeighborUp(peer))) => {
                        println!("[registry] peer joined : {}", &peer.to_string()[..12]);
                    }
                    Some(Event::Gossip(GossipEvent::NeighborDown(peer))) => {
                        println!("[registry] peer left   : {}", &peer.to_string()[..12]);
                    }
                    Some(Event::Gossip(GossipEvent::Joined(peers))) => {
                        println!("[registry] joined swarm with {} peer(s)", peers.len());
                    }
                    // Forward all received messages back to the swarm so peers
                    // that joined later can still hear HAVE/WANT messages.
                    Some(Event::Gossip(GossipEvent::Received(_msg))) => {
                        // The gossip protocol handles forwarding automatically via
                        // Plumtree.  Nothing extra needed here for a relay node.
                    }
                    Some(_) | None => {}
                }
            }
        }
    }

    Ok(())
}
