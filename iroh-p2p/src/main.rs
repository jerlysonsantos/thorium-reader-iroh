use anyhow::{bail, Context, Result};
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::{
    net_protocol::Blobs,
    protocol::ALPN,
    rpc::client::blobs::WrapOption,
    store::{ExportFormat, ExportMode},
    ticket::BlobTicket,
    util::SetTagOption,
    BlobFormat,
};
use std::path::PathBuf;
use tokio::signal;

/// Expose a local PDF on the IROH P2P network.
///
/// Usage:
///   provide <path/to/file.pdf>   — shares the file and prints a ticket
///   get <ticket> [output.pdf]    — downloads a file using a ticket

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("thorium_iroh_p2p=info".parse()?)
                .add_directive("iroh=warn".parse()?),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("provide") => {
            let path = args.get(2).context("Usage: provide <path/to/file.pdf>")?;
            provide(PathBuf::from(path)).await
        }
        Some("get") => {
            let ticket_str = args.get(2).context("Usage: get <ticket> [output.pdf]")?;
            let output = args
                .get(3)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("output.pdf"));
            get(ticket_str, output).await
        }
        _ => {
            eprintln!("Thorium IROH P2P — share files over the IROH network");
            eprintln!();
            eprintln!("Commands:");
            eprintln!(
                "  provide <path/to/file.pdf>         Share a file and print a download ticket"
            );
            eprintln!("  get <ticket> [output.pdf]          Download a file using a ticket");
            Ok(())
        }
    }
}

/// Adds a file to the IROH network and waits until interrupted.
/// Prints the ticket peers need to download the file.
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

    // Build the network endpoint with IROH's n0 discovery (DHT-based peer discovery).
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;

    // In-memory blob store — data lives only while this process is running.
    // Replace with Blobs::persistent(data_dir) to survive restarts.
    let blobs = Blobs::memory().build(&endpoint);

    // Register the blobs protocol on the router so incoming connections are handled.
    let router = Router::builder(endpoint)
        .accept(ALPN, blobs.clone())
        .spawn();

    let client = blobs.client();

    println!("Adding file: {}", path.display());

    let outcome = client
        .add_from_path(
            path,
            false, // in_place: copy the file into the blob store
            SetTagOption::Auto,
            WrapOption::NoWrap,
        )
        .await?
        .finish()
        .await?;

    let node_addr = router.endpoint().node_addr().await?;
    let ticket = BlobTicket::new(node_addr, outcome.hash, BlobFormat::Raw)?;

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║  {} is live on IROH", file_name);
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("Hash:   {}", outcome.hash);
    println!("Ticket: {}", ticket);
    println!();
    println!("Share the ticket above with anyone who wants to download.");
    println!("Press Ctrl+C to stop serving.");
    println!();

    signal::ctrl_c().await?;
    println!("Shutting down...");
    router.shutdown().await?;

    Ok(())
}

/// Downloads a file from the IROH network using a ticket.
async fn get(ticket_str: &str, output: PathBuf) -> Result<()> {
    let ticket: BlobTicket = ticket_str
        .parse()
        .context("Invalid ticket — paste the full ticket string printed by 'provide'")?;

    println!("Starting IROH node...");

    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let blobs = Blobs::memory().build(&endpoint);
    let _router = Router::builder(endpoint)
        .accept(ALPN, blobs.clone())
        .spawn();

    let client = blobs.client();

    println!("Connecting to peer: {}", ticket.node_addr().node_id);
    println!("Downloading...");

    // Simple download: hash + node address from the ticket.
    client
        .download(ticket.hash(), ticket.node_addr().clone())
        .await?
        .finish()
        .await?;

    println!("Saving to: {}", output.display());

    client
        .export(
            ticket.hash(),
            output.clone(),
            ExportFormat::Blob,
            ExportMode::Copy,
        )
        .await?
        .finish()
        .await?;

    println!("Done! Saved to: {}", output.display());

    Ok(())
}
