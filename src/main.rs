//! Minecraft QUIC Tunnel v0.3
//! Using Iroh — free public QUIC relay servers by n0.computer. No VPS, no port forwarding.

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use std::io::{self, Write};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const ALPN: &[u8] = b"mc-quic-tunnel/0.3";

// ─── Bridge TCP ↔ QUIC ─────────────────────────────────────────────────────────

async fn bridge(conn: Connection, mc_port: u16) -> Result<(), std::io::Error> {
    loop {
        let (mut qs, mut qr) = conn
            .accept_bi()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        tokio::spawn(async move {
            let mc_addr = format!("127.0.0.1:{mc_port}");
            match TcpStream::connect(&mc_addr).await {
                Err(e) => eprintln!("  ✗ No Minecraft at {mc_addr}: {e}"),
                Ok(tcp) => {
                    let (mut tr, mut tw) = tcp.into_split();

                    let a = async {
                        let mut buf = vec![0u8; 65536];
                        loop {
                            match qr.read(&mut buf).await {
                                Ok(Some(0)) | Ok(None) => break,
                                Ok(Some(n)) => tw.write_all(&buf[..n]).await?,
                                Err(e) => {
                                    return Err(std::io::Error::other(e.to_string()));
                                }
                            }
                        }
                        tw.shutdown().await?;
                        Ok::<_, std::io::Error>(())
                    };

                    let b = async {
                        let mut buf = vec![0u8; 65536];
                        loop {
                            let n = tr.read(&mut buf).await?;
                            if n == 0 {
                                break;
                            }
                            qs.write_all(&buf[..n]).await?;
                        }
                        qs.finish()
                            .map_err(|e| std::io::Error::other(e.to_string()))?;
                        Ok::<_, std::io::Error>(())
                    };

                    let _ = tokio::try_join!(a, b);
                }
            }
        });
    }
}

// ─── Protocol handler (host is accepting connections) ────────────────────────────

#[derive(Debug, Clone)]
struct McTunnel {
    mc_port: u16,
}

impl ProtocolHandler for McTunnel {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let peer = conn.remote_id();
        println!("  🟢 Friend connected! (id: {}...)", &peer.to_string()[..12]);
        // std::io::Error implements From for AcceptError
        bridge(conn, self.mc_port).await.map_err(AcceptError::from)
    }
}

// ─── HOST mode ─────────────────────────────────────────────────────────────

async fn run_host(mc_port: u16) -> Result<()> {
    println!("  ⏳ Starting the tunnel...");

    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .context("Failed to create iroh endpoint")?;

    endpoint.online().await;

    let node_id = endpoint.id();
    let ticket = node_id.to_string();

    let router = Router::builder(endpoint)
        .accept(ALPN.to_vec(), McTunnel { mc_port })
        .spawn();

    println!();
    println!("  ✅ Tunnel is ready!");
    println!("  ─────────────────────────────────────────");
    println!("  🎟️  Your ticket:");
    println!();
    println!("      {ticket}");
    println!();
    println!("  📋 Send this ticket to a friend.");
    println!("     No IP, no port forwarding!");
    println!("  🎮 Minecraft LAN port: {mc_port}");
    println!();
    println!("  ⏳ Waiting for a friend... (Ctrl+C to exit)");
    println!("  ─────────────────────────────────────────");

    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

// ─── JOIN mode ─────────────────────────────────────────────────────────────

async fn run_join(ticket: String, local_port: u16) -> Result<()> {
    println!("  ⏳ Starting iroh endpoint...");

    let endpoint = Endpoint::bind(presets::N0)
        .await
        .context("Failed to create iroh endpoint")?;

    endpoint.online().await;

    let node_id: EndpointId = ticket
        .trim()
        .parse()
        .context("Invalid ticket. Please copy it exactly from the host.")?;

    println!("  🔌 Connecting to the host...");

    let conn = endpoint
        .connect(node_id, ALPN)
        .await
        .context("Failed to connect. Check the ticket or ask the host to restart.")?;

    println!("  ✅ Connected!");
    println!("  ─────────────────────────────────────────");
    println!("  🎮 Open Minecraft:");
    println!("     Multiplayer → Add Server");
    println!("     Address: 127.0.0.1:{local_port}");
    println!();
    println!("  ⏳ Waiting for Minecraft... (Ctrl+C to exit)");
    println!("  ─────────────────────────────────────────");

    let listener = TcpListener::bind(format!("127.0.0.1:{local_port}"))
        .await
        .with_context(|| format!("Port {local_port} is already in use"))?;

    loop {
        let (tcp, _) = listener.accept().await?;
        println!("  🟢 Minecraft connected — let's play!");
        let conn2 = conn.clone();
        tokio::spawn(async move {
            match conn2.open_bi().await {
                Err(e) => eprintln!("  ✗ Stream error: {e}"),
                Ok((mut qs, mut qr)) => {
                    let (mut tr, mut tw) = tcp.into_split();

                    let a = async {
                        let mut buf = vec![0u8; 65536];
                        loop {
                            match qr.read(&mut buf).await {
                                Ok(Some(0)) | Ok(None) => break,
                                Ok(Some(n)) => tw.write_all(&buf[..n]).await?,
                                Err(e) => {
                                    return Err(std::io::Error::other(e.to_string()));
                                }
                            }
                        }
                        tw.shutdown().await?;
                        Ok::<_, std::io::Error>(())
                    };

                    let b = async {
                        let mut buf = vec![0u8; 65536];
                        loop {
                            let n = tr.read(&mut buf).await?;
                            if n == 0 {
                                break;
                            }
                            qs.write_all(&buf[..n]).await?;
                        }
                        qs.finish()
                            .map_err(|e| std::io::Error::other(e.to_string()))?;
                        Ok::<_, std::io::Error>(())
                    };

                    let _ = tokio::try_join!(a, b);
                }
            }
        });
    }
}

// ─── Menu ────────────────────────────────────────────────────────────────────

fn ask(prompt: &str) -> String {
    print!("  {prompt}");
    let _ = io::stdout().flush();
    let mut s = String::new();
    io::stdin().read_line(&mut s).unwrap();
    s.trim().to_string()
}

fn ask_or(prompt: &str, default: &str) -> String {
    let v = ask(&format!("{prompt} [default: {default}]: "));
    if v.is_empty() {
        default.to_string()
    } else {
        v
    }
}

enum Mode {
    Host { mc_port: u16 },
    Join { ticket: String, local_port: u16 },
}

fn show_menu() -> Result<Mode> {
    print!("\x1B[2J\x1B[1;1H");
    let _ = io::stdout().flush();

    println!();
    println!("  ╔══════════════════════════════════════════╗");
    println!("  ║   🎮  Minecraft QUIC Tunnel  v0.3        ║");
    println!("  ║   Free. Without Port Forwarding.         ║");
    println!("  ╚══════════════════════════════════════════╝");
    println!();
    println!("  Works through public relay servers iroh.computer");
    println!("  If possible — punches through NAT directly (P2P).");
    println!();
    println!("  [1] 🏠 Host a game   (get a ticket for a friend)");
    println!("  [2] 🔗 Connect   (enter a ticket from the host)");
    println!();

    let choice = loop {
        let c = ask("Choice (1 or 2): ");
        if c == "1" || c == "2" {
            break c;
        }
        println!("  ⚠️  Please enter 1 or 2");
    };

    if choice == "1" {
        println!();
        println!("  ── Host Mode ──────────────────────────");
        println!("  1. Start Minecraft");
        println!("  2. Open world → Pause → Open to LAN");
        println!("  3. Remember the LAN port shown by the game");
        println!();
        let mc = ask_or("Minecraft Port (LAN port from the game)", "25565");
        Ok(Mode::Host {
            mc_port: mc.parse().context("Invalid port")?,
        })
    } else {
        println!();
        println!("  ── Client Mode ──────────────────────────");
        println!();
        let ticket = ask("Ticket from the host (long string): ");
        if ticket.is_empty() {
            anyhow::bail!("Ticket cannot be empty");
        }
        let lp = ask_or("Local port for Minecraft", "25566");
        Ok(Mode::Join {
            ticket,
            local_port: lp.parse().context("Invalid port")?,
        })
    }
}

// ─── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let result: Result<()> = async {
        match show_menu()? {
            Mode::Host { mc_port } => run_host(mc_port).await,
            Mode::Join { ticket, local_port } => run_join(ticket, local_port).await,
        }
    }
    .await;

    if let Err(e) = result {
        println!();
        println!("  ❌ Error: {e:#}");
        println!();
        println!("  Press Enter to exit...");
        let _ = io::stdin().read_line(&mut String::new());
    }
}