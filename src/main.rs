use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::BytesMut;
use crossbeam_channel::{Receiver, Sender, unbounded};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod startup_arguments;
use crate::startup_arguments::{Mode, StartupArguments};

// Build a nonblocking std::net::TcpListener with SO_REUSEPORT (where supported) so multiple
// listeners can bind to the same addr:port across shards, Seastar-style.
fn build_reuseport_listener(addr: SocketAddr) -> io::Result<StdTcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    socket.set_reuse_address(true)?;

    // Enable SO_REUSEPORT on platforms that support it
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        socket.set_reuse_port(true)?;
    }

    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let listener: StdTcpListener = socket.into();
    listener.set_nonblocking(true)?;
    Ok(listener)
}

// Per-connection handler: read into a reusable BytesMut buffer and echo back.
// Uses clear() to retain capacity and avoid reallocations on subsequent reads.
async fn handle_connection(mut stream: TcpStream, _shard_id: usize) -> io::Result<()> {
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // Ensure some spare capacity; read_buf will grow if needed.
        buf.reserve(4096);
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            break;
        }

        // Echo back the bytes we have; write_all guarantees full write.
        stream.write_all(&buf).await?;
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}

async fn shard_accept_loop(listener: TcpListener, shard_id: usize) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        // Each shard owns its accepted connections; no cross-shard handoff.
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, shard_id).await {
                eprintln!("[shard {shard_id}] error with {peer}: {e}");
            }
        });
    }
}

// Shard thread that owns a current-thread Tokio runtime and a listener created with SO_REUSEPORT.
fn spawn_reuseport_shard(shard_id: usize, std_listener: StdTcpListener) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(format!("shard-reuseport-{shard_id}"))
        .spawn(move || {
            // Optional: pin to a core for stronger isolation (Linux only). Uncomment if desired.
            // #[cfg(target_os = "linux")]
            // {
            //     let core = shard_id % num_cpus::get();
            //     let _ = affinity::set_thread_affinity([core]);
            // }

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("build runtime");

            rt.block_on(async move {
                let listener = TcpListener::from_std(std_listener).expect("tokio listener");
                if let Err(e) = shard_accept_loop(listener, shard_id).await {
                    eprintln!("[shard {shard_id}] accept loop error: {e}");
                }
            });
        })
        .expect("spawn shard thread")
}

// Shard thread that receives std::net::TcpStream via a channel, converts to Tokio TcpStream and serves.
fn spawn_dispatch_shard(shard_id: usize, rx: Receiver<StdTcpStream>) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(format!("shard-dispatch-{shard_id}"))
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("build runtime");

            rt.block_on(async move {
                while let Ok(std_stream) = rx.recv() {
                    match TcpStream::from_std(std_stream) {
                        Ok(stream) => {
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, shard_id).await {
                                    eprintln!("[shard {shard_id}] connection error: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("[shard {shard_id}] from_std failed: {e}");
                        }
                    }
                }
            });
        })
        .expect("spawn shard thread")
}

fn run_reuseport(addr: SocketAddr, shards: usize) -> io::Result<()> {
    println!("Starting {shards} shards on {addr} with SO_REUSEPORT...");

    // Build one listener per shard. Each gets its own accept loop.
    let mut listeners = Vec::with_capacity(shards);
    for _ in 0..shards {
        listeners.push(build_reuseport_listener(addr)?);
    }
    let listeners = Arc::new(listeners);

    let mut handles = Vec::with_capacity(shards);
    for shard_id in 0..shards {
        let std_listener = listeners[shard_id]
            .try_clone()
            .expect("clone listener per shard");
        handles.push(spawn_reuseport_shard(shard_id, std_listener));
    }

    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn run_dispatcher(addr: SocketAddr, shards: usize) -> io::Result<()> {
    println!("Dispatcher listening on {addr}, shards: {shards}");

    // Create shards and their channels
    let mut senders: Vec<Sender<StdTcpStream>> = Vec::with_capacity(shards);
    let mut handles = Vec::with_capacity(shards);
    for shard_id in 0..shards {
        let (tx, rx) = unbounded::<StdTcpStream>();
        senders.push(tx);
        handles.push(spawn_dispatch_shard(shard_id, rx));
    }

    // Single acceptor in std; hand off sockets to shards by a simple hash.
    let listener = StdTcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;

    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                let shard_id = (peer.port() as usize) % shards;
                if let Err(e) = senders[shard_id].send(stream) {
                    eprintln!("Failed to dispatch to shard {shard_id}: {e}");
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_micros(200));
            }
            Err(e) => return Err(e),
        }
    }

    // Unreachable in demo; shards run forever.
    // for h in handles { let _ = h.join(); }
}

fn main() -> io::Result<()> {
    let program_arguments = StartupArguments::parse_args();

    println!("Program arguments: {program_arguments:?}");

    match program_arguments.mode {
        Mode::ReusePort => run_reuseport(
            program_arguments.socket_address,
            program_arguments.shards_count,
        ),
        Mode::Dispatcher => run_dispatcher(
            program_arguments.socket_address,
            program_arguments.shards_count,
        ),
    }
}
