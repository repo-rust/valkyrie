use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::thread;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

use crate::network::connection_handler::handle_connection;

pub fn run_reuseport(addr: SocketAddr, shards: usize) -> io::Result<()> {
    println!("[main] Starting {shards} shards on {addr} with SO_REUSEPORT...");

    // Build one listener per shard. Each gets its own accept loop.
    let mut listeners = Vec::with_capacity(shards);
    for shard_id in 0..shards {
        listeners.push(build_reuseport_listener(shard_id, addr)?);
    }

    let mut handles = Vec::with_capacity(shards);

    for (shard_id, single_listener) in listeners.iter().enumerate() {
        let std_listener = single_listener
            .try_clone()
            .expect("clone listener per shard");
        handles.push(spawn_reuseport_shard(shard_id, std_listener));
    }

    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

// Build a nonblocking std::net::TcpListener with SO_REUSEPORT (where supported) so multiple
// listeners can bind to the same addr:port across shards, Seastar-style.
fn build_reuseport_listener(shard_id: usize, addr: SocketAddr) -> io::Result<StdTcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    socket.set_reuse_address(true)?;

    // Enable SO_REUSEPORT on platforms that support it
    #[cfg(any(target_os = "linux", target_os = "macos",))]
    {
        println!("[shard-{shard_id}] SO_REUSEPORT enabled");
        socket.set_reuse_port(true)?;
    }
    #[cfg(target_os = "windows")]
    {
        println!("[shard-{shard_id}] SO_REUSEPORT disabled");
    }

    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let listener: StdTcpListener = socket.into();
    listener.set_nonblocking(true)?;
    Ok(listener)
}

// Shard thread that owns a current-thread Tokio runtime and a listener created with SO_REUSEPORT.
fn spawn_reuseport_shard(shard_id: usize, std_listener: StdTcpListener) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(format!("shard-reuseport-{shard_id}"))
        .spawn(move || {
            // Pin to a CRU core for stronger isolation and betetr performance(Linux only).
            #[cfg(target_os = "linux")]
            {
                let core = shard_id % num_cpus::get();
                let _ = affinity::set_thread_affinity([core]);
                println!("[shard-{shard_id}] Pinned to CPU {core}");
            }

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
