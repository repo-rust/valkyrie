#![allow(dead_code)]

use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::thread::{self, JoinHandle};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

use crate::network::connection_handler::handle_tcp_connection_from_client;
use crate::startup_arguments::StartupArguments;
use crate::thread_utils::{current_thread_name_or_default, pin_current_thread_to_cpu};

pub fn start_reuseport_tcp_handlers(arguments: &StartupArguments) -> io::Result<()> {
    println!(
        "[{}] Starting {} TCP handlers with SO_REUSEPORT",
        current_thread_name_or_default("main"),
        arguments.tcp_handlers
    );

    let tcp_affinity_cores = arguments.shards..arguments.shards + arguments.tcp_handlers;

    let tcp_handlers = start_tcp_handler_threads(
        arguments.address,
        arguments.tcp_handlers,
        tcp_affinity_cores,
    );

    for h in tcp_handlers {
        let _ = h.join();
    }
    Ok(())
}

fn start_tcp_handler_threads(
    address: SocketAddr,
    tcp_handlers_count: usize,
    core_affinity_range: std::ops::Range<usize>,
) -> Vec<JoinHandle<()>> {
    //
    // Build one listener per tcp-handler. Each gets its own accept loop.
    //
    let mut listeners = Vec::with_capacity(tcp_handlers_count);
    for tcp_handler_id in 0..tcp_handlers_count {
        listeners.push(
            build_reuseport_listener(tcp_handler_id, address)
                .expect("Failed to create TCP listener for tcp-handler"),
        );
    }

    let mut tcp_handlers = Vec::with_capacity(tcp_handlers_count);

    for (handler_id, single_listener) in listeners.iter().enumerate().take(tcp_handlers_count) {
        let core_affinity_range_copy = core_affinity_range.clone();

        let std_listener = single_listener
            .try_clone()
            .expect("clone listener per shard");

        let tcp_handler = thread::Builder::new()
            .name(format!("tcp-handler-{handler_id}"))
            .spawn(move || {
                pin_current_thread_to_cpu("tcp-handler", handler_id, core_affinity_range_copy);

                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("Failed to create tokio runtime for TCP handler");

                runtime.block_on(async move {
                    println!(
                        "[{}] started",
                        current_thread_name_or_default("tcp-handler-???")
                    );

                    match TcpListener::from_std(std_listener) {
                        Ok(listener) => {
                            loop {
                                match listener.accept().await {
                                    Ok((stream, peer)) => {
                                        // Each shard owns its accepted connections; no cross-shard handoff.
                                        tokio::spawn(async move {
                                            if let Err(e) =
                                                handle_tcp_connection_from_client(stream).await
                                            {
                                                eprintln!(
                                                    "[{}] error with {}: {}",
                                                    current_thread_name_or_default(
                                                        "tcp-handler-???"
                                                    ),
                                                    peer,
                                                    e
                                                );
                                            }
                                        });
                                    }
                                    Err(error) => {
                                        eprintln!(
                                            "[{}] TCP accept failed with: {}",
                                            current_thread_name_or_default("tcp-handler-???"),
                                            error
                                        );
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            eprintln!(
                                "[{}] Can't convert from STD listener to Tokio: {}",
                                current_thread_name_or_default("tcp-handler-???"),
                                error
                            );
                        }
                    }
                });
            })
            .expect("spawn shard thread");

        tcp_handlers.push(tcp_handler);
    }

    tcp_handlers
}

// Build a nonblocking std::net::TcpListener with SO_REUSEPORT (where supported) so multiple
// listeners can bind to the same addr:port across shards, Seastar-style.
#[allow(unused_variables)]
fn build_reuseport_listener(tcp_handler_id: usize, addr: SocketAddr) -> io::Result<StdTcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    socket.set_reuse_address(true)?;

    // Enable SO_REUSEPORT for Linux
    #[cfg(target_os = "linux")]
    {
        println!("[{tcp_handler_id}] SO_REUSEPORT enabled");
        socket.set_reuse_port(true)?;
    }

    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let listener: StdTcpListener = socket.into();

    // Required for integrating with Tokio via TcpListener::from_std,
    // which expects a nonblocking socket so the runtime can drive it with readiness-based I/O.
    listener.set_nonblocking(true)?;
    Ok(listener)
}

async fn shard_accept_loop(listener: TcpListener, shard_id: usize) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;

        // Each shard owns its accepted connections; no cross-shard handoff.
        tokio::spawn(async move {
            if let Err(e) = handle_tcp_connection_from_client(stream).await {
                eprintln!("[shard {shard_id}] error with {peer}: {e}");
            }
        });
    }
}
