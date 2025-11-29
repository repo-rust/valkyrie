#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

use tokio::net::TcpListener;

use crate::network::connection_handler::{build_tcp_listener, run_client_connection};
use crate::startup_arguments::StartupArguments;
use crate::utils::thread_utils::pin_current_thread_to_cpu;

pub fn start_reuseport_tcp_handlers(arguments: &StartupArguments) -> io::Result<()> {
    tracing::info!(
        "Starting {} TCP handlers with SO_REUSEPORT",
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
    for _ in 0..tcp_handlers_count {
        listeners.push(
            build_tcp_listener(address).expect("Failed to create TCP listener for tcp-handler"),
        );
    }

    let mut tcp_handlers = Vec::with_capacity(tcp_handlers_count);

    for (handler_id, single_listener) in listeners.into_iter().enumerate() {
        let core_affinity_range_copy = core_affinity_range.clone();

        let tcp_handler = thread::Builder::new()
            .name(format!("tcp-handler-{handler_id}"))
            .spawn(move || {
                pin_current_thread_to_cpu(handler_id, core_affinity_range_copy);

                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("Failed to create tokio runtime for TCP handler");

                runtime.block_on(async move {
                    tracing::debug!("Started");

                    match TcpListener::from_std(single_listener) {
                        Ok(listener) => {
                            loop {
                                match listener.accept().await {
                                    Ok((stream, _)) => {
                                        // Each shard owns its accepted connections; no cross-shard handoff.
                                        tokio::spawn(run_client_connection(stream));
                                    }
                                    Err(error) => {
                                        tracing::error!("TCP accept failed with: {}", error);
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            tracing::error!("Can't convert from std listener to tokio: {}", error);
                        }
                    }
                });
            })
            .expect("Failed to spawn tcp-handler thread");

        tcp_handlers.push(tcp_handler);
    }

    tcp_handlers
}
