#![allow(dead_code)]

use std::io;
use std::net::{TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::thread::{self};

use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::network::connection_handler::handle_tcp_connection_from_client;
use crate::startup_arguments::StartupArguments;
use crate::utils::thread_utils::pin_current_thread_to_cpu;

pub fn start_dispatcher_tcp_handlers(arguments: &StartupArguments) -> io::Result<()> {
    let tcp_affinity_cores = arguments.shards..arguments.shards + arguments.tcp_handlers;

    let tcp_handler_channels =
        start_tcp_handler_threads(arguments.tcp_handlers, tcp_affinity_cores);

    let maybe_listener = StdTcpListener::bind(arguments.address);

    if let Err(error) = maybe_listener {
        tracing::error!(
            "Failed to bind TCP listener to address {}: {}",
            arguments.address,
            error
        );
        return Err(error);
    }

    // Single acceptor in main thread. Hand off sockets to tcp-handlers by a simple hash.
    let listener = maybe_listener.unwrap();

    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                // Required for integrating with Tokio via TcpListener::from_std,
                // which expects a nonblocking socket so the runtime can drive it with readiness-based I/O.
                if let Err(error) = stream.set_nonblocking(true) {
                    tracing::error!(
                        "Can't move TCP accepted stream socket to non-blocking mode: {error}"
                    );
                    continue;
                }

                // Dispatch to TCP handlers based on peer port hash.
                let tcp_handler_channel_idx = (peer.port() as usize) % tcp_handler_channels.len();

                if let Err(e) = tcp_handler_channels[tcp_handler_channel_idx].send(stream) {
                    tracing::error!(
                        "Failed to dispatch to TCP handler with index '{tcp_handler_channel_idx}': {e}"
                    );
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn start_tcp_handler_threads(
    tcp_handlers_count: usize,
    core_affinity_range: std::ops::Range<usize>,
) -> Vec<UnboundedSender<StdTcpStream>> {
    let mut tcp_handlers = Vec::with_capacity(tcp_handlers_count);

    for handler_id in 0..tcp_handlers_count {
        let core_affinity_range_copy = core_affinity_range.clone();

        let (stream_sender, mut stream_receiver) = unbounded_channel::<StdTcpStream>();

        let _ = thread::Builder::new()
            .name(format!("tcp-handler-{handler_id}"))
            .spawn(move || {
                pin_current_thread_to_cpu("tcp-handler", handler_id, core_affinity_range_copy);

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async move {
                    tracing::info!("Started");

                    loop {
                        while let Some(std_stream) = stream_receiver.recv().await {
                            match TcpStream::from_std(std_stream) {
                                Ok(stream) => {
                                    tokio::spawn(async move {
                                        if let Err(e) =
                                            handle_tcp_connection_from_client(stream).await
                                        {
                                            tracing::error!(
                                                "[tcp-handler-{handler_id}] connection error: {e}"
                                            );
                                        }
                                    });
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "[tcp-handler-{handler_id}] from_std failed: {e}"
                                    );
                                }
                            }
                        }
                    }
                });
            })
            .expect("spawn shard thread");

        tcp_handlers.push(stream_sender);
    }

    tcp_handlers
}
