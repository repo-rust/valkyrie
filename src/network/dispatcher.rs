use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, unbounded};
use tokio::net::TcpStream;

use crate::network::connection_handler::handle_tcp_connection_from_client;
use crate::startup_arguments::StartupArguments;
use crate::thread_utils::{current_thread_name_or_default, pin_current_thread_to_cpu};

pub struct HandleAndSender {
    _handle: JoinHandle<()>,
    pub sender: crossbeam_channel::Sender<StdTcpStream>,
}

pub fn start_dispatcher(arguments: &StartupArguments) -> io::Result<()> {
    let tcp_affinity_cores = arguments.shards..arguments.shards + arguments.tcp_handlers;
    let tcp_handlers = start_tcp_handler_threads(arguments.tcp_handlers, tcp_affinity_cores);

    loop {
        // Single acceptor in std; hand off sockets to shards by a simple hash.
        let listener = StdTcpListener::bind(arguments.address)?;

        // Required for integrating with Tokio via TcpListener::from_std,
        // which expects a nonblocking socket so the runtime can drive it with readiness-based I/O.
        //listener.set_nonblocking(true)?;

        match listener.accept() {
            Ok((stream, peer)) => {
                // dispatch to tcp handlers
                let tcp_handle_id = (peer.port() as usize) % tcp_handlers.len();

                if let Err(e) = tcp_handlers[tcp_handle_id].sender.send(stream) {
                    eprintln!("Failed to dispatch to tcp-handler-{tcp_handle_id}: {e}");
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                use std::time::Duration;

                thread::sleep(Duration::from_micros(200));
            }
            Err(e) => return Err(e),
        }
    }
}

fn start_tcp_handler_threads(
    tcp_handlers_count: usize,
    core_affinity_range: std::ops::Range<usize>,
) -> Vec<HandleAndSender> {
    let mut tcp_handlers = Vec::with_capacity(tcp_handlers_count);

    for handler_id in 0..tcp_handlers_count {
        let core_affinity_range_copy = core_affinity_range.clone();

        let (stream_sender, stream_receiver) = unbounded::<StdTcpStream>();

        let tcp_handler = thread::Builder::new()
            .name(format!("tcp-handler-{handler_id}"))
            .spawn(move || {
                pin_current_thread_to_cpu("tcp-handler", handler_id, core_affinity_range_copy);

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async move {
                    println!(
                        "[{}] started",
                        current_thread_name_or_default("tcp-handler-???")
                    );

                    loop {
                        let received_tcp = stream_receiver.recv();

                        if received_tcp.is_err() {
                            println!("Error receiving tcp stream from channel");
                        } else {
                            println!("Received tcp stream from channel");
                        }

                        // while let Ok(std_stream) = stream_receiver.recv() {
                        //     match TcpStream::from_std(std_stream) {
                        //         Ok(stream) => {
                        //             tokio::spawn(async move {
                        //                 if let Err(e) =
                        //                     handle_tcp_connection_from_client(stream, handler_id)
                        //                         .await
                        //                 {
                        //                     eprintln!(
                        //                         "[tcp-handler-{handler_id}] connection error: {e}"
                        //                     );
                        //                 }
                        //             });
                        //         }
                        //         Err(e) => {
                        //             eprintln!("[tcp-handler-{handler_id}] from_std failed: {e}");
                        //         }
                        //     }
                        // }
                    }
                });
            })
            .expect("spawn shard thread");

        tcp_handlers.push(HandleAndSender {
            _handle: tcp_handler,
            sender: stream_sender,
        });
    }

    tcp_handlers
}

pub fn run_dispatcher(addr: SocketAddr, tcp_handlers_count: usize) -> io::Result<()> {
    println!("[main] Dispatcher listening on {addr}, tcp-handlers: {tcp_handlers_count}");

    // Create shards and their channels
    let mut senders: Vec<Sender<StdTcpStream>> = Vec::with_capacity(tcp_handlers_count);
    let mut handles = Vec::with_capacity(tcp_handlers_count);

    for shard_id in 0..tcp_handlers_count {
        let (stream_sender, stream_receiver) = unbounded::<StdTcpStream>();
        senders.push(stream_sender);
        handles.push(spawn_dispatch_shard(shard_id, stream_receiver));
    }

    // Single acceptor in std; hand off sockets to shards by a simple hash.
    let listener = StdTcpListener::bind(addr)?;

    // Required for integrating with Tokio via TcpListener::from_std,
    // which expects a nonblocking socket so the runtime can drive it with readiness-based I/O.
    listener.set_nonblocking(true)?;

    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                let shard_id = (peer.port() as usize) % tcp_handlers_count;
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
                                if let Err(e) =
                                    handle_tcp_connection_from_client(stream, shard_id).await
                                {
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
