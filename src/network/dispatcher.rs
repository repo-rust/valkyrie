use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, unbounded};
use tokio::net::TcpStream;

use crate::network::connection::handle_connection;

pub fn run_dispatcher(addr: SocketAddr, shards: usize) -> io::Result<()> {
    println!("[main] Dispatcher listening on {addr}, shards: {shards}");

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
