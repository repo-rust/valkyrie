use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::command::{dispatch_and_execute, ensure_storage_engine};
use crate::protocol::redis_serialization_protocol::{RedisType, try_parse_frame};
use crate::storage::StorageEngine;

use std::net::TcpListener as StdTcpListener;

use socket2::{Domain, Protocol, Socket, Type};

// Build a nonblocking std::net::TcpListener with SO_REUSEPORT (where supported) so multiple
// listeners can bind to the same addr:port across shards.
pub fn build_tcp_listener(addr: SocketAddr) -> anyhow::Result<StdTcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    socket.set_reuse_address(true)?;

    // Enable SO_REUSEPORT for Linux
    #[cfg(target_os = "linux")]
    {
        tracing::info!("SO_REUSEPORT enabled");
        socket.set_reuse_port(true)?;
    }

    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let listener: StdTcpListener = socket.into();

    #[cfg(target_os = "linux")]
    {
        // Required for integrating with Tokio via TcpListener::from_std,
        // which expects a nonblocking socket so the runtime can drive it with readiness-based I/O.
        listener.set_nonblocking(true)?;
    }

    Ok(listener)
}

pub async fn run_client_connection(stream: TcpStream, storage_engine: Arc<StorageEngine>) {
    if let Err(error) = handle_tcp_connection_from_client(stream, storage_engine).await {
        // Expected client disconnects are not errors but normal cases.
        if let Some(io_err) = error.downcast_ref::<std::io::Error>() {
            match io_err.kind() {
                std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted => {
                    tracing::debug!("Client disconnected: {io_err}");
                }
                _ => {
                    tracing::error!("Connection error: {io_err}");
                }
            }
        } else {
            tracing::error!("Connection error: {error}");
        }
    }
}

const INITIAL_READ_CAPACITY: usize = 1024; // Initial buffer with 1 KB. Grows on demand. RESP frames are typically small.
const MAX_REQUEST_SIZE: usize = 64 * 1024; // fail-safe limit to avoid unbounded memory usage

const DEFAULT_WRITE_CAPACITY: usize = 1024;

async fn handle_tcp_connection_from_client(
    mut stream: TcpStream,
    storage_engine: Arc<StorageEngine>,
) -> anyhow::Result<()> {
    let mut input_buf = BytesMut::with_capacity(INITIAL_READ_CAPACITY);

    let mut output_buf = BytesMut::with_capacity(DEFAULT_WRITE_CAPACITY);

    // Provide StorageEngine to command implementations (initialized once)
    ensure_storage_engine(storage_engine.clone());

    'outer: loop {
        // Incremental parsing: parse a single complete frame (if available).
        // Do not reparse bytes already consumed; keep leftovers for the next iteration.
        let received_redis_type = loop {
            if let Some((parsed_redis_type, consumed_bytes_cnt)) = try_parse_frame(&input_buf) {
                // Drop the consumed prefix; keep any pipelined bytes in the buffer.
                let _ = input_buf.split_to(consumed_bytes_cnt);
                break parsed_redis_type;
            }

            // Need more bytes to complete a frame.
            let n = stream.read_buf(&mut input_buf).await?;

            // Guardrail: avoid unbounded memory growth on malformed or huge requests.
            if input_buf.len() > MAX_REQUEST_SIZE {
                RedisType::SimpleError("Request too large".to_string())
                    .write_resp_to_stream(&mut output_buf, &mut stream)
                    .await?;
                break 'outer;
            }

            if n == 0 {
                // connection closed by the client
                break 'outer;
            }
        };

        if let Err(error) =
            dispatch_and_execute(&received_redis_type, &mut output_buf, &mut stream).await
        {
            tracing::warn!("Unsupported command received: {error:?}");

            RedisType::SimpleError(error.to_string())
                .write_resp_to_stream(&mut output_buf, &mut stream)
                .await?;
        }
    }

    output_buf.clear();

    Ok(())
}
