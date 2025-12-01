use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::engine::{StorageEngine, StorageRequest, StorageResponse};
use crate::{
    command::factory::RedisCommand, protocol::redis_serialization_protocol::try_parse_type,
};

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

async fn handle_tcp_connection_from_client(
    mut stream: TcpStream,
    storage_engine: Arc<StorageEngine>,
) -> anyhow::Result<()> {
    //
    // 10 KB should be enough to parse request bytes into RedisType
    //
    let mut buf = BytesMut::with_capacity(10 * 1024);

    'outer: loop {
        let mut read_it_idx = 0;
        let mut received_bytes_cnt = 0;

        let mut type_opt = None;

        while type_opt.is_none() {
            received_bytes_cnt += stream.read_buf(&mut buf).await?;

            if read_it_idx == 0 && received_bytes_cnt == 0 {
                // connection closed by client
                break 'outer;
            }

            type_opt = try_parse_type(&buf);
            read_it_idx += 1;
        }

        let received_redis_type = type_opt.unwrap();

        let maybe_redis_command = RedisCommand::from_redis_type(&received_redis_type);

        match maybe_redis_command {
            Ok(RedisCommand::Ping(maybe_argument)) => {
                if let Some(argument) = maybe_argument {
                    // let response_bytes = format!("${}\r\n{}\r\n", argument.len(), argument);

                    // Bulk string reply: the provided argument.
                    // stream.write_all(response_bytes.as_bytes()).await?;

                    RedisType::BulkString(argument)
                        .write_resp_bytes(&mut stream)
                        .await?;
                } else {
                    // Simple string reply: PONG when no argument is provided.
                    RedisType::SimpleString("PONG".to_string())
                        .write_resp_bytes(&mut stream)
                        .await?;
                }
            }
            Ok(RedisCommand::Command()) => {
                RedisType::Array(vec![])
                    .write_resp_bytes(&mut stream)
                    .await?;
            }
            Ok(RedisCommand::Echo(argument)) => {
                // Bulk string reply: the given string.
                RedisType::BulkString(argument)
                    .write_resp_bytes(&mut stream)
                    .await?;
            }
            Ok(RedisCommand::Set {
                key,
                value,
                expiration_in_ms,
            }) => {
                let command_response = storage_engine
                    .execute(StorageRequest::Set {
                        key,
                        value,
                        expiration_in_ms,
                    })
                    .await?;

                if let StorageResponse::Success = command_response {
                    // Simple string reply: OK: The key was set.
                    RedisType::SimpleString("OK".to_string())
                        .write_resp_bytes(&mut stream)
                        .await?;
                } else {
                    RedisType::SimpleError("Error occurred during SET".to_string())
                        .write_resp_bytes(&mut stream)
                        .await?;
                }
            }

            Ok(RedisCommand::Get { key }) => {
                let command_response = storage_engine.execute(StorageRequest::Get { key }).await?;

                match command_response {
                    StorageResponse::Nill => {
                        // $-1\r\n
                        // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
                        RedisType::NullBulkString
                            .write_resp_bytes(&mut stream)
                            .await?;
                    }
                    StorageResponse::KeyValue { value } => {
                        // Bulk string reply: the value of the key.
                        RedisType::BulkString(value)
                            .write_resp_bytes(&mut stream)
                            .await?;
                    }
                    _ => {
                        RedisType::SimpleError("Error occurred during GET".to_string())
                            .write_resp_bytes(&mut stream)
                            .await?;
                    }
                }
            }

            Err(error) => {
                tracing::warn!("Unsupported command received: {error:?}");

                RedisType::SimpleError(error.to_string())
                    .write_resp_bytes(&mut stream)
                    .await?
            }
        }
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}
