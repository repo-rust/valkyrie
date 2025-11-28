use std::io;

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::{
    command::factory::RedisCommand, protocol::redis_serialization_protocol::try_parse_type,
};

pub async fn handle_tcp_connection_from_client(mut stream: TcpStream) -> io::Result<()> {
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
            Some(RedisCommand::Ping(maybe_argument)) => {
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
            Some(RedisCommand::Command()) => {
                RedisType::Array(vec![])
                    .write_resp_bytes(&mut stream)
                    .await?;
            }
            None => {
                // For unsupported commands, do nothing for now.
                tracing::warn!("Unsupported command received.");
            }
        }
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}
