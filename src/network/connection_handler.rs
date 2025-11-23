use std::io;

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::{RedisType, parse_type};

pub async fn handle_connection(mut stream: TcpStream, shard_id: usize) -> io::Result<()> {
    //
    // 10KB should be enough to parse request bytes into REdisType
    //
    let mut buf = BytesMut::with_capacity(10 * 1024);

    loop {
        let mut received_bytes_cnt = stream.read_buf(&mut buf).await?;

        if received_bytes_cnt == 0 {
            break;
        }

        let mut maybe_type = parse_type(&buf);

        while maybe_type.is_none() {
            received_bytes_cnt += stream.read_buf(&mut buf).await?;
            maybe_type = parse_type(&buf);
        }

        let received_redis_type = maybe_type.unwrap();

        match received_redis_type {
            RedisType::SimpleString(value) => {
                println!("[shard-{shard_id}]: rcv: \"{value}\", bytes: {received_bytes_cnt}");
            }
            RedisType::BulkString(value) => {
                println!("[shard-{shard_id}]: rcv: \"{value}\", bytes: {received_bytes_cnt}");
            }
            _ => {
                println!(
                    "[shard-{}]: rcv: \"{}\", bytes: {}",
                    shard_id,
                    "Undefined RedisType".to_owned(),
                    received_bytes_cnt
                );
            }
        }
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}
