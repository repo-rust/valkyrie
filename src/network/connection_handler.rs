use std::io;

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::{RedisType, try_parse_type};

pub async fn handle_connection(mut stream: TcpStream, shard_id: usize) -> io::Result<()> {
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

        match received_redis_type {
            RedisType::SimpleString(value) => {
                println!(
                    "[shard-{shard_id}]: rcv: SimpleString({value}), bytes: {received_bytes_cnt}"
                );
            }
            RedisType::BulkString(value) => {
                println!(
                    "[shard-{shard_id}]: rcv: BulkString({value}), bytes: {received_bytes_cnt}"
                );
            }
            RedisType::Array(elements) => {
                for elem in &elements {
                    println!("[shard-{shard_id}]: rcv: \"{elem:?}\", bytes: {received_bytes_cnt}");
                }
            }
            RedisType::Integer(value) => {
                println!(
                    "[shard-{shard_id}]: rcv: \"Integer({value})\", bytes: {received_bytes_cnt}"
                );
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
