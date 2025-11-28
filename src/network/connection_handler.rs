use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::utils::thread_utils::current_thread_name_or_default;
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
                    let response_bytes = format!("${}\r\n{}\r\n", argument.len(), argument);

                    // Bulk string reply: the provided argument.
                    stream.write_all(response_bytes.as_bytes()).await?;
                } else {
                    // Simple string reply: PONG when no argument is provided.
                    stream.write_all(b"+PONG\r\n").await?;
                }
            }
            Some(RedisCommand::Command()) => {
                stream.write_all(b"*0\r\n").await?;
            }
            None => {
                // For unsupported commands, do nothing for now.
                println!(
                    "[{}]: Unsupported command received.",
                    current_thread_name_or_default("tcp-handler-???")
                );
            }
        }

        // match received_redis_type {
        //     RedisType::SimpleString(value) => {
        //         println!(
        //             "[shard-{shard_id}]: rcv: SimpleString({value}), bytes: {received_bytes_cnt}"
        //         );
        //     }
        //     RedisType::BulkString(value) => {
        //         println!(
        //             "[shard-{shard_id}]: rcv: BulkString({value}), bytes: {received_bytes_cnt}"
        //         );
        //     }
        //     RedisType::Array(elements) => {
        //         for elem in &elements {
        //             println!("[shard-{shard_id}]: rcv: \"{elem:?}\", bytes: {received_bytes_cnt}");
        //         }
        //     }
        //     RedisType::Integer(value) => {
        //         println!(
        //             "[shard-{shard_id}]: rcv: \"Integer({value})\", bytes: {received_bytes_cnt}"
        //         );
        //     }
        //     _ => {
        //         println!(
        //             "[shard-{}]: rcv: \"{}\", bytes: {}",
        //             shard_id,
        //             "Undefined RedisType".to_owned(),
        //             received_bytes_cnt
        //         );
        //     }
        // }
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}
