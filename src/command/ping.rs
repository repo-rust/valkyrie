use anyhow::{Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;

use super::RedisCommand;

// Access parent helpers via `super::expect_cmd_array` and `super::upper_first_bulk_string`.

#[derive(Debug)]
pub struct PingCommand {
    argument: Option<String>,
}

impl RedisCommand for PingCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;

        match elements.len() {
            1 => Ok(Self { argument: None }),
            2 => {
                if let RedisType::BulkString(arg) = &elements[1] {
                    Ok(Self {
                        argument: Some(arg.clone()),
                    })
                } else {
                    Err(anyhow!("PING argument should be BulkString"))
                }
            }
            _ => Err(anyhow!("Incorrect number of arguments for PING command")),
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        if let Some(arg) = &self.argument {
            RedisType::BulkString(arg.clone())
                .write_resp_to_stream(output_buf, stream)
                .await?;
        } else {
            RedisType::SimpleString("PONG".to_string())
                .write_resp_to_stream(output_buf, stream)
                .await?;
        }
        Ok(())
    }
}
