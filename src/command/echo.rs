use anyhow::{Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;

use super::RedisCommand;

#[derive(Debug)]
pub struct EchoCommand {
    argument: String,
}

impl RedisCommand for EchoCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;

        if elements.len() != 2 {
            return Err(anyhow!("No argument for ECHO command"));
        }

        if let RedisType::BulkString(arg) = &elements[1] {
            Ok(Self {
                argument: arg.clone(),
            })
        } else {
            Err(anyhow!("ECHO argument is not a BulkString"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        RedisType::BulkString(self.argument.clone())
            .write_resp_to_stream(output_buf, stream)
            .await?;
        Ok(())
    }
}
