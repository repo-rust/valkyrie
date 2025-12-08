use anyhow::Result;
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;

use super::RedisCommand;

#[derive(Debug)]
pub struct CommandCommand;

impl RedisCommand for CommandCommand {
    fn parse(_redis_type: &RedisType) -> Result<Self> {
        Ok(Self)
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        RedisType::Array(vec![])
            .write_resp_to_stream(output_buf, stream)
            .await?;
        Ok(())
    }
}
