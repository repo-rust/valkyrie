use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{SetStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

#[derive(Debug)]
pub struct SetCommand {
    key: String,
    value: String,
    expiration_in_ms: u64,
}

impl RedisCommand for SetCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;
        if elements.len() < 3 {
            return Err(anyhow!("Not enough arguments for SET command"));
        }

        if let RedisType::BulkString(key) = &elements[1]
            && let RedisType::BulkString(value) = &elements[2]
        {
            let mut expiration_in_ms = 0_u64;

            // Optional EX seconds / PX milliseconds
            if elements.len() >= 5
                && let (RedisType::BulkString(arg), RedisType::BulkString(arg_value)) =
                    (&elements[3], &elements[4])
            {
                if arg.eq_ignore_ascii_case("EX") {
                    expiration_in_ms = 1000
                        * arg_value.parse::<u64>().with_context(|| {
                            format!("Can't convert EX value '{arg_value}' to number")
                        })?;
                } else if arg.eq_ignore_ascii_case("PX") {
                    expiration_in_ms = arg_value.parse::<u64>().with_context(|| {
                        format!("Can't convert PX value '{arg_value}' to number")
                    })?;
                }
            }

            Ok(Self {
                key: key.clone(),
                value: value.clone(),
                expiration_in_ms,
            })
        } else {
            Err(anyhow!("SET arguments are not BulkString"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;
        let resp = engine
            .execute(SetStorage {
                key: self.key.clone(),
                value: self.value.clone(),
                expiration_in_ms: self.expiration_in_ms,
            })
            .await?;

        match resp {
            StorageResponse::Success => {
                RedisType::SimpleString("OK".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            _ => {
                RedisType::SimpleError("Error occurred during SET".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
