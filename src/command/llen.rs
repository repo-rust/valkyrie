use anyhow::{Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{ListLengthStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

///
/// https://redis.io/docs/latest/commands/llen/
/// Returns the length of the list stored at key.
/// If key does not exist, it is interpreted as an empty list and 0 is returned.
/// An error is returned when the value stored at key is not a list.
///
#[derive(Debug)]
pub struct LLenCommand {
    key: String,
}

impl RedisCommand for LLenCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;

        // LLEN key
        if elements.len() < 2 {
            return Err(anyhow!("Not enough arguments for LLEN command"));
        }

        if let RedisType::BulkString(key) = &elements[1] {
            Ok(Self { key: key.clone() })
        } else {
            Err(anyhow!("LLEN argument is not a BulkString"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;
        let resp = engine
            .execute(ListLengthStorage {
                key: self.key.clone(),
            })
            .await?;

        match resp {
            StorageResponse::ListLength(len) => {
                RedisType::Integer(len as i32)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            StorageResponse::Failed(msg) => {
                RedisType::SimpleError(msg)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            _ => {
                RedisType::SimpleError("Unknown error occurred during LLEN".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
