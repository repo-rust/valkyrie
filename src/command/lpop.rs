use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{ListLeftPopStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

///
/// https://redis.io/docs/latest/commands/lpop/
/// Removes and returns the first elements of the list stored at key.
/// - Without count: returns the first element as BulkString, or Null if key doesn't exist or list empty.
/// - With count: returns an Array of up to `count` elements. Returns Null if the key doesn't exist.
///
#[derive(Debug)]
pub struct LPopCommand {
    key: String,
    count: Option<usize>,
}

impl RedisCommand for LPopCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;

        // LPOP key [count]
        if elements.len() < 2 {
            return Err(anyhow!("Not enough arguments for LPOP command"));
        }

        if let RedisType::BulkString(key) = &elements[1] {
            // Optional count
            let count = if elements.len() >= 3 {
                match &elements[2] {
                    RedisType::BulkString(count_str) => {
                        let parsed = count_str.parse::<usize>().with_context(|| {
                            format!(
                                "Failed to parse LPOP count parameter '{}' as unsigned integer",
                                count_str
                            )
                        })?;
                        Some(parsed)
                    }
                    _ => return Err(anyhow!("LPOP count is not BulkString")),
                }
            } else {
                None
            };

            Ok(Self {
                key: key.clone(),
                count,
            })
        } else {
            Err(anyhow!("LPOP key is not BulkString"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;
        let resp = engine
            .execute(ListLeftPopStorage {
                key: self.key.clone(),
                count: self.count,
            })
            .await?;

        match resp {
            StorageResponse::KeyValue { value } => {
                // Single element popped (no count provided)
                RedisType::BulkString(value)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            StorageResponse::ListValues { values } => {
                // Count provided: return array of popped elements
                let arr = RedisType::Array(values.into_iter().map(RedisType::BulkString).collect());
                arr.write_resp_to_stream(output_buf, stream).await?;
            }
            StorageResponse::Null => {
                // Null reply if key does not exist or list empty:
                // - Without count: Null Bulk String
                // - With count: Null Array
                let null_reply = if self.count.is_some() {
                    RedisType::NullArray
                } else {
                    RedisType::NullBulkString
                };
                null_reply.write_resp_to_stream(output_buf, stream).await?;
            }
            StorageResponse::Failed(msg) => {
                RedisType::SimpleError(msg)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            _ => {
                RedisType::SimpleError("Unknown error occurred during LPOP".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
