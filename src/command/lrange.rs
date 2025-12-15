use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{ListRangeStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

///
/// Returns the specified elements of the list stored at key. The offsets start and stop are zero-based indexes, with 0 being the first element of the list
///  (the head of the list), 1 being the next element and so on.
///
/// https://redis.io/docs/latest/commands/lrange/
///
#[derive(Debug)]
pub struct LRange {
    key: String,
    start: i32,
    end: i32,
}

impl RedisCommand for LRange {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;

        // LRANGE key start stop
        if elements.len() < 4 {
            return Err(anyhow!("Not enough arguments for LRANGE command"));
        }

        if let RedisType::BulkString(key) = &elements[1]
            && let RedisType::BulkString(start_str) = &elements[2]
            && let RedisType::BulkString(end_str) = &elements[3]
        {
            Ok(Self {
                key: key.clone(),
                start: start_str.parse::<i32>().with_context(|| {
                    format!(
                        "Failed to parse LRANGE start parameter '{}' as integer",
                        start_str
                    )
                })?,
                end: end_str.parse::<i32>().with_context(|| {
                    format!(
                        "Failed to parse LRANGE end parameter '{}' as integer",
                        end_str
                    )
                })?,
            })
        } else {
            Err(anyhow!(
                "LRANGE incorrect parameter types, expected BulkString, BulkString, BulkString"
            ))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;
        let resp = engine
            .execute(ListRangeStorage {
                key: self.key.clone(),
                start: self.start,
                end: self.end,
            })
            .await?;

        match resp {
            StorageResponse::ListValues { values } => {
                let redis_values = values.into_iter().map(RedisType::BulkString).collect();

                RedisType::Array(redis_values)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            StorageResponse::Failed(msg) => {
                RedisType::SimpleError(msg)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            _ => {
                RedisType::SimpleError("Unknown error occurred during LRANGE".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
