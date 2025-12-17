use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{ListLeftBlockingPopStorage, ListLeftPopStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

///
/// https://redis.io/docs/latest/commands/blpop/
/// Removes and returns the first elements of the list stored at key.
/// - Without count: returns the first element as BulkString, or Null if key doesn't exist or list empty.
/// - With count: returns an Array of up to `count` elements. Returns Null if the key doesn't exist.
///
#[derive(Debug)]
pub struct BlockingLeftPopCommand {
    keys: Vec<String>,
    timeout: u64,
}

impl RedisCommand for BlockingLeftPopCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;

        // BLPOP key [key ...] timeout
        if elements.len() < 3 {
            return Err(anyhow!(
                "Incomplete BLPOP command, expected at least 3 values: 'BLPOP key timeout'"
            ));
        }

        let mut keys = Vec::new();
        let mut timeout = 0;

        for single_argument in &elements[1..elements.len() - 1] {
            if let RedisType::BulkString(key) = single_argument {
                keys.push(key.clone());
            } else {
                return Err(anyhow!("BLPOP incorrect list key, not BulkString"));
            }
        }

        if let Some(RedisType::BulkString(timeout_str)) = elements.last() {
            timeout = timeout_str
                .parse::<u64>()
                .with_context(|| "BLPOP  'timeout' is not unsigned integer")?;
        } else {
            return Err(anyhow!("BLPOP incorrect 'timeout' argument"));
        }

        Ok(BlockingLeftPopCommand { keys, timeout })
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;

        // Execute storage command with timeout
        let resp_or_timeout = timeout(
            Duration::from_secs(self.timeout),
            engine.execute(ListLeftBlockingPopStorage {
                keys: self.keys.clone(),
                timeout: self.timeout,
            }),
        )
        .await;

        if let Ok(resp) = resp_or_timeout {
            match resp? {
                StorageResponse::KeyValue { value } => {
                    // Return single value
                    RedisType::BulkString(value)
                        .write_resp_to_stream(output_buf, stream)
                        .await?;
                }
                StorageResponse::Failed(msg) => {
                    RedisType::SimpleError(msg)
                        .write_resp_to_stream(output_buf, stream)
                        .await?;
                }
                _ => {
                    RedisType::SimpleError("Unknown error occurred during BLPOP".to_string())
                        .write_resp_to_stream(output_buf, stream)
                        .await?;
                }
            };
        } else {
            tracing::debug!("BLPOP timed out");
            // BLPOP timed out waiting
            RedisType::NullBulkString
                .write_resp_to_stream(output_buf, stream)
                .await?;
        }

        Ok(())
    }
}
