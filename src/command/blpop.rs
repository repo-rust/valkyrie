use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{ListLeftBlockingPopStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

///
/// https://redis.io/docs/latest/commands/blpop/
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

        for single_argument in &elements[1..elements.len() - 1] {
            if let RedisType::BulkString(key) = single_argument {
                keys.push(key.clone());
            } else {
                return Err(anyhow!("BLPOP incorrect list key, not BulkString"));
            }
        }

        if let Some(RedisType::BulkString(timeout_str)) = elements.last() {
            let timeout = timeout_str
                .parse::<u64>()
                .with_context(|| "BLPOP  'timeout' is not unsigned integer")?;
            Ok(BlockingLeftPopCommand { keys, timeout })
        } else {
            Err(anyhow!("BLPOP incorrect 'timeout' argument"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;

        // Build a set of futures (one per key) each with a timeout.
        let mut futures = FuturesUnordered::new();

        for single_key in &self.keys {
            let fut = timeout(
                Duration::from_secs(self.timeout),
                engine.execute(ListLeftBlockingPopStorage {
                    key: single_key.clone(),
                }),
            );
            futures.push(fut);
        }

        // Wait for the first completed future that didn't time out.
        let mut first_result: Option<anyhow::Result<StorageResponse>> = None;
        while let Some(res) = futures.next().await {
            match res {
                Ok(inner) => {
                    first_result = Some(inner);
                    break;
                }
                Err(_elapsed) => {
                    // This key timed out; keep waiting for others.
                    continue;
                }
            }
        }

        // Cancell all not completed futures
        drop(futures);

        match first_result {
            Some(Ok(response)) => {
                match response {
                    StorageResponse::ValueFromList { value, list_name } => {
                        // Array reply: the key from which the element was popped and the value of the popped element.
                        RedisType::Array(vec![
                            RedisType::BulkString(list_name),
                            RedisType::BulkString(value),
                        ])
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
            }
            Some(Err(e)) => {
                // Storage returned an error
                RedisType::SimpleError(format!("BLPOP error: {e}"))
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            None => {
                // All keys timed out
                tracing::debug!("BLPOP timed out");
                RedisType::NullBulkString
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
