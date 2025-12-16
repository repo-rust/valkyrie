use anyhow::{Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{ListLeftPushStorage, StorageResponse};

use super::{RedisCommand, storage_engine};

///
/// https://redis.io/docs/latest/commands/lpush/
///
#[derive(Debug)]
pub struct LPushCommand {
    key: String,
    values: Vec<String>,
}

impl RedisCommand for LPushCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;
        if elements.len() < 3 {
            return Err(anyhow!("Not enough arguments for LPUSH command"));
        }

        if let RedisType::BulkString(key) = &elements[1] {
            let mut values = Vec::new();
            for element in &elements[2..] {
                match element {
                    RedisType::BulkString(v) => values.push(v.clone()),
                    RedisType::Integer(i) => values.push(i.to_string()),
                    _ => return Err(anyhow!("LPUSH argument is not BulkString or Integer")),
                }
            }
            Ok(Self {
                key: key.clone(),
                values,
            })
        } else {
            Err(anyhow!("LPUSH key is not BulkString"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;
        let resp = engine
            .execute(ListLeftPushStorage {
                key: self.key.clone(),
                values: self.values.clone(),
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
                RedisType::SimpleError("Unknown error occurred during LPUSH".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
