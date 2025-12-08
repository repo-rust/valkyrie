use anyhow::{Result, anyhow};
use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::{StorageRequest, StorageResponse};

use super::{RedisCommandInstance, storage_engine};

#[derive(Debug)]
pub struct GetCommand {
    pub(crate) key: String,
}

impl RedisCommandInstance for GetCommand {
    fn parse(redis_type: &RedisType) -> Result<Self> {
        let elements = super::expect_cmd_array(redis_type)?;
        if elements.len() < 2 {
            return Err(anyhow!("No enough arguments for GET command"));
        }

        if let RedisType::BulkString(key) = &elements[1] {
            Ok(Self { key: key.clone() })
        } else {
            Err(anyhow!("GET argument is not a BulkString"))
        }
    }

    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
        let engine = storage_engine()?;
        let resp = engine
            .execute(StorageRequest::Get {
                key: self.key.clone(),
            })
            .await?;

        match resp {
            StorageResponse::Nill => {
                RedisType::NullBulkString
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            StorageResponse::KeyValue { value } => {
                RedisType::BulkString(value)
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
            _ => {
                RedisType::SimpleError("Error occurred during GET".to_string())
                    .write_resp_to_stream(output_buf, stream)
                    .await?;
            }
        }

        Ok(())
    }
}
