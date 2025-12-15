use anyhow::{Result, anyhow};
use bytes::BytesMut;
use std::sync::{Arc, OnceLock};
use tokio::net::TcpStream;

use crate::protocol::redis_serialization_protocol::RedisType;
use crate::storage::StorageEngine;

/// Command trait following the Open-Closed Principle.
/// New commands can be added by implementing this trait and registering
/// a new dispatch inside `dispatch_and_execute`.
pub trait RedisCommand: Sized {
    /// Parses the given RedisType into a concrete command instance.
    fn parse(redis_type: &RedisType) -> Result<Self>;

    /// Executes the command and writes a RESP reply to the stream.
    async fn execute(&self, output_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()>;
}

// Global access to the storage engine for command implementations.
// We initialize it once from the server code.
static STORAGE_ENGINE: OnceLock<Arc<StorageEngine>> = OnceLock::new();

pub fn ensure_storage_engine(engine: Arc<StorageEngine>) {
    let _ = STORAGE_ENGINE.get_or_init(|| engine);
}

fn storage_engine() -> Result<Arc<StorageEngine>> {
    STORAGE_ENGINE
        .get()
        .cloned()
        .ok_or_else(|| anyhow!("Storage engine is not initialized"))
}

// Helpers used by submodules
fn expect_cmd_array(redis_type: &RedisType) -> Result<&[RedisType]> {
    if let RedisType::Array(elements) = redis_type {
        Ok(elements.as_slice())
    } else {
        Err(anyhow!("Unsupported request, expected Array"))
    }
}

fn upper_first_bulk_string(redis_type: &RedisType) -> Option<String> {
    if let RedisType::Array(elements) = redis_type
        && let Some(RedisType::BulkString(cmd)) = elements.first()
    {
        return Some(cmd.to_uppercase());
    }
    None
}

// Submodules containing individual command implementations
mod command_meta;
mod echo;
mod get;
mod ping;
mod rpush;
mod set;

// Re-export for convenience
pub use command_meta::CommandCommand;
pub use echo::EchoCommand;
pub use get::GetCommand;
pub use ping::PingCommand;
pub use rpush::RPushCommand;
pub use set::SetCommand;

/// Dispatches a parsed RESP value to the corresponding command and executes it.
/// Returns an error if the command is unsupported or invalid.
pub async fn dispatch_and_execute(
    redis_type: &RedisType,
    output_buf: &mut BytesMut,
    stream: &mut TcpStream,
) -> Result<()> {
    match upper_first_bulk_string(redis_type).as_deref() {
        Some("PING") => {
            return PingCommand::parse(redis_type)?
                .execute(output_buf, stream)
                .await;
        }
        Some("ECHO") => {
            return EchoCommand::parse(redis_type)?
                .execute(output_buf, stream)
                .await;
        }
        Some("COMMAND") => {
            return CommandCommand::parse(redis_type)?
                .execute(output_buf, stream)
                .await;
        }
        Some("SET") => {
            return SetCommand::parse(redis_type)?
                .execute(output_buf, stream)
                .await;
        }
        Some("GET") => {
            return GetCommand::parse(redis_type)?
                .execute(output_buf, stream)
                .await;
        }
        Some("RPUSH") => {
            return RPushCommand::parse(redis_type)?
                .execute(output_buf, stream)
                .await;
        }
        Some(cmd) => Err(anyhow!("Command type is not defined or unknown {cmd}")),
        None => Err(anyhow!("Incorrect command type format")),
    }
}
