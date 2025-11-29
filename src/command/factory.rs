use crate::protocol::redis_serialization_protocol::RedisType;

use anyhow::Result;

pub enum RedisCommand {
    Ping(Option<String>),
    Echo(String), //https://redis.io/docs/latest/commands/echo/
    Command(),
}

impl RedisCommand {
    pub fn from_redis_type(redis_type: &RedisType) -> Result<RedisCommand, String> {
        match redis_type {
            RedisType::Array(elements) => {
                if elements.is_empty() {
                    return Err("Operation failed".to_string());
                }

                match &elements[0] {
                    RedisType::BulkString(command_name) => {
                        if command_name.to_uppercase() == "PING" {
                            if elements.len() == 1 {
                                return Ok(RedisCommand::Ping(None));
                            }

                            if elements.len() == 2 {
                                if let RedisType::BulkString(arg) = &elements[1] {
                                    return Ok(RedisCommand::Ping(Some(arg.clone())));
                                }
                            }
                        } else if command_name.to_uppercase() == "COMMAND" {
                            return Ok(RedisCommand::Command());
                        } else if command_name.to_uppercase() == "ECHO" {
                            if elements.len() != 2 {
                                return Err("No argument for ECHO command".into());
                            }

                            if let RedisType::BulkString(arg) = &elements[1] {
                                return Ok(RedisCommand::Echo(arg.clone()));
                            } else {
                                return Err("ECHO command argument is not BulkString".into());
                            }
                        }

                        Err(format!(
                            "Command type is not defined or unknown {}",
                            command_name
                        ))
                    }
                    _ => Err("RedisArray 0 element is not a BulkString".into()),
                }
            }

            _ => Err("undefined command".into()),
        }
    }
}
