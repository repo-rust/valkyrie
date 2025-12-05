use crate::protocol::redis_serialization_protocol::RedisType;

use anyhow::{Context, Result, anyhow};

#[derive(Debug)]
pub enum RedisCommand {
    //https://redis.io/docs/latest/commands/ping/
    Ping(Option<String>),

    //https://redis.io/docs/latest/commands/echo/
    Echo(String),

    // https://redis.io/docs/latest/commands/set/
    Set {
        key: String,
        value: String,
        expiration_in_ms: u64,
    },
    //https://redis.io/docs/latest/commands/get/
    Get {
        key: String,
    },

    // https://redis.io/docs/latest/commands/command/
    Command(),

    // https://redis.io/docs/latest/commands/rpush/
    RPush {
        key: String,
        values: Vec<String>,
    },
}

impl RedisCommand {
    pub fn from_redis_type(redis_type: &RedisType) -> Result<RedisCommand, anyhow::Error> {
        tracing::debug!("received {:?}", redis_type);

        match redis_type {
            RedisType::Array(elements) => {
                if elements.is_empty() {
                    return Err(anyhow!("Operation failed"));
                }

                match &elements[0] {
                    RedisType::BulkString(command_name) => {
                        if command_name.to_uppercase() == "PING" {
                            parse_ping(elements)
                        } else if command_name.to_uppercase() == "COMMAND" {
                            parse_command()
                        } else if command_name.to_uppercase() == "ECHO" {
                            parse_echo(elements)
                        } else if command_name.to_uppercase() == "SET" {
                            parse_set(elements)
                        } else if command_name.to_uppercase() == "GET" {
                            parse_get(elements)
                        } else if command_name.to_uppercase() == "RPUSH" {
                            parse_rpush(elements)
                        } else {
                            Err(anyhow!(
                                "Command type is not defined or unknown {command_name}"
                            ))
                        }
                    }
                    _ => Err(anyhow!("RedisArray 0 element is not a BulkString")),
                }
            }

            _ => Err(anyhow!("undefined command")),
        }
    }
}

fn parse_ping(elements: &[RedisType]) -> Result<RedisCommand, anyhow::Error> {
    if elements.len() == 1 {
        return Ok(RedisCommand::Ping(None));
    }

    if elements.len() == 2 {
        if let RedisType::BulkString(arg) = &elements[1] {
            Ok(RedisCommand::Ping(Some(arg.clone())))
        } else {
            Err(anyhow!("PING argument should be BulkString"))
        }
    } else {
        Err(anyhow!("Incorrect number of arguments for PING command"))
    }
}

fn parse_command() -> Result<RedisCommand, anyhow::Error> {
    Ok(RedisCommand::Command())
}

fn parse_echo(elements: &[RedisType]) -> Result<RedisCommand, anyhow::Error> {
    if elements.len() != 2 {
        return Err(anyhow!("No argument for ECHO command"));
    }

    if let RedisType::BulkString(arg) = &elements[1] {
        Ok(RedisCommand::Echo(arg.clone()))
    } else {
        Err(anyhow!("ECHO argument is not a BulkString"))
    }
}

// https://redis.io/docs/latest/commands/set/#options
fn parse_set(elements: &[RedisType]) -> Result<RedisCommand, anyhow::Error> {
    if elements.len() < 3 {
        return Err(anyhow!("Not enough arguments for SET command"));
    }

    // EX seconds -- Set the specified expire time, in seconds (a positive integer).
    // PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).

    if let RedisType::BulkString(key) = &elements[1]
        && let RedisType::BulkString(value) = &elements[2]
    {
        let mut expiration_in_ms = 0;

        // SET key1 "value1" EX 60
        if elements.len() >= 5
            && let RedisType::BulkString(arg) = &elements[3]
            && let RedisType::BulkString(arg_value) = &elements[4]
        {
            if arg.to_uppercase() == "EX" {
                expiration_in_ms = 1000
                    * arg_value.parse::<u64>().with_context(|| {
                        format!("Can't convert EX value '{arg_value}' to number")
                    })?;
            } else if arg.to_uppercase() == "PX" {
                expiration_in_ms = arg_value
                    .parse::<u64>()
                    .with_context(|| format!("Can't convert PX value '{arg_value}' to number"))?;
            }
        }

        Ok(RedisCommand::Set {
            key: key.to_owned(),
            value: value.to_owned(),
            expiration_in_ms,
        })
    } else {
        Err(anyhow!("SET arguments are not BulkString"))
    }
}

fn parse_get(elements: &[RedisType]) -> Result<RedisCommand, anyhow::Error> {
    if elements.len() < 2 {
        return Err(anyhow!("No enough arguments for GET command"));
    }

    if let RedisType::BulkString(key) = &elements[1] {
        Ok(RedisCommand::Get {
            key: key.to_owned(),
        })
    } else {
        Err(anyhow!("GET argument is not a BulkString"))
    }
}

fn parse_rpush(elements: &[RedisType]) -> Result<RedisCommand, anyhow::Error> {
    if elements.len() < 3 {
        return Err(anyhow!("Not enough arguments for RPUSH command"));
    }

    if let RedisType::BulkString(key) = &elements[1] {
        let mut values = Vec::new();

        for element in &elements[2..] {
            if let RedisType::BulkString(value) = element {
                values.push(value.clone());
            } else if let RedisType::Integer(value) = element {
                values.push(value.to_string());
            } else {
                return Err(anyhow!("RPUSH argument is not BulkString or Integer"));
            }
        }

        Ok(RedisCommand::RPush {
            key: key.to_owned(),
            values,
        })
    } else {
        Err(anyhow!("RPUSH key is not BulkString"))
    }
}
