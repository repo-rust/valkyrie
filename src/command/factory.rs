use crate::protocol::redis_serialization_protocol::RedisType;

pub enum RedisCommand {
    Ping(Option<String>),
}

impl RedisCommand {
    pub fn from_redis_type(redis_type: &RedisType) -> Option<RedisCommand> {
        match redis_type {
            RedisType::Array(elements) => {
                if elements.is_empty() {
                    return None;
                }

                match &elements[0] {
                    RedisType::BulkString(command_name) => {
                        if command_name.to_lowercase() == "ping" {
                            if elements.len() == 1 {
                                return Some(RedisCommand::Ping(None));
                            }

                            if elements.len() == 2 {
                                if let RedisType::BulkString(arg) = &elements[1] {
                                    return Some(RedisCommand::Ping(Some(arg.clone())));
                                }
                            }
                        }

                        None
                    }
                    _ => None,
                }
            }

            _ => None,
        }
    }
}
