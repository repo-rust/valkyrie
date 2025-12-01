use std::{
    collections::HashMap,
    thread::{self},
};

use tokio::sync::oneshot;

use crate::{
    command::factory::RedisCommand, protocol::redis_serialization_protocol::RedisType,
    utils::thread_utils::pin_current_thread_to_cpu,
};

pub struct StorageEngine {
    storage_shards: Vec<StorageShard>,
}

struct StorageShard {
    request_channel: crossbeam_channel::Sender<StorageCommandEnvelope>,
}

#[derive(Debug)]
pub struct StorageCommandEnvelope {
    command: Option<RedisCommand>,
    response_channel: Option<oneshot::Sender<StorageCommandEnvelope>>,
    pub response: Option<RedisType>,
}

impl StorageEngine {
    pub fn new(shards: usize, core_affinity_range: std::ops::Range<usize>) -> Self {
        let mut storage_threads = Vec::with_capacity(shards);

        for shard_id in 0..shards {
            let (queue_sender, queue_receiver) =
                crossbeam_channel::unbounded::<StorageCommandEnvelope>();

            let core_affinity_range_copy = core_affinity_range.clone();

            let _ = thread::Builder::new()
                .name(format!("storage-shard-{shard_id}"))
                .spawn(move || {
                    pin_current_thread_to_cpu(shard_id, core_affinity_range_copy);

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .enable_time()
                        .build()
                        .expect("Failed to create tokio runtime");

                    rt.block_on(async move {
                        let mut local_map: HashMap<String, String> = HashMap::new();

                        tracing::debug!("Started");

                        for storage_request in queue_receiver.iter() {
                            let redis_command = storage_request.command.unwrap();

                            match redis_command {
                                RedisCommand::Get { key } => {
                                    let value_by_key = match local_map.get(&key) {
                                        Some(value) => value,
                                        None => "Not found",
                                    };

                                    let res = StorageCommandEnvelope {
                                        command: None,
                                        response_channel: None,
                                        response: Some(RedisType::SimpleString(
                                            value_by_key.to_owned(),
                                        )),
                                    };

                                    storage_request
                                        .response_channel
                                        .unwrap()
                                        .send(res)
                                        .expect("Failed to send response");
                                }
                                RedisCommand::Set { key, value } => {
                                    local_map.insert(key, value);

                                    let res = StorageCommandEnvelope {
                                        command: None,
                                        response_channel: None,
                                        response: Some(RedisType::SimpleString("OK".to_string())),
                                    };

                                    storage_request
                                        .response_channel
                                        .unwrap()
                                        .send(res)
                                        .expect("Failed to send response");
                                }
                                _ => {
                                    tracing::warn!("Unsupported redis command {:?}", redis_command)
                                }
                            }
                        }
                    });
                })
                .expect("Can't spawn storage-shard thread");

            storage_threads.push(StorageShard {
                request_channel: queue_sender,
            });
        }

        Self {
            storage_shards: storage_threads,
        }
    }

    pub async fn execute(
        &self,
        redis_command: RedisCommand,
    ) -> anyhow::Result<StorageCommandEnvelope> {
        let (sender, receiver) = oneshot::channel::<StorageCommandEnvelope>();

        let request = StorageCommandEnvelope {
            command: Some(redis_command),
            response_channel: Some(sender),
            response: None,
        };

        let storage_thread = &self.storage_shards[0];

        // TODO: handle properly all errors here
        storage_thread.request_channel.send(request)?;

        let response = receiver.await?;
        Ok(response)
    }
}
