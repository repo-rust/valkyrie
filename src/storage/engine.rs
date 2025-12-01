use std::{
    collections::HashMap,
    hash::DefaultHasher,
    thread::{self},
};

use std::hash::{Hash, Hasher};

use tokio::sync::oneshot;

use crate::utils::thread_utils::pin_current_thread_to_cpu;

pub struct StorageEngine {
    storage_shards: Vec<StorageShard>,
}

struct StorageShard {
    commands_channel: crossbeam_channel::Sender<StorageCommandEnvelope>,
}

#[derive(Debug)]
enum StorageCommandEnvelope {
    Request {
        request: StorageRequest,
        reply_channel: oneshot::Sender<StorageCommandEnvelope>,
    },
    Response {
        response: StorageResponse,
    },
}

#[derive(Debug)]
pub enum StorageRequest {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        expiration_in_ms: u64,
    },
}

#[derive(Debug)]
pub enum StorageResponse {
    KeyValue { value: String },
    Nill,
    Success,
}

impl StorageEngine {
    pub fn new(shards: usize, core_affinity_range: std::ops::Range<usize>) -> Self {
        // shards count should be greater than 0, convert to 1 if 0
        let shards = if shards == 0 { 1 } else { shards };

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

                        for storage_command in queue_receiver.iter() {
                            if let StorageCommandEnvelope::Request {
                                request,
                                reply_channel,
                            } = storage_command
                            {
                                tracing::debug!("Engine handling storage request {:?}", request);

                                match request {
                                    StorageRequest::Get { key } => {
                                        let response_value = match local_map.get(&key) {
                                            Some(value) => StorageResponse::KeyValue {
                                                value: value.to_string(),
                                            },
                                            None => StorageResponse::Nill,
                                        };

                                        Self::send_reply(reply_channel, response_value);

                                    }
                                    StorageRequest::Set { key, value, expiration_in_ms } => {
                                        local_map.insert(key, value);

                                        if expiration_in_ms > 0 {
                                            tracing::debug!("expiration_in_ms = {expiration_in_ms}");
                                        }

                                        Self::send_reply(reply_channel,  StorageResponse::Success);
                                    }
                                }
                            }
                            else {
                                unreachable!("Incorrect 'StorageCommandEnvelope' type received expected 'Request' but found 'Response'")
                            }
                        }
                    });
                })
                .expect("Can't spawn storage-shard thread");

            storage_threads.push(StorageShard {
                commands_channel: queue_sender,
            });
        }

        Self {
            storage_shards: storage_threads,
        }
    }

    fn send_reply(
        reply_channel: oneshot::Sender<StorageCommandEnvelope>,
        storage_reponse: StorageResponse,
    ) {
        if let Err(error) = reply_channel.send(StorageCommandEnvelope::Response {
            response: storage_reponse,
        }) {
            tracing::error!("Failed to send reply {error:?}");
        }
    }

    pub async fn execute(
        &self,
        storage_request: StorageRequest,
    ) -> anyhow::Result<StorageResponse> {
        // this channel will be used like a future/promise
        let (sender, receiver) = oneshot::channel::<StorageCommandEnvelope>();

        let storage_thread = self.find_shard_for_request(&storage_request);

        // TODO: handle properly all errors here
        storage_thread
            .commands_channel
            .send(StorageCommandEnvelope::Request {
                request: storage_request,
                reply_channel: sender,
            })?;

        let response_envelope = receiver.await?;

        if let StorageCommandEnvelope::Response { response } = response_envelope {
            Ok(response)
        } else {
            unreachable!(
                "Invalid 'StorageCommandEnvelope' response type received, expected 'Response' but found 'Request'"
            );
        }
    }

    /// Selects the storage shard for a request by hashing the key.
    ///
    /// The shard index is computed as `hash(key) % shard_count`.
    /// Both GET and SET variants use the request key, ensuring that reads
    /// go to the same shard where writes for that key were stored. This
    /// keeps per-key data localized and avoids cross-shard coordination.
    ///
    /// Implementation details:
    /// - Uses `DefaultHasher` (SipHash) to hash the key via `hash_string`.
    /// - Reduces the hash with modulo `self.storage_shards.len()` to obtain
    ///   a stable index into the shard vector.
    /// - The mapping is stable for a fixed shard count; changing the number
    ///   of shards will remap keys (this is not consistent hashing).
    fn find_shard_for_request(&self, storage_request: &StorageRequest) -> &StorageShard {
        let shard_idx = match storage_request {
            StorageRequest::Get { key } => self.hash_string(key) % self.storage_shards.len(),
            StorageRequest::Set {
                key,
                value: _,
                expiration_in_ms: _,
            } => self.hash_string(key) % self.storage_shards.len(),
        };

        &self.storage_shards[shard_idx]
    }

    fn hash_string(&self, value: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish() as usize
    }
}
