use std::{
    cell::RefCell,
    collections::HashMap,
    hash::DefaultHasher,
    rc::Rc,
    thread::{self},
    time::Duration,
};

use std::hash::{Hash, Hasher};

use tokio::time::sleep;
use tokio::{sync::oneshot, task::LocalSet};

use crate::utils::thread_utils::pin_current_thread_to_cpu;

pub struct StorageEngine {
    storage_shards: Vec<StorageShard>,
}

struct StorageShard {
    commands_channel: tokio::sync::mpsc::UnboundedSender<StorageCommandEnvelope>,
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
            let (sender, receiver) =
                tokio::sync::mpsc::unbounded_channel::<StorageCommandEnvelope>();

            let core_affinity_range_copy = core_affinity_range.clone();

            let _ = thread::Builder::new()
                .name(format!("storage-shard-{shard_id}"))
                .spawn(move || {
                    pin_current_thread_to_cpu(shard_id, core_affinity_range_copy);

                    let local = LocalSet::new();

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .enable_time()
                        .build()
                        .expect("Failed to create tokio runtime");

                    rt.block_on(local.run_until(async move {
                        Self::shard_loop(receiver).await;
                    }));
                })
                .expect("Can't spawn storage-shard thread");

            storage_threads.push(StorageShard {
                commands_channel: sender,
            });
        }

        Self {
            storage_shards: storage_threads,
        }
    }

    async fn shard_loop(
        mut queue_receiver: tokio::sync::mpsc::UnboundedReceiver<StorageCommandEnvelope>,
    ) {
        let local_map: Rc<RefCell<HashMap<String, String>>> = Rc::new(RefCell::new(HashMap::new()));

        tracing::debug!("Started");

        while let Some(storage_command) = queue_receiver.recv().await {
            if let StorageCommandEnvelope::Request {
                request,
                reply_channel,
            } = storage_command
            {
                tracing::debug!("Engine handling storage request {:?}", request);

                match request {
                    StorageRequest::Get { key } => {
                        let response_value = {
                            match local_map.borrow().get(&key) {
                                Some(value) => StorageResponse::KeyValue {
                                    value: value.clone(),
                                },
                                None => StorageResponse::Nill,
                            }
                        };

                        Self::send_reply(reply_channel, response_value);
                    }
                    StorageRequest::Set {
                        key,
                        value,
                        expiration_in_ms,
                    } => {
                        {
                            // short-lived mutable borrow; do not await while borrowed
                            local_map.borrow_mut().insert(key.clone(), value);
                        }

                        if expiration_in_ms > 0 {
                            // Delete expired key after 'expiration_in_ms' milliseconds delay
                            let key_for_exp = key.clone();
                            let map_for_exp = Rc::clone(&local_map);

                            tokio::task::spawn_local(async move {
                                sleep(Duration::from_millis(expiration_in_ms)).await;
                                map_for_exp.borrow_mut().remove(&key_for_exp);
                                tracing::debug!("Key {key_for_exp} expired and was deleted.");
                            });
                        }

                        Self::send_reply(reply_channel, StorageResponse::Success);
                    }
                }
            } else {
                unreachable!(
                    "Incorrect 'StorageCommandEnvelope' type received expected 'Request' but found 'Response'"
                )
            }
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
            })
            .map_err(|_| anyhow::anyhow!("failed to send to storage shard: channel closed"))?;

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
