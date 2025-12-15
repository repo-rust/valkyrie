use std::{
    cell::RefCell,
    cmp::min,
    collections::HashMap,
    hash::DefaultHasher,
    rc::Rc,
    thread::{self},
    time::Duration,
};

use std::hash::{Hash, Hasher};

use tokio::{sync::oneshot, task::LocalSet};
use tokio::{task::JoinHandle, time::sleep};

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

    ListRightPush {
        key: String,
        values: Vec<String>,
    },

    ListRange {
        key: String,
        start: i32,
        end: i32,
    },
}

#[derive(Debug)]
pub enum StorageResponse {
    KeyValue { value: String },
    Nill,
    Success,
    ListLength(usize),
    ListValues { values: Vec<String> },
    Failed(String),
}

#[derive(Debug)]
enum StorageValue {
    Str(String),
    List(Vec<String>),
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
        let stored_data: Rc<RefCell<HashMap<String, StorageValue>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let delayed_tasks: Rc<RefCell<HashMap<String, JoinHandle<()>>>> =
            Rc::new(RefCell::new(HashMap::new()));

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
                            match stored_data.borrow().get(&key) {
                                Some(StorageValue::Str(value)) => StorageResponse::KeyValue {
                                    value: value.clone(),
                                },
                                Some(StorageValue::List(_)) => {
                                    // Currently we do not have List support in the public API
                                    StorageResponse::Nill
                                }

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
                            stored_data
                                .borrow_mut()
                                .insert(key.clone(), StorageValue::Str(value));
                            if let Some(prev_exp_handle) = delayed_tasks.borrow_mut().remove(&key) {
                                // abort any previously created expiration tasks if any
                                tracing::debug!("Previous expiration aborted");
                                prev_exp_handle.abort();
                            }
                        }

                        if expiration_in_ms > 0 {
                            // Delete expired key after 'expiration_in_ms' milliseconds delay
                            let key_copy = key.clone();

                            let local_map_copy = Rc::clone(&stored_data);
                            let delayed_tasks_copy = Rc::clone(&delayed_tasks);

                            let exp_handler = tokio::task::spawn_local(async move {
                                sleep(Duration::from_millis(expiration_in_ms)).await;
                                local_map_copy.borrow_mut().remove(&key_copy);
                                tracing::debug!("Key {key_copy} expired and was deleted.");
                            });

                            let key_for_exp = key.clone();

                            delayed_tasks_copy
                                .borrow_mut()
                                .insert(key_for_exp, exp_handler);
                        }

                        Self::send_reply(reply_channel, StorageResponse::Success);
                    }
                    StorageRequest::ListRightPush { key, values } => {
                        let local_map_copy = Rc::clone(&stored_data);

                        // Mutably borrow the map and push without cloning elements
                        let mut map_ref = local_map_copy.borrow_mut();

                        let response = match map_ref.get_mut(&key) {
                            Some(StorageValue::List(original_values)) => {
                                let new_length = original_values.len() + values.len();
                                // Move elements from `values` into the existing vector, no cloning
                                original_values.extend(values);
                                StorageResponse::ListLength(new_length)
                            }
                            Some(StorageValue::Str(_)) => {
                                // Key exists but is not a list
                                StorageResponse::Failed(
                                    "Can't execute Right Push for a String value, should be List"
                                        .to_string(),
                                )
                            }
                            None => {
                                let length = values.len();
                                map_ref.insert(key.clone(), StorageValue::List(values));
                                StorageResponse::ListLength(length)
                            }
                        };

                        Self::send_reply(reply_channel, response);
                    }

                    StorageRequest::ListRange { key, start, end } => {
                        let local_map_copy = Rc::clone(&stored_data);

                        let response = match local_map_copy.borrow().get(&key) {
                            Some(StorageValue::List(values)) => {
                                if values.is_empty() {
                                    StorageResponse::ListValues {
                                        values: Vec::with_capacity(0),
                                    }
                                } else {
                                    /*
                                    If start is larger than the end of the list, an empty list is returned.
                                    If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
                                    These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is the last element of the list, -2 the penultimate, and so on.
                                     */
                                    let start =
                                        Self::normalize_list_range_index(start, values, true);
                                    let end = Self::normalize_list_range_index(end, values, false);

                                    tracing::debug!("[{start}..{end}]");

                                    if start == values.len() || start > end {
                                        StorageResponse::ListValues {
                                            values: Vec::with_capacity(0),
                                        }
                                    } else {
                                        StorageResponse::ListValues {
                                            values: values[start..=end].to_vec(),
                                        }
                                    }
                                }
                            }
                            Some(_) => StorageResponse::Failed(format!("'{}' is not a list.", key)),
                            None => StorageResponse::Failed(format!(
                                "No list found with name '{}'",
                                key
                            )),
                        };

                        Self::send_reply(reply_channel, response);
                    }
                }
            } else {
                unreachable!(
                    "Incorrect 'StorageCommandEnvelope' type received expected 'Request' but found 'Response'"
                )
            }
        }
    }

    fn normalize_list_range_index(index: i32, values: &[String], start_index: bool) -> usize {
        let values_len = values.len() as i32;
        let mut index = index;

        if index < 0 {
            index += values_len;

            if index < 0 {
                index = 0;
            }
        } else {
            index = min(
                index,
                if start_index {
                    values_len
                } else {
                    values_len - 1
                },
            );
        }

        assert!(index >= 0);
        index as usize
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
            StorageRequest::Get { key }
            | StorageRequest::Set { key, .. }
            | StorageRequest::ListRightPush { key, .. }
            | StorageRequest::ListRange { key, .. } => {
                self.hash_string(key) % self.storage_shards.len()
            }
        };

        &self.storage_shards[shard_idx]
    }

    fn hash_string(&self, value: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish() as usize
    }
}
