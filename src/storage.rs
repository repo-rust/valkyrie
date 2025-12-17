use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    hash::DefaultHasher,
    rc::Rc,
    thread::{self},
};

use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio::{sync::oneshot, task::LocalSet};

use crate::utils::thread_utils::pin_current_thread_to_cpu;
pub mod get_storage;
pub use get_storage::GetStorage;
pub mod set_storage;
pub use set_storage::SetStorage;
pub mod list_right_push_storage;
pub use list_right_push_storage::ListRightPushStorage;
pub mod list_left_push_storage;
pub use list_left_push_storage::ListLeftPushStorage;

pub mod list_left_blocking_pop_storage;
pub use list_left_blocking_pop_storage::ListLeftBlockingPopStorage;

pub mod list_left_pop_storage;
pub use list_left_pop_storage::ListLeftPopStorage;
pub mod list_range_storage;
pub use list_range_storage::ListRangeStorage;
pub mod list_length_storage;
pub use list_length_storage::ListLengthStorage;

pub struct StorageEngine {
    storage_shards: Vec<StorageShard>,
}

struct StorageShard {
    commands_channel: tokio::sync::mpsc::UnboundedSender<StorageCommandEnvelope>,
}

// Do NOT derive Debug because the Request holds a trait object
enum StorageCommandEnvelope {
    Request {
        request: Box<dyn StorageRequest + Send>,
        reply_channel: oneshot::Sender<StorageCommandEnvelope>,
    },
    Response {
        response: StorageResponse,
    },
}

// Trait-based request interface, enabling separate request structs
#[async_trait(?Send)]
pub trait StorageRequest: Send {
    fn key(&self) -> &str;

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse;
}

#[derive(Debug)]
pub enum StorageResponse {
    KeyValue { value: String },
    ValueFromList { value: String, list_name: String },
    Null,
    Success,
    ListLength(usize),
    ListValues { values: Vec<String> },
    Failed(String),
}

#[derive(Debug)]
pub enum StorageValue {
    Str(String),
    List(VecDeque<String>),
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
        let stored_data = Rc::new(RefCell::new(HashMap::new()));
        let delayed_tasks = Rc::new(RefCell::new(HashMap::new()));
        tracing::debug!("Started");

        while let Some(storage_command) = queue_receiver.recv().await {
            if let StorageCommandEnvelope::Request {
                request,
                reply_channel,
            } = storage_command
            {
                tracing::debug!("Engine handling storage request");

                let response = request.handle(&stored_data, &delayed_tasks).await;

                if reply_channel
                    .send(StorageCommandEnvelope::Response { response })
                    .is_err()
                {
                    tracing::warn!(
                        "Failed to send reply: oneshot reply channel probably cancelled"
                    );
                }
            } else {
                unreachable!(
                    "Incorrect 'StorageCommandEnvelope' type received expected 'Request' but found 'Response'"
                )
            }
        }
    }

    pub async fn execute<R>(&self, storage_request: R) -> anyhow::Result<StorageResponse>
    where
        R: StorageRequest + 'static,
    {
        // this channel will be used like a future/promise
        let (sender, receiver) = oneshot::channel::<StorageCommandEnvelope>();

        let storage_thread = self.find_shard_for_key(storage_request.key());

        storage_thread
            .commands_channel
            .send(StorageCommandEnvelope::Request {
                request: Box::new(storage_request),
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
    /// All request variants use the request key, ensuring that reads/writes
    /// go to the same shard where the data for that key is stored.
    fn find_shard_for_key(&self, key: &str) -> &StorageShard {
        let shard_idx = self.hash_string(key) % self.storage_shards.len();
        &self.storage_shards[shard_idx]
    }

    fn hash_string(&self, value: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish() as usize
    }
}
