use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};

use async_trait::async_trait;
use tokio::{task::JoinHandle, time::sleep};

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug)]
pub struct SetStorage {
    pub key: String,
    pub value: String,
    pub expiration_in_ms: u64,
}

#[async_trait(?Send)]
impl StorageRequest for SetStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        {
            // short-lived mutable borrow; do not await while borrowed
            stored_data
                .borrow_mut()
                .insert(self.key.clone(), StorageValue::Str(self.value.clone()));
            if let Some(prev_exp_handle) = delayed_tasks.borrow_mut().remove(&self.key) {
                // abort any previously created expiration tasks if any
                tracing::debug!("Previous expiration aborted");
                prev_exp_handle.abort();
            }
        }

        if self.expiration_in_ms > 0 {
            // Delete expired key after 'expiration_in_ms' milliseconds delay
            let key_copy = self.key.clone();

            let local_map_copy = Rc::clone(stored_data);
            let delayed_tasks_copy = Rc::clone(delayed_tasks);
            let exp_ms = self.expiration_in_ms;

            let exp_handler = tokio::task::spawn_local(async move {
                sleep(Duration::from_millis(exp_ms)).await;
                local_map_copy.borrow_mut().remove(&key_copy);
                tracing::debug!("Key {key_copy} expired and was deleted.");
            });

            delayed_tasks_copy
                .borrow_mut()
                .insert(self.key.clone(), exp_handler);
        }

        StorageResponse::Success
    }
}
