use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::storage::LIST_NOTIFIERS;
use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug)]
pub struct ListLeftBlockingPopStorage {
    pub key: String,
}

#[async_trait(?Send)]
impl StorageRequest for ListLeftBlockingPopStorage {
    fn key(&self) -> &str {
        &self.key
    }

    fn commit(&self, stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>) {
        let mut map_ref = stored_data.borrow_mut();

        if let Some(StorageValue::List(values)) = map_ref.get_mut(&self.key) {
            values.pop_front();
        } else {
            tracing::warn!("commit stage failed for BLPOP")
        }
    }

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        loop {
            // Get or create per-key notifier for this shard thread
            let notifier = LIST_NOTIFIERS.with(|cell| {
                let mut m = cell.borrow_mut();
                m.entry(self.key.clone())
                    .or_insert_with(|| Rc::new(Notify::new()))
                    .clone()
            });

            // Acquire awaitable BEFORE checking state to avoid missed wakeups
            let notified = notifier.notified();

            {
                let mut map_ref = stored_data.borrow_mut();

                // Try to pop without holding a borrow across .await
                if let Some(popped_value) = {
                    match map_ref.get_mut(&self.key) {
                        Some(StorageValue::List(values)) => {
                            if !values.is_empty() {
                                values.front()
                            } else {
                                None
                            }
                        }
                        Some(StorageValue::Str(_)) => {
                            return StorageResponse::Failed(format!(
                                "'{}' is not a list.",
                                self.key
                            ));
                        }
                        _ => None,
                    }
                } {
                    return StorageResponse::ValueFromList {
                        value: popped_value.to_string(),
                        list_name: self.key.clone(),
                    };
                }
            }

            // Wait until someone pushes into the list
            notified.await;
        }
    }
}
