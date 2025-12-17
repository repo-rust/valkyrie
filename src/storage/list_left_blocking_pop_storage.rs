use std::{cell::RefCell, collections::HashMap, rc::Rc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct ListLeftBlockingPopStorage {
    pub key: String,
}

#[async_trait(?Send)]
impl StorageRequest for ListLeftBlockingPopStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        let mut map_ref = stored_data.borrow_mut();

        loop {
            match map_ref.get_mut(&self.key) {
                Some(StorageValue::List(values)) => {
                    if !values.is_empty() {
                        let popped_value = values.pop_front().unwrap();
                        return StorageResponse::ValueFromList {
                            value: popped_value,
                            list_name: self.key.clone(),
                        };
                    }
                }

                Some(StorageValue::Str(_)) => {
                    return StorageResponse::Failed(format!("'{}' is not a list.", self.key));
                }
                _ => {}
            };

            // Wait till other async tasks potentially add value to list
            //notifier.notified().await;
        }
    }
}
