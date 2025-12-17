use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
};

use crate::storage::LIST_NOTIFIERS;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug)]
pub struct ListRightPushStorage {
    pub key: String,
    pub values: Vec<String>,
}

#[async_trait(?Send)]
impl StorageRequest for ListRightPushStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        let key_clone = self.key.clone();

        // Perform mutation while holding the map borrow, but compute the response and whether to notify
        let (response, should_notify) = {
            let mut map_ref = stored_data.borrow_mut();
            match map_ref.get_mut(&self.key) {
                Some(StorageValue::List(original_values)) => {
                    // Push to the tail for each provided value
                    for v in &self.values {
                        original_values.push_back(v.clone());
                    }
                    (StorageResponse::ListLength(original_values.len()), true)
                }
                Some(StorageValue::Str(_)) => (
                    StorageResponse::Failed(
                        "Can't execute Right Push for a String value, should be List".to_string(),
                    ),
                    false,
                ),
                None => {
                    let length = self.values.len();
                    let mut deque = VecDeque::with_capacity(length);
                    for single_value in &self.values {
                        deque.push_back(single_value.clone());
                    }
                    map_ref.insert(self.key.clone(), StorageValue::List(deque));
                    (StorageResponse::ListLength(length), true)
                }
            }
        };

        // Notify ALL waiters if we actually added items to a list
        if should_notify
            && let Some(notifier) =
                LIST_NOTIFIERS.with(|cell| cell.borrow().get(&key_clone).cloned())
        {
            notifier.notify_waiters();
        }

        response
    }
}
