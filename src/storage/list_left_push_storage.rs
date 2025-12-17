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
pub struct ListLeftPushStorage {
    pub key: String,
    pub values: Vec<String>,
}

#[async_trait(?Send)]
impl StorageRequest for ListLeftPushStorage {
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
                    // Push to the head for each provided value in order
                    // LPUSH a b c -> final list [c, b, a, ...]
                    for v in &self.values {
                        original_values.push_front(v.clone());
                    }
                    (StorageResponse::ListLength(original_values.len()), true)
                }
                Some(StorageValue::Str(_)) => (
                    StorageResponse::Failed(
                        "Can't execute Left Push for a String value, should be List".to_string(),
                    ),
                    false,
                ),
                None => {
                    // Create a new deque and push to head in order
                    let length = self.values.len();
                    let mut deque = VecDeque::with_capacity(length);
                    for single_value in &self.values {
                        deque.push_front(single_value.clone());
                    }
                    map_ref.insert(self.key.clone(), StorageValue::List(deque));
                    (StorageResponse::ListLength(length), true)
                }
            }
        };

        // Notify one waiter if we actually added items to a list
        if should_notify
            && let Some(notifier) =
                LIST_NOTIFIERS.with(|cell| cell.borrow().get(&key_clone).cloned())
        {
            notifier.notify_one();
        }

        response
    }
}
