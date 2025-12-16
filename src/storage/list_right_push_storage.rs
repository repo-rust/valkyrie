use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
};

use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug)]
pub struct ListRightPushStorage {
    pub key: String,
    pub values: Vec<String>,
}

impl StorageRequest for ListRightPushStorage {
    fn key(&self) -> &str {
        &self.key
    }

    fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        let mut map_ref = stored_data.borrow_mut();

        match map_ref.get_mut(&self.key) {
            Some(StorageValue::List(original_values)) => {
                // Push to the tail for each provided value
                for v in &self.values {
                    original_values.push_back(v.clone());
                }
                StorageResponse::ListLength(original_values.len())
            }
            Some(StorageValue::Str(_)) => StorageResponse::Failed(
                "Can't execute Right Push for a String value, should be List".to_string(),
            ),
            None => {
                let length = self.values.len();
                let mut deque = VecDeque::with_capacity(length);
                for single_value in &self.values {
                    deque.push_back(single_value.clone());
                }
                map_ref.insert(self.key.clone(), StorageValue::List(deque));
                StorageResponse::ListLength(length)
            }
        }
    }
}
