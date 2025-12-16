use std::{cell::RefCell, collections::HashMap, rc::Rc};

use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug)]
pub struct ListLeftPushStorage {
    pub key: String,
    pub values: Vec<String>,
}

impl StorageRequest for ListLeftPushStorage {
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
                let new_length = original_values.len() + self.values.len();

                // Build a new vector with LPUSH semantics:
                // Elements are inserted at the head in the order they appear,
                // resulting in final list: [..., (last arg), ..., (first arg), previous...]
                let mut combined = Vec::with_capacity(new_length);
                combined.extend(self.values.iter().rev().cloned());
                combined.extend(original_values.iter().cloned());
                *original_values = combined;

                StorageResponse::ListLength(new_length)
            }
            Some(StorageValue::Str(_)) => StorageResponse::Failed(
                "Can't execute Left Push for a String value, should be List".to_string(),
            ),
            None => {
                // Create a new list with head insertion semantics
                let length = self.values.len();
                let mut new_list = Vec::with_capacity(length);
                new_list.extend(self.values.iter().rev().cloned());
                map_ref.insert(self.key.clone(), StorageValue::List(new_list));
                StorageResponse::ListLength(length)
            }
        }
    }
}
