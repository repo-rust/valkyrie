use std::{cell::RefCell, collections::HashMap, rc::Rc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug)]
pub struct ListLeftPopStorage {
    pub key: String,
    /// None = pop a single element
    /// Some(count) = pop up to `count` elements
    pub count: Option<usize>,
}

#[async_trait(?Send)]
impl StorageRequest for ListLeftPopStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        let mut map_ref = stored_data.borrow_mut();

        // Use a flag to remove the key after we finish mutably borrowing its value.
        let mut remove_empty_list = false;

        let response = match map_ref.get_mut(&self.key) {
            None => StorageResponse::Null,
            Some(StorageValue::Str(_)) => {
                StorageResponse::Failed(format!("'{}' is not a list.", self.key))
            }
            Some(StorageValue::List(values)) => {
                match self.count {
                    // Single element pop
                    None => {
                        match values.pop_front() {
                            Some(v) => {
                                if values.is_empty() {
                                    remove_empty_list = true;
                                }
                                StorageResponse::KeyValue { value: v }
                            }
                            None => {
                                // List exists but is empty; treat as nil and remove the key
                                remove_empty_list = true;
                                StorageResponse::Null
                            }
                        }
                    }
                    // Multi pop (up to count)
                    Some(count) => {
                        if count == 0 {
                            // Return empty array for zero count (no elements popped)
                            StorageResponse::ListValues {
                                values: Vec::with_capacity(0),
                            }
                        } else {
                            let elems_to_removed_cnt = count.min(values.len());

                            let mut out = Vec::with_capacity(elems_to_removed_cnt);
                            for _ in 0..elems_to_removed_cnt {
                                if let Some(popped_value) = values.pop_front() {
                                    out.push(popped_value);
                                } else {
                                    tracing::warn!(
                                        "None value popped from list using LPOP, but should not"
                                    );
                                    break;
                                }
                            }
                            if values.is_empty() {
                                remove_empty_list = true;
                            }
                            StorageResponse::ListValues { values: out }
                        }
                    }
                }
            }
        };

        if remove_empty_list {
            map_ref.remove(&self.key);
        }

        response
    }
}
