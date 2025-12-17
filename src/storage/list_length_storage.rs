use std::{cell::RefCell, collections::HashMap, rc::Rc};

use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct ListLengthStorage {
    pub key: String,
}

impl StorageRequest for ListLengthStorage {
    fn shard_keys(&self) -> Vec<&str> {
        vec![&self.key]
    }

    fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        match stored_data.borrow().get(&self.key) {
            Some(StorageValue::List(values)) => StorageResponse::ListLength(values.len()),
            Some(_) => StorageResponse::Failed(format!("'{}' is not a list.", self.key)),
            None => StorageResponse::ListLength(0),
        }
    }
}
