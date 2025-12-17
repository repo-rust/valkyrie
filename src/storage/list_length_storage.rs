use std::{cell::RefCell, collections::HashMap, rc::Rc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct ListLengthStorage {
    pub key: String,
}

#[async_trait(?Send)]
impl StorageRequest for ListLengthStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
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
