use std::{cell::RefCell, collections::HashMap, rc::Rc};

use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct GetStorage {
    pub key: String,
}

impl StorageRequest for GetStorage {
    fn shard_keys(&self) -> Vec<&str> {
        vec![&self.key]
    }

    fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        match stored_data.borrow().get(&self.key) {
            Some(StorageValue::Str(value)) => StorageResponse::KeyValue {
                value: value.clone(),
            },
            Some(StorageValue::List(_)) => {
                // Currently we do not have List support in the public GET API
                StorageResponse::Null
            }
            None => StorageResponse::Null,
        }
    }
}
