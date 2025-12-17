use std::{cell::RefCell, collections::HashMap, rc::Rc};

use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct ListLeftBlockingPopStorage {
    pub keys: Vec<String>,
    pub timeout: u64,
}

impl StorageRequest for ListLeftBlockingPopStorage {
    fn shard_keys(&self) -> Vec<&str> {
        let mut all_shards = Vec::new();

        for val in &self.keys {
            all_shards.push(val.as_str());
        }

        all_shards
    }

    fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        let mut map_ref = stored_data.borrow_mut();

        let response = StorageResponse::KeyValue {
            value: "hello, world!!!".to_string(),
        };

        response
    }
}
