use std::{cell::RefCell, collections::HashMap, rc::Rc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct GetStorage {
    pub key: String,
}

#[async_trait(?Send)]
impl StorageRequest for GetStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
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
