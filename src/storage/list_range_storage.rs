use std::cmp::min;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use super::{StorageRequest, StorageResponse, StorageValue};

#[derive(Debug, Clone)]
pub struct ListRangeStorage {
    pub key: String,
    pub start: i32,
    pub end: i32,
}

impl ListRangeStorage {
    fn normalize_list_range_index(index: i32, values_len_usize: usize, start_index: bool) -> usize {
        let values_len = values_len_usize as i32;
        let mut index = index;

        if index < 0 {
            index += values_len;

            if index < 0 {
                index = 0;
            }
        } else {
            index = min(
                index,
                if start_index {
                    values_len
                } else {
                    values_len - 1
                },
            );
        }

        assert!(index >= 0);
        index as usize
    }
}

#[async_trait(?Send)]
impl StorageRequest for ListRangeStorage {
    fn key(&self) -> &str {
        &self.key
    }

    async fn handle(
        &self,
        stored_data: &Rc<RefCell<HashMap<String, StorageValue>>>,
        _delayed_tasks: &Rc<RefCell<HashMap<String, JoinHandle<()>>>>,
    ) -> StorageResponse {
        match stored_data.borrow().get(&self.key) {
            Some(StorageValue::List(values)) => {
                if values.is_empty() {
                    StorageResponse::ListValues {
                        values: Vec::with_capacity(0),
                    }
                } else {
                    /*
                    If start is larger than the end of the list, an empty list is returned.
                    If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
                    These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is the last element of the list, -2 the penultimate, and so on.
                    */
                    let start = Self::normalize_list_range_index(self.start, values.len(), true);
                    let end = Self::normalize_list_range_index(self.end, values.len(), false);

                    tracing::debug!("[{start}..{end}]");

                    if start == values.len() || start > end {
                        StorageResponse::ListValues {
                            values: Vec::with_capacity(0),
                        }
                    } else {
                        // Collect a contiguous slice from VecDeque by indexing
                        // Note: VecDeque supports indexing by logical index
                        let mut out = Vec::with_capacity(end - start + 1);
                        for single_value in values.iter().skip(start).take(end + 1) {
                            out.push(single_value.clone());
                        }
                        StorageResponse::ListValues { values: out }
                    }
                }
            }
            Some(_) => StorageResponse::Failed(format!("'{}' is not a list.", self.key)),
            None => StorageResponse::Failed(format!("No list found with name '{}'", self.key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ListRangeStorage;

    fn to_strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn normalize_start_index_cases() {
        let values = to_strings(&["a", "b", "c", "d", "e"]);
        // len = 5
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(-1, values.len(), true),
            4
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(-10, values.len(), true),
            0
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(0, values.len(), true),
            0
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(3, values.len(), true),
            3
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(5, values.len(), true),
            5
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(15, values.len(), true),
            5
        );
    }

    #[test]
    fn normalize_end_index_cases() {
        let values = to_strings(&["a", "b", "c", "d", "e"]);
        // len = 5, last valid index = 4
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(-1, values.len(), false),
            4
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(-10, values.len(), false),
            0
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(0, values.len(), false),
            0
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(4, values.len(), false),
            4
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(5, values.len(), false),
            4
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(15, values.len(), false),
            4
        );
    }

    #[test]
    fn normalize_empty_values_start_index() {
        let values: Vec<String> = Vec::new();
        // For empty values, start index always normalizes to 0
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(-1, values.len(), true),
            0
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(0, values.len(), true),
            0
        );
        assert_eq!(
            ListRangeStorage::normalize_list_range_index(10, values.len(), true),
            0
        );
    }
}
