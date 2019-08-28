use crate::cache::ImMutCache;
use crate::error::TdbError;
use crate::meta::InnerTable;
use crate::object::{Object, Entry, ObjectId};
use crate::storage::DataLogFileReader;
use crate::transaction::TimeStamp;
use std::sync::Arc;

pub struct ImMutTable {
    table: Arc<InnerTable>,
    data_reader: DataLogFileReader,
    cache: ImMutCache,
}

impl ImMutTable {
    pub fn new(table: Arc<InnerTable>, data_reader: DataLogFileReader, cache: ImMutCache) -> Self {
        Self {
            table,
            data_reader,
            cache,
        }
    }
    pub fn get_obj(&mut self, oid: ObjectId, ts: TimeStamp) -> Result<Arc<Object>, TdbError> {
        let (pos, obj) = self.table.get(oid, ts, &mut self.data_reader)?;
        if !obj.is::<Entry>() {
            self.cache.insert(pos, obj.clone());
        }
        Ok(obj)
    }
}
