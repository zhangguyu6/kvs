use crate::cache::IndexCache;
use crate::object::{Object, ObjectId, ObjectTable};
use crate::storage::{BlockDev, RawBlockDev};
use crate::transaction::{Context, TimeStamp};
use std::sync::Arc;

pub struct ObjectAccess<'a, C: IndexCache, D: RawBlockDev + Unpin> {
    ts: TimeStamp,
    cache: &'a C,
    dev: &'a BlockDev<D>,
    obj_table: &'a ObjectTable,
}

impl<'a, C: IndexCache, D: RawBlockDev + Unpin> ObjectAccess<'a, C, D> {
    fn get(&self, oid: ObjectId) -> Option<Arc<Object>> {
        if let Some(obj) = self.cache.get(oid, self.ts) {
            Some(obj)
        } else {
            if let Some(obj) = self.obj_table.get(oid, self.ts, self.dev) {
                self.cache.insert(oid, self.ts, obj.clone());

                Some(obj)
            } else {
                None
            }
        }
    }
}
