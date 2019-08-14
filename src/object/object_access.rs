use crate::cache::IndexCache;
use crate::object::{Object, ObjectId, ObjectTable};
use crate::storage::{BlockDev, RawBlockDev};
use crate::transaction::{Context, TimeStamp};
use crate::tree::Entry;
use std::sync::Arc;

pub struct ObjectAccess<'a, C: IndexCache, D: RawBlockDev + Unpin> {
    pub ts: TimeStamp,
    pub cache: &'a C,
    pub dev: &'a BlockDev<D>,
    pub obj_table: &'a ObjectTable,
}

impl<'a, C: IndexCache, D: RawBlockDev + Unpin> Clone for ObjectAccess<'a,C,D> {
    fn clone(&self) -> Self {
        Self {
            ts:self.ts,
            cache:self.cache,
            dev:self.dev,
            obj_table:self.obj_table
        }
    }
}

impl<'a, C: IndexCache, D: RawBlockDev + Unpin> ObjectAccess<'a, C, D> {
    pub fn new(ctx: &'a Context<C, D>) -> Self {
        Self {
            ts: ctx.ts,
            cache: &ctx.cache,
            dev: &ctx.dev,
            obj_table: &ctx.obj_table,
        }
    }
    pub fn get(&self, oid: ObjectId) -> Option<Arc<Object>> {
        if let Some(obj) = self.cache.get(oid, self.ts) {
            Some(obj)
        } else {
            if let Some(obj) = self.obj_table.get(oid, self.ts, self.dev) {
                // only cache index node
                if !obj.is::<Entry>() {
                    self.cache.insert(oid, self.ts, obj.clone());
                }
                Some(obj)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BackgroundCacheInner;
    use crate::object::*;
    use crate::storage::{Dummy, ObjectPos};
    use crate::tree::Entry;

    #[test]
    fn test_object_access() {
        let dummy = Dummy {};
        let dev = BlockDev::new(dummy);
        let obj_table = ObjectTable::with_capacity(1 << 16);
        let cache = BackgroundCacheInner::new(32);
        let obj_access = ObjectAccess {
            ts: 0,
            cache: &cache,
            dev: &dev,
            obj_table: &obj_table,
        };
        assert_eq!(obj_access.get(0), None);
        let arc_entry = Arc::new(Object::E(Entry::new(vec![1], vec![1], 1)));
        let pos = ObjectPos::default();
        let obj_ref = ObjectRef::new(&arc_entry, pos, 0);
        obj_table.add_new(1, obj_ref, ObjectTag::Entry);
        assert_eq!(obj_access.get(1).unwrap(), arc_entry);
        cache.close();
    }
}
