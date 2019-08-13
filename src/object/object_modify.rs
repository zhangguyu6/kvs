use crate::cache::MutCache;
use crate::object::{MutObject, Object, ObjectAllocater, ObjectId, ObjectTable};
use crate::storage::{BlockAllocater, BlockDev, RawBlockDev};
use crate::transaction::{Context, TimeStamp};
use std::sync::Arc;

const DEFAULT_OBJECT_EXTEND_NUM: usize = 1 << 16;

pub struct ObjectModify<'a, C: MutCache, D: RawBlockDev + Unpin> {
    ts: TimeStamp,
    dev: &'a BlockDev<D>,
    obj_table: &'a ObjectTable,
    obj_allocater: &'a mut ObjectAllocater,
    dirty_cache: &'a mut C,
}

impl<'a, C: MutCache, D: RawBlockDev + Unpin> ObjectModify<'a, C, D> {
    // Return reference of New/Insert/Ondisk object, None for del object
    // try to find object_table if not found
    pub fn get_ref(&mut self, oid: ObjectId) -> Option<&Object> {
        if !self.dirty_cache.contain(oid) {
            if let Some(arc_obj) = self.obj_table.get(oid, self.ts, self.dev) {
                self.dirty_cache.insert(oid, MutObject::Readonly(arc_obj));
            }
        }
        if let Some(mut_obj) = self.dirty_cache.get_mut(oid) {
            if let Some(obj_ref) = mut_obj.get_ref() {
                return Some(obj_ref);
            }
        }
        None
    }
    // Return mut reference of New/Insert/Ondisk object
    // Not allow to update removed object
    pub fn get_mut(&mut self, oid: ObjectId) -> Option<&mut Object> {
        if !self.dirty_cache.contain(oid) {
            if let Some(arc_obj) = self.obj_table.get(oid, self.ts, self.dev) {
                self.dirty_cache.insert(oid, MutObject::Readonly(arc_obj));
            }
        }
        if let Some(mut_obj) = self.dirty_cache.get_mut_dirty(oid) {
            if let Some(obj_mut) = mut_obj.get_mut() {
                return Some(obj_mut);
            }
        }
        None
    }
    // Insert Del tag if object is ondisk, otherwise just remove it
    pub fn remove(&mut self, oid: ObjectId) -> Option<Arc<Object>> {
        if let Some(mut_obj) = self.dirty_cache.remove(oid) {
            match mut_obj {
                // object is del, do nothing
                MutObject::Del => None,
                // object is new allcated, just remove it
                MutObject::New(obj) => Some(obj),
                // object is on disk, insert remove tag
                MutObject::Readonly(obj) | MutObject::Dirty(obj) => {
                    self.dirty_cache.insert(oid, MutObject::Del);
                    Some(obj)
                }
            }
        } else {
            // object is on disk, insert remove tag
            self.dirty_cache.insert(oid, MutObject::Del);
            None
        }
    }

    // Insert New object to dirty cache and Return allocated oid
    pub fn insert(&mut self, obj: Object) -> ObjectId {
        let oid = match self.obj_allocater.allocate() {
            Some(oid) => oid,
            None => {
                self.obj_allocater.extend(DEFAULT_OBJECT_EXTEND_NUM);
                self.obj_table.extend(DEFAULT_OBJECT_EXTEND_NUM);
                self.obj_allocater.allocate().unwrap()
            }
        };
        let result = self.dirty_cache.insert(oid, MutObject::New(Arc::new(obj)));
        assert!(result.is_none());
        oid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::MutObjectCache;
    use crate::object::*;
    use crate::storage::{Dummy, ObjectPos};
    use crate::tree::Entry;
    #[test]
    fn test_object_modify() {
        let dummy = Dummy {};
        let dev = BlockDev::new(dummy);
        let obj_table = ObjectTable::with_capacity(1 << 16);
        let mut obj_allocater = ObjectAllocater::with_capacity(1 << 16);
        let mut cache = MutObjectCache::with_capacity(512);
        let mut obj_mod = ObjectModify {
            ts: 0,
            dev: &dev,
            obj_table: &obj_table,
            obj_allocater: &mut obj_allocater,
            dirty_cache: &mut cache,
        };
        assert_eq!(obj_mod.insert(Object::E(Entry::default())), 0);
        assert!(obj_mod.get_ref(0).is_some());
        obj_mod.get_mut(0).unwrap().get_mut::<Entry>().key = vec![1];
        assert_eq!(obj_mod.get_ref(0).unwrap().get_ref::<Entry>().key, vec![1]);
        assert!(obj_mod.dirty_cache.get_mut(0).unwrap().is_new());
        assert!(obj_mod.remove(0).is_some());
        assert!(obj_mod.dirty_cache.insert(1, MutObject::Del).is_none());
        assert!(obj_mod.get_ref(0).is_none());
        assert!(obj_mod.get_ref(1).is_none());
    }
}
