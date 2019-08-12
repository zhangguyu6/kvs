use super::MutCache;
use crate::object::{MutObject, ObjectId};
use lru_cache::LruCache;
use std::collections::HashMap;
use std::mem;
pub struct MutObjectCache {
    dirties: HashMap<ObjectId, MutObject>,
    cache: LruCache<ObjectId, MutObject>,
}

// Readonly in cache
// New/Del/Dirty in dirties
// There is no intersection between the dirties and cache
impl MutCache for MutObjectCache {
    //  Return true if oid in cache or in dirties
    fn contain(&mut self, oid: ObjectId) -> bool {
        self.dirties.contains_key(&oid) || self.cache.contains_key(&oid)
    }
    fn remove(&mut self, oid: ObjectId) -> Option<MutObject> {
        let old_obj = self.dirties.remove(&oid);
        if old_obj.is_some() {
            return old_obj;
        } else {
            self.cache.remove(&oid)
        }
    }
    fn insert(&mut self, oid: ObjectId, obj_mut: MutObject) -> Option<MutObject> {
        if obj_mut.is_readonly() {
            assert!(!self.dirties.contains_key(&oid));
            self.cache.insert(oid, obj_mut)
        } else {
            let old_obj = self.remove(oid);
            self.dirties.insert(oid, obj_mut);
            old_obj
        }
    }
    fn get_mut(&mut self, oid: ObjectId) -> Option<&mut MutObject> {
        let obj_mut = self.dirties.get_mut(&oid);
        if obj_mut.is_some() {
            return obj_mut;
        }
        self.cache.get_mut(&oid)
    }
    fn get_mut_dirty(&mut self, oid: ObjectId) -> Option<&mut MutObject> {
        if let Some(obj_mut) = self.cache.remove(&oid) {
            assert!(obj_mut.is_readonly());
            let new_obj = obj_mut.to_dirty(); 
            self.dirties.insert(oid, new_obj);
        }
        self.dirties.get_mut(&oid)
    }
    fn drain(&mut self) -> Box<dyn Iterator<Item = (ObjectId, MutObject)>> {
        self.cache.clear();
        Box::new(mem::replace(&mut self.dirties, HashMap::with_capacity(0)).into_iter())
    }
}
 