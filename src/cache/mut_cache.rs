use crate::object::{Object, ObjectId, ObjectState};
use std::collections::HashMap;

/// Dirty Object cache for MutTable
pub struct MutCache {
    dirties: HashMap<ObjectId, ObjectState>,
}

impl Default for MutCache {
    fn default() -> Self {
        MutCache {
            dirties: HashMap::default(),
        }
    }
}

impl MutCache {
    ///  Return true if oid in dirties
    pub fn contain(&mut self, oid: ObjectId) -> bool {
        self.dirties.contains_key(&oid)
    }
    pub fn remove(&mut self, oid: ObjectId) -> Option<ObjectState> {
        self.dirties.remove(&oid)
    }
    pub fn insert(&mut self, oid: ObjectId, obj_mut: ObjectState) -> Option<ObjectState> {
        self.dirties.insert(oid, obj_mut)
    }
    pub fn get_mut(&mut self, oid: ObjectId) -> Option<&mut Object> {
        let obj_mut = self.dirties.remove(&oid)?;
        if obj_mut.is_readonly() {
            // if object is on disk, Logically remove it
            let obj_dirty = obj_mut.to_dirty();
            self.dirties.insert(oid, obj_dirty);
        } else {
            self.dirties.insert(oid, obj_mut);
        }
        self.dirties.get_mut(&oid)?.get_mut()
    }
    pub fn get_ref(&self, oid: ObjectId) -> Option<&Object> {
        self.dirties.get(&oid)?.get_ref()
    }
    /// Clear cache and return objects
    pub fn drain(&mut self) -> Vec<(ObjectId, ObjectState)> {
        self.dirties.drain().collect()
    }

    pub fn get_inner_mut(&mut self) -> &mut HashMap<ObjectId, ObjectState> {
        &mut self.dirties
    }
}
