use crate::object::{MutObject, Object, ObjectId};
use std::collections::{HashMap,hash_map::{IterMut,Iter}} ;

pub struct MutCache {
    dirties: HashMap<ObjectId, MutObject>,
    removed_size: u64,
}

impl Default for MutCache {
    fn default() -> Self {
        MutCache {
            dirties: HashMap::default(),
            removed_size:0,
        }
    }
}

/// Readonly in cache
/// New/Del/Dirty in dirties
/// There is no intersection between the dirties and cache
impl MutCache {
    ///  Return true if oid in dirties
    pub fn contain(&mut self, oid: ObjectId) -> bool {
        self.dirties.contains_key(&oid)
    }
    pub fn remove(&mut self, oid: ObjectId) -> Option<MutObject> {
        let mut_obj =  self.dirties.remove(&oid);
        if let Some(mut_obj) = mut_obj {
            // if object is on disk, Logically remove it and add remove size 
            if mut_obj.is_readonly() {
                self.removed_size += mut_obj.get_ref().unwrap().get_pos().get_len() as u64;
            }
        }
        mut_obj
    }
    pub fn insert(&mut self, oid: ObjectId, obj_mut: MutObject) -> Option<MutObject> {
        self.dirties.insert(oid, obj_mut)
    }
    pub fn get_mut(&mut self, oid: ObjectId) -> Option<&mut Object> {
        self.dirties.get_mut(&oid)?.get_mut()
    }
    pub fn get_mut_dirty(&mut self, oid: ObjectId) -> Option<&mut Object> {
        let obj_mut = self.dirties.remove(&oid)?;
        if obj_mut.is_readonly() {
            // if object is on disk, Logically remove it and add remove size 
            self.removed_size += obj_mut.get_ref().unwrap().get_pos().get_len() as u64;
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
    pub fn drain(&mut self) -> Vec<(ObjectId, MutObject)> {
        self.removed_size = 0;
        self.dirties.drain().collect()
    }
    pub fn iter_mut(&mut self) -> IterMut<ObjectId, MutObject> {
        self.dirties.iter_mut()
    }

    pub fn iter(&mut self) -> Iter<ObjectId, MutObject> {
        self.dirties.iter()
    }

    pub fn get_removed_size(&self) -> u64 {
        self.removed_size
    }
}
