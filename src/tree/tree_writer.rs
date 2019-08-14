use super::{Branch, Entry, Key, Leaf, Val};
use crate::cache::MutCache;
use crate::object::{Object, ObjectId, ObjectModify, UNUSED_OID};
use crate::storage::RawBlockDev;

use std::borrow::Borrow;
use std::ops::Range;
use std::sync::Arc;

pub struct TreeWriter<'a, C: MutCache, D: RawBlockDev + Unpin> {
    obj_modify: ObjectModify<'a, C, D>,
    root_oid: ObjectId,
}

impl<'a, C: MutCache, D: RawBlockDev + Unpin> TreeWriter<'a, C, D> {
    pub fn insert<K: Into<Key>, V: Into<Val>>(&mut self, key: K, val: V) -> Option<ObjectId> {
        let key: Key = key.into();
        let val: Val = val.into();
        if let Some(oid) = self.get(&key) {
            // make oid dirty
            let obj_mut = self.obj_modify.get_mut(oid).unwrap();
            assert!(obj_mut.is::<Entry>());
            let entry_mut = obj_mut.get_mut::<Entry>();
            assert!(entry_mut.key == key);
            entry_mut.update(val);
            return Some(oid);
        } else {
            let mut current_oid = self.root_oid;
            let mut current_index = 0;
            let mut parent_oid = self.root_oid;
            // allocate new node
            let entry_obj = Object::E(Entry::new(key.clone(), val, UNUSED_OID));
            let entry_oid = self.obj_modify.insert(entry_obj);
            loop {
                let current_obj = self.obj_modify.get_ref(current_oid).unwrap();
                match current_obj {
                    Object::E(_) => unreachable!(),
                    Object::L(_) => {
                        let obj_mut = self
                            .obj_modify
                            .get_mut(current_oid)
                            .unwrap()
                            .get_mut::<Leaf>();
                        let insert_index = obj_mut.search(&key).unwrap_err();
                        obj_mut.insert_non_full(insert_index, key, entry_oid);
                        if obj_mut.should_split() {
                            let (split_key, new_leaf) = obj_mut.split();
                            let new_leaf_oid = self.obj_modify.insert(Object::L(new_leaf));
                            // leaf is root
                            if current_oid == self.root_oid {
                                let branch = Branch::new(split_key,current_oid,new_leaf_oid); 
                                self.root_oid = self.obj_modify.insert(Object::B(branch));
                            }
                            // insert parent branch
                            else {
                                let parent_branch = self.obj_modify.get_mut(parent_oid).unwrap().get_mut::<Branch>();
                                parent_branch.insert_non_full(current_index,split_key,new_leaf_oid);
                            }
                        }
                        return Some(entry_oid);
                    }
                    _ => {
                        unimplemented!()
                    }
                }
            }
        }
        unimplemented!()
    }

    pub fn remove<K: Borrow<[u8]>>(&mut self, key: &K) -> Option<ObjectId> {
        unimplemented!()
    }

    fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Option<ObjectId> {
        // tree is empty
        if self.root_oid == UNUSED_OID {
            return None;
        }
        let mut current_oid = self.root_oid;
        loop {
            let current_obj = self.obj_modify.get_ref(current_oid).unwrap();
            match current_obj {
                Object::E(_) => unreachable!(),
                Object::L(leaf) => match leaf.search(key) {
                    Ok(oid) => return Some(oid),
                    Err(_) => return None,
                },
                Object::B(branch) => {
                    let (oid, _) = branch.search(key);
                    current_oid = oid;
                }
            }
        }
    }
}
