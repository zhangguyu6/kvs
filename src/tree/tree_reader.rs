use super::{Branch, Entry, Leaf};
use crate::cache::IndexCache;
use crate::object::{Object, ObjectAccess, ObjectId, UNUSED_OID};
use crate::storage::RawBlockDev;

use std::borrow::Borrow;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone)]
pub struct TreeReader<'a, C: IndexCache, D: RawBlockDev + Unpin> {
    obj_access: ObjectAccess<'a, C, D>,
    root_oid: ObjectId,
}

impl<'a, C: IndexCache, D: RawBlockDev + Unpin> TreeReader<'a, C, D> {
    pub fn get<K: Borrow<[u8]>>(&self, key: &K) -> Option<Entry> {
        // tree is empty
        if self.root_oid == UNUSED_OID {
            return None;
        }
        let mut current_oid = self.root_oid;
        loop {
            let current_obj = self.obj_access.get(current_oid).unwrap();
            match &*current_obj {
                Object::E(_) => {
                    // beacuse we don't cache
                    return Some(Arc::try_unwrap(current_obj).unwrap().unwrap::<Entry>());
                }
                Object::L(leaf) => match leaf.search(key) {
                    Ok(oid) => current_oid = oid,
                    Err(_) => return None,
                },
                Object::B(branch) => {
                    let (oid, _) = branch.search(key);
                    current_oid = oid;
                }
            }
        }
    }
    pub fn contains_key<K: Borrow<[u8]>>(&self, key: &K) -> bool {
        // tree is empty
        if self.root_oid == UNUSED_OID {
            return false;
        }
        let mut current_oid = self.root_oid;
        loop {
            let current_obj = self.obj_access.get(current_oid).unwrap();
            match &*current_obj {
                Object::E(_) => {
                    return true;
                }
                Object::L(leaf) => match leaf.search(key) {
                    Ok(_) => return true,
                    Err(_) => return false,
                },
                Object::B(branch) => {
                    let (oid, _) = branch.search(key);
                    current_oid = oid;
                }
            }
        }
    }
    pub fn range<K: Borrow<[u8]>>(&'a self, range: Range<&'a K>) -> Option<Iter<C, D, K>> {
        if self.root_oid == UNUSED_OID {
            return None;
        }
        let mut current_oid = self.root_oid;
        let mut index = 0;
        let mut path = vec![];
        loop {
            let current_obj = self.obj_access.get(current_oid).unwrap();
            path.push((current_oid, current_obj.clone(), index));
            match &*current_obj {
                Object::E(_) => break,
                Object::B(branch) => {
                    let (_oid, _index) = branch.search(range.start);
                    current_oid = _oid;
                    index = _index;
                }
                Object::L(leaf) => {
                    if let Some((_oid, _index)) = leaf.search_index(range.start) {
                        current_oid = _oid;
                        index = _index;
                    } else {
                        return None;
                    }
                }
            }
        }
        Some(Iter {
            obj_access: self.obj_access.clone(),
            path: path,
            range: range,
        })
    }
}

pub struct Iter<'a, C: IndexCache, D: RawBlockDev + Unpin, K: Borrow<[u8]>> {
    obj_access: ObjectAccess<'a, C, D>,
    path: Vec<(ObjectId, Arc<Object>, usize)>,
    range: Range<&'a K>,
}
