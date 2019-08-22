use super::TimeStamp;
use crate::cache::BackgroundCache;
use crate::error::TdbError;
use crate::meta::ObjectTable;
use crate::object::{Object, ObjectId, UNUSED_OID};
use crate::storage::DataLogFileReader;
use crate::tree::{Branch, Entry, Leaf};

use std::borrow::Borrow;
use std::ops::Range;
use std::sync::Arc;

pub struct ImmutContext {
    pub ts: TimeStamp,
    pub root_oid: ObjectId,
    pub obj_table: Arc<ObjectTable>,
    pub cache: BackgroundCache,
    pub data_file: DataLogFileReader,
}

pub struct Iter<'a, K: Borrow<[u8]>> {
    ctx: &'a mut ImmutContext,
    path: Vec<(ObjectId, Arc<Object>, usize)>,
    range: Range<&'a K>,
    entry_index: usize,
}

impl<'a, K: Borrow<[u8]>> Iter<'a, K> {
    pub fn next_path(&mut self) -> Result<(), TdbError> {
        loop {
            if let Some((_, _, index)) = self.path.pop() {
                if let Some((_, _obj, _)) = self.path.last() {
                    let mut parent_obj = _obj.clone();
                    if index + 1 < parent_obj.get_ref::<Branch>().children.len() {
                        let mut new_index = index + 1;
                        loop {
                            let new_oid = parent_obj.get_ref::<Branch>().children[new_index];
                            let new_obj = self.ctx.get_oid(new_oid)?.unwrap();
                            self.path.push((new_oid, new_obj.clone(), new_index));
                            if new_obj.is::<Leaf>() {
                                break;
                            } else {
                                parent_obj = new_obj;
                                new_index = 0;
                            }
                        }
                        break;
                    } else {
                        continue;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        self.entry_index = 0;
        assert!(self.path.is_empty() || self.path.last().unwrap().1.is::<Leaf>());
        Ok(())
    }
}

impl<'a, K: Borrow<[u8]>> Iterator for Iter<'a, K> {
    type Item = Result<Vec<u8>, TdbError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.path.is_empty() {
            None
        } else {
            let (_, leaf, _) = self.path.last().unwrap();
            let mut leaf_ref = leaf.get_ref::<Leaf>();
            if self.entry_index >= leaf_ref.entrys.len() {
                match self.next_path() {
                    Err(e) => {
                        self.path.clear();
                        return Some(Err(e));
                    }
                    Ok(()) => {}
                }
                if let Some((_, leaf, _)) = self.path.last() {
                    leaf_ref = leaf.get_ref::<Leaf>();
                } else {
                    return None;
                }
            }
            let (key, oid) = &leaf_ref.entrys[self.entry_index];
            if key.as_slice() < self.range.end.borrow() {
                let obj = self.ctx.get_oid(*oid);
                self.entry_index += 1;
                match obj {
                    Ok(Some(obj)) => Some(Ok(obj.get_ref::<Entry>().val.clone())),
                    Err(err) => {
                        self.path.clear();
                        Some(Err(err))
                    }
                    Ok(None) => {
                        self.path.clear();
                        Some(Err(TdbError::NotFindObject))
                    }
                }
            } else {
                None
            }
        }
    }
}


impl ImmutContext {
    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<Vec<u8>>, TdbError> {
        if self.root_oid == UNUSED_OID {
            return Ok(None);
        }
        let mut current_oid = self.root_oid;
        loop {
            if let Some(current_obj) = self.get_oid(current_oid)? {
                match &*current_obj {
                    Object::E(entry) => {
                        // Notice that , we don't cache entry
                        return Ok(Some(entry.val.clone()));
                    }
                    Object::L(leaf) => match leaf.search(key) {
                        Ok(oid) => current_oid = oid,
                        Err(_) => return Ok(None),
                    },
                    Object::B(branch) => {
                        let (oid, _) = branch.search(key);
                        current_oid = oid;
                    }
                }
            } else {
                return Err(TdbError::NotFindObject);
            }
        }
    }

    pub fn range<'a, K: Borrow<[u8]>>(
        &'a mut self,
        range: Range<&'a K>,
    ) -> Result<Option<Iter<'a, K>>, TdbError> {
        if self.root_oid == UNUSED_OID {
            return Ok(None);
        }
        let mut current_oid = self.root_oid;
        let mut index = 0;
        let mut entry_index = 0;
        let mut path = vec![];
        loop {
            if let Some(current_obj) = self.get_oid(current_oid)? {
                path.push((current_oid, current_obj.clone(), index));
                match &*current_obj {
                    Object::E(_) => unreachable!(),
                    Object::B(branch) => {
                        let (_oid, _index) = branch.search(range.start);
                        current_oid = _oid;
                        index = _index;
                    }
                    Object::L(leaf) => {
                        entry_index = leaf.search_index(range.start);
                        break;
                    }
                }
            } else {
                return Err(TdbError::NotFindObject);
            }
        }
        Ok(Some(Iter {
            ctx: self,
            path: path,
            range: range,
            entry_index: entry_index,
        }))
    }

    fn get_oid(&mut self, oid: ObjectId) -> Result<Option<Arc<Object>>, TdbError> {
        if let Some(obj) = self.cache.get(oid, self.ts) {
            Ok(Some(obj))
        } else {
            match self.obj_table.get(oid, self.ts, &mut self.data_file)? {
                Some(obj) => {
                    // only cache index node
                    if !obj.is::<Entry>() {
                        self.cache.insert(oid, self.ts, obj.clone());
                    }
                    Ok(Some(obj))
                }
                None => Ok(None),
            }
        }
    }

}


// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::cache::BackgroundCacheInner;
//     use crate::object::*;
//     use crate::storage::{Dummy, ObjectPos};
//     use crate::tree::Entry;

//     #[test]
//     fn test_object_access() {
//         let dummy = Dummy {};
//         let dev = BlockDev::new(dummy);
//         let obj_table = ObjectTable::with_capacity(1 << 16);
//         let cache = BackgroundCacheInner::new(32);
//         let obj_access = ObjectAccess {
//             ts: 0,
//             cache: &cache,
//             dev: &dev,
//             obj_table: &obj_table,
//         };
//         assert_eq!(obj_access.get(0), None);
//         let arc_entry = Arc::new(Object::E(Entry::new(vec![1], vec![1], 1)));
//         let pos = ObjectPos::default();
//         let obj_ref = ObjectRef::new(&arc_entry, pos, 0);
//         obj_table.insert(1, obj_ref, 0);
//         assert_eq!(obj_access.get(1).unwrap(), arc_entry);
//         cache.close();
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::cache::BackgroundCacheInner;
//     use crate::object::*;
//     use crate::storage::{BlockDev, Dummy, ObjectPos};
//     use crate::tree::Entry;
//     use crate::meta::ObjectTable;
//     #[test]
//     fn test_tree_reader() {
//         let dummy = Dummy {};
//         let dev = BlockDev::new(dummy);
//         let obj_table = ObjectTable::with_capacity(1 << 16);
//         let cache = BackgroundCacheInner::new(32);
//         let obj_access = ObjectAccess {
//             ts: 0,
//             cache: &cache,
//             dev: &dev,
//             obj_table: &obj_table,
//         };
//         let e1 = Arc::new(Object::E(Entry::new(vec![1], vec![1], 1)));
//         let obj1 = ObjectRef::new(&e1, ObjectPos::default(), 0);
//         obj_table.insert(1, obj1, 0);

//         let e2 = Arc::new(Object::E(Entry::new(vec![2], vec![2], 2)));
//         let obj2 = ObjectRef::new(&e2, ObjectPos::default(), 0);
//         obj_table.insert(2, obj2, 0);

//         let e3 = Arc::new(Object::E(Entry::new(vec![3], vec![3], 3)));
//         let obj3 = ObjectRef::new(&e3, ObjectPos::default(), 0);
//         obj_table.insert(3, obj3, 0);

//         let e4 = Arc::new(Object::E(Entry::new(vec![4], vec![4], 4)));
//         let obj4 = ObjectRef::new(&e4, ObjectPos::default(), 0);
//         obj_table.insert(4, obj4, 0);

//         let mut l1 = Leaf::default();
//         l1.info.oid = 5;
//         l1.insert_non_full(0, vec![1], 1);
//         l1.insert_non_full(1, vec![2], 2);
//         let l1 = Arc::new(Object::L(l1));
//         let obj5 = ObjectRef::new(&l1, ObjectPos::default(), 0);
//         obj_table.insert(5, obj5, 0);

//         let mut l2 = Leaf::default();
//         l2.info.oid = 6;
//         l2.insert_non_full(0, vec![3], 3);
//         l2.insert_non_full(1, vec![4], 4);
//         let l2 = Arc::new(Object::L(l2));
//         let obj6 = ObjectRef::new(&l2, ObjectPos::default(), 0);
//         obj_table.insert(6, obj6, 0);

//         let mut b1 = Branch::default();
//         b1.info.oid = 7;
//         b1.keys.push(vec![3]);
//         b1.children.push(5);
//         b1.children.push(6);
//         let b1 = Arc::new(Object::B(b1));
//         let obj7 = ObjectRef::new(&b1, ObjectPos::default(), 0);
//         obj_table.insert(7, obj7, 0);

//         let tree_reader = TreeReader {
//             obj_access: obj_access,
//             root_oid: 7,
//         };

//         assert_eq!(tree_reader.get(&vec![1]).unwrap(), e1);
//         assert_eq!(tree_reader.get(&vec![2]).unwrap(), e2);
//         assert_eq!(tree_reader.get(&vec![3]).unwrap(), e3);
//         assert_eq!(tree_reader.get(&vec![4]).unwrap(), e4);
//         let low = vec![1];
//         let high = vec![4];
//         let mut range = tree_reader.range(&low..&high).unwrap();

//         assert_eq!(range.next(), Some(e1));
//         assert_eq!(range.next(), Some(e2));
//         assert_eq!(range.next(), Some(e3));
//         assert_eq!(range.next(), None);
//         let low = vec![4];
//         let high = vec![5];
//         range = tree_reader.range(&low..&high).unwrap();
//         assert_eq!(range.next(), Some(e4.clone()));
//         assert_eq!(range.next(), None);
//         cache.close();
//     }
// }
