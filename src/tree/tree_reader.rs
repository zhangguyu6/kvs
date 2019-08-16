use super::{Branch, Leaf};
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
    pub fn get<K: Borrow<[u8]>>(&self, key: &K) -> Option<Arc<Object>> {
        // tree is empty
        if self.root_oid == UNUSED_OID {
            return None;
        }
        let mut current_oid = self.root_oid;
        loop {
            let current_obj = self.obj_access.get(current_oid).unwrap();
            match &*current_obj {
                Object::E(_) => {
                    // Notice that , we don't cache entry
                    return Some(current_obj);
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
        let mut entry_index = 0;
        let mut path = vec![];
        loop {
            let current_obj = self.obj_access.get(current_oid).unwrap();
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
        }

        Some(Iter {
            obj_access: self.obj_access.clone(),
            path: path,
            range: range,
            entry_index: entry_index,
        })
    }
}

pub struct Iter<'a, C: IndexCache, D: RawBlockDev + Unpin, K: Borrow<[u8]>> {
    obj_access: ObjectAccess<'a, C, D>,
    path: Vec<(ObjectId, Arc<Object>, usize)>,
    range: Range<&'a K>,
    entry_index: usize,
}

impl<'a, C: IndexCache, D: RawBlockDev + Unpin, K: Borrow<[u8]>> Iter<'a, C, D, K> {
    pub fn next_path(&mut self) {
        loop {
            if let Some((_, _, index)) = self.path.pop() {
                if let Some((_, _obj, _)) = self.path.last() {
                    let mut parent_obj = _obj.clone();
                    if index + 1 < parent_obj.get_ref::<Branch>().children.len() {
                        let mut new_index = index + 1;
                        loop {
                            let new_oid = parent_obj.get_ref::<Branch>().children[new_index];
                            let new_obj = self.obj_access.get(new_oid).unwrap();
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
    }
}

impl<'a, C: IndexCache, D: RawBlockDev + Unpin, K: Borrow<[u8]>> Iterator for Iter<'a, C, D, K> {
    type Item = Arc<Object>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.path.is_empty() {
            None
        } else {
            let (_, leaf, _) = self.path.last().unwrap();
            let mut leaf_ref = leaf.get_ref::<Leaf>();
            if self.entry_index >= leaf_ref.entrys.len() {
                self.next_path();
                if let Some((_, leaf, _)) = self.path.last() {
                    leaf_ref = leaf.get_ref::<Leaf>();
                } else {
                    return None;
                }
            }
            let (key, oid) = &leaf_ref.entrys[self.entry_index];
            if key.as_slice() < self.range.end.borrow() {
                let obj = self.obj_access.get(*oid).unwrap();
                self.entry_index += 1;
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
    use crate::storage::{BlockDev, Dummy, ObjectPos};
    use crate::tree::Entry;

    #[test]
    fn test_tree_reader() {
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
        let e1 = Arc::new(Object::E(Entry::new(vec![1], vec![1], 1)));
        let obj1 = ObjectRef::new(&e1, ObjectPos::default(), 0);
        obj_table.insert(1, obj1, 0);

        let e2 = Arc::new(Object::E(Entry::new(vec![2], vec![2], 2)));
        let obj2 = ObjectRef::new(&e2, ObjectPos::default(), 0);
        obj_table.insert(2, obj2, 0);

        let e3 = Arc::new(Object::E(Entry::new(vec![3], vec![3], 3)));
        let obj3 = ObjectRef::new(&e3, ObjectPos::default(), 0);
        obj_table.insert(3, obj3, 0);

        let e4 = Arc::new(Object::E(Entry::new(vec![4], vec![4], 4)));
        let obj4 = ObjectRef::new(&e4, ObjectPos::default(), 0);
        obj_table.insert(4, obj4, 0);

        let mut l1 = Leaf::default();
        l1.info.oid = 5;
        l1.insert_non_full(0, vec![1], 1);
        l1.insert_non_full(1, vec![2], 2);
        let l1 = Arc::new(Object::L(l1));
        let obj5 = ObjectRef::new(&l1, ObjectPos::default(), 0);
        obj_table.insert(5, obj5, 0);

        let mut l2 = Leaf::default();
        l2.info.oid = 6;
        l2.insert_non_full(0, vec![3], 3);
        l2.insert_non_full(1, vec![4], 4);
        let l2 = Arc::new(Object::L(l2));
        let obj6 = ObjectRef::new(&l2, ObjectPos::default(), 0);
        obj_table.insert(6, obj6, 0);

        let mut b1 = Branch::default();
        b1.info.oid = 7;
        b1.keys.push(vec![3]);
        b1.children.push(5);
        b1.children.push(6);
        let b1 = Arc::new(Object::B(b1));
        let obj7 = ObjectRef::new(&b1, ObjectPos::default(), 0);
        obj_table.insert(7, obj7, 0);

        let tree_reader = TreeReader {
            obj_access: obj_access,
            root_oid: 7,
        };

        assert_eq!(tree_reader.get(&vec![1]).unwrap(), e1);
        assert_eq!(tree_reader.get(&vec![2]).unwrap(), e2);
        assert_eq!(tree_reader.get(&vec![3]).unwrap(), e3);
        assert_eq!(tree_reader.get(&vec![4]).unwrap(), e4);
        let low = vec![1];
        let high = vec![4];
        let mut range = tree_reader.range(&low..&high).unwrap();

        assert_eq!(range.next(), Some(e1));
        assert_eq!(range.next(), Some(e2));
        assert_eq!(range.next(), Some(e3));
        assert_eq!(range.next(), None);
        let low = vec![4];
        let high = vec![5];
        range = tree_reader.range(&low..&high).unwrap();
        assert_eq!(range.next(), Some(e4.clone()));
        assert_eq!(range.next(), None);
        cache.close();
    }
}
