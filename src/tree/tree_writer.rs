use super::{Branch, Entry, Key, Leaf, Val, MAX_KEY_LEN};
use crate::cache::MutCache;
use crate::object::{Object, ObjectId, ObjectModify, UNUSED_OID};
use crate::storage::{ObjectPos, RawBlockDev};
use crate::transaction::TimeStamp;

use std::borrow::Borrow;

pub struct TreeWriter<'a, C: MutCache, D: RawBlockDev + Unpin> {
    obj_modify: ObjectModify<'a, C, D>,
    root_oid: ObjectId,
}

impl<'a, C: MutCache, D: RawBlockDev + Unpin> TreeWriter<'a, C, D> {
    pub fn insert<K: Into<Key>, V: Into<Val>>(&mut self, key: K, val: V) {
        let key: Key = key.into();
        assert!(key.len() <= MAX_KEY_LEN);
        let val: Val = val.into();
        if let Some(oid) = self.get_oid(&key) {
            // make oid dirty
            let obj_mut = self.obj_modify.get_mut(oid).unwrap();
            assert!(obj_mut.is::<Entry>());
            let entry_mut = obj_mut.get_mut::<Entry>();
            assert!(entry_mut.key == key);
            entry_mut.update(val);
            return;
        } else {
            // create empty leaf if tree is empty
            if self.root_oid == UNUSED_OID {
                let new_leaf = Leaf::default();
                self.root_oid = self.obj_modify.insert(Object::L(new_leaf));
            }
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
                        // split if leaf is full
                        if obj_mut.should_split() {
                            let (split_key, new_leaf) = obj_mut.split();
                            let new_leaf_oid = self.obj_modify.insert(Object::L(new_leaf));
                            // leaf is root
                            if current_oid == self.root_oid {
                                let branch = Branch::new(split_key, current_oid, new_leaf_oid);
                                self.root_oid = self.obj_modify.insert(Object::B(branch));
                            }
                            // insert parent branch
                            else {
                                let parent_branch = self
                                    .obj_modify
                                    .get_mut(parent_oid)
                                    .unwrap()
                                    .get_mut::<Branch>();
                                parent_branch.insert_non_full(
                                    current_index,
                                    split_key,
                                    new_leaf_oid,
                                );
                            }
                        }
                        return;
                    }
                    Object::B(branch) => {
                        if branch.should_split() {
                            let obj_mut = self
                                .obj_modify
                                .get_mut(current_oid)
                                .unwrap()
                                .get_mut::<Branch>();
                            let (split_key, new_branch) = obj_mut.split();
                            let new_branch_oid = self.obj_modify.insert(Object::B(new_branch));
                            let val_in_left = split_key <= key;
                            // leaf is root
                            if current_oid == self.root_oid {
                                // new  root
                                let branch = Branch::new(split_key, current_oid, new_branch_oid);
                                self.root_oid = self.obj_modify.insert(Object::B(branch));
                            }
                            // insert parent branch
                            else {
                                let parent_branch = self
                                    .obj_modify
                                    .get_mut(parent_oid)
                                    .unwrap()
                                    .get_mut::<Branch>();
                                parent_branch.insert_non_full(
                                    current_index,
                                    split_key,
                                    new_branch_oid,
                                );
                            }
                            // reset current obj
                            if val_in_left {
                                current_oid = new_branch_oid;
                                current_index += 1;
                            }
                        }
                        // find next child
                        else {
                            let (oid, index) = branch.search(&key);
                            parent_oid = current_oid;
                            current_oid = oid;
                            current_index = index;
                        }
                    }
                }
            }
        }
    }

    pub fn remove<K: Borrow<[u8]>>(&mut self, key: &K) -> Option<(Key, ObjectId)> {
        if let Some(entry_oid) = self.get_oid(key) {
            let mut current_oid = self.root_oid;
            let mut current_index = 0;
            let mut parent_oid = self.root_oid;
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
                        // remove entry
                        let result = obj_mut.remove(key);
                        assert_eq!(result.as_ref().unwrap().1, entry_oid);
                        if obj_mut.should_rebalance_merge() {
                            // leaf is root, don't merge
                            if self.root_oid == current_oid {
                                return result;
                            }
                            // leaf is not root
                            let parent_branch = self
                                .obj_modify
                                .get_ref(parent_oid)
                                .unwrap()
                                .get_ref::<Branch>();
                            // use next obj to rebalance or merge
                            if current_index + 1 < parent_branch.children.len() {
                                let next_oid = parent_branch.children[current_index + 1];
                                // hack to get two mut ref at one time
                                unsafe {
                                    let next_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(next_oid)
                                        .unwrap()
                                        .get_mut::<Leaf>()
                                        as *mut _;
                                    let current_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)
                                        .unwrap()
                                        .get_mut::<Leaf>()
                                        as *mut _;
                                    // merge is possible
                                    if Leaf::should_merge(&*current_leaf_ptr, &*next_leaf_ptr) {
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        let next_leaf_mut = &mut *next_leaf_ptr;
                                        current_leaf_mut.merge(next_leaf_mut);
                                        // remove next oid in obj table
                                        self.obj_modify.remove(next_oid);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        //  remove next oid in parent
                                        let next_obj_tup =
                                            parent_branch_mut.remove_index(current_index);
                                        assert_eq!(next_obj_tup.1, next_oid);
                                        if parent_branch_mut.keys.is_empty() {
                                            // parent mut be root , non-root branch at least has 3 child (4K page, 255 max key size)
                                            assert!(
                                                parent_branch_mut.children.len() == 1
                                                    && parent_oid == self.root_oid
                                            );
                                            self.obj_modify.remove(parent_oid);
                                            self.root_oid = current_oid;
                                        }
                                    }
                                    // rebalance is possible
                                    else {
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        let next_leaf_mut = &mut *next_leaf_ptr;
                                        let new_key = current_leaf_mut.rebalance(next_leaf_mut);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        // change key
                                        parent_branch_mut.update_key(current_index, new_key);
                                    }
                                }
                            }
                            // use prev obj to rebalance or merge
                            else {
                                let prev_oid = parent_branch.children[current_index - 1];
                                // hack to get two mut ref at one time
                                unsafe {
                                    let prev_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(prev_oid)
                                        .unwrap()
                                        .get_mut::<Leaf>()
                                        as *mut _;
                                    let current_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)
                                        .unwrap()
                                        .get_mut::<Leaf>()
                                        as *mut _;
                                    // merge is possible
                                    if Leaf::should_merge(&*prev_leaf_ptr, &*current_leaf_ptr) {
                                        let prev_leaf_mut = &mut *prev_leaf_ptr;
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        prev_leaf_mut.merge(current_leaf_mut);
                                        // remove current oid in obj table
                                        self.obj_modify.remove(current_oid);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        //  remove current oid in parent
                                        let current_obj_tup =
                                            parent_branch_mut.remove_index(current_index - 1);
                                        assert_eq!(current_obj_tup.1, current_oid);
                                        if parent_branch_mut.keys.is_empty() {
                                            // parent mut be root , non-root branch at least has 3 child (4K page, 255 max key size)
                                            assert!(
                                                parent_branch_mut.children.len() == 1
                                                    && parent_oid == self.root_oid
                                            );
                                            self.obj_modify.remove(parent_oid);
                                            self.root_oid = prev_oid;
                                        }
                                    }
                                    // rebalance is possible
                                    else {
                                        assert!(Leaf::should_rebalance(
                                            &*prev_leaf_ptr,
                                            &*current_leaf_ptr
                                        ));
                                        let prev_leaf_mut = &mut *prev_leaf_ptr;
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        let new_key = prev_leaf_mut.rebalance(current_leaf_mut);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        // change key
                                        parent_branch_mut.update_key(current_index - 1, new_key);
                                    }
                                }
                            }
                        }
                        return result;
                    }
                    Object::B(branch) => {
                        // leaf is root, don't merge
                        if branch.should_rebalance_merge() && self.root_oid != current_oid {
                            // leaf is not root
                            let parent_branch = self
                                .obj_modify
                                .get_ref(parent_oid)
                                .unwrap()
                                .get_ref::<Branch>();

                            if current_index + 1 < parent_branch.children.len() {
                                let next_oid = parent_branch.children[current_index + 1];
                                unsafe {
                                    let next_branch_ptr = self
                                        .obj_modify
                                        .get_mut(next_oid)
                                        .unwrap()
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    let current_branch_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)
                                        .unwrap()
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    // merge is possible
                                    if Branch::should_merge(&*current_branch_ptr, &*next_branch_ptr)
                                    {
                                        let current_branch_mut = &mut *current_branch_ptr;
                                        let next_branch_mut = &mut *next_branch_ptr;
                                        let next_key = self
                                            .obj_modify
                                            .get_ref(next_branch_mut.children[0])
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        current_branch_mut.merge(next_branch_mut, next_key);
                                        // remove next oid in obj table
                                        self.obj_modify.remove(next_oid);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        //  remove next oid in parent
                                        let next_obj_tup =
                                            parent_branch_mut.remove_index(current_index);
                                        assert_eq!(next_obj_tup.1, next_oid);
                                        if parent_branch_mut.keys.is_empty() {
                                            // parent mut be root , non-root branch at least has 3 child (4K page, 255 max key size)
                                            assert!(
                                                parent_branch_mut.children.len() == 1
                                                    && parent_oid == self.root_oid
                                            );
                                            self.obj_modify.remove(parent_oid);
                                            self.root_oid = current_oid;
                                            // restart from current_oid
                                            parent_oid = current_oid;
                                        }
                                    }
                                    // rebalance is possible
                                    else {
                                        let current_branch_mut = &mut *current_branch_ptr;
                                        let next_branch_mut = &mut *next_branch_ptr;
                                        let next_key = self
                                            .obj_modify
                                            .get_ref(next_branch_mut.children[0])
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        let new_key =
                                            current_branch_mut.rebalance(next_branch_mut, next_key);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        if new_key.as_slice() <= key.borrow() {
                                            current_oid = next_oid;
                                            current_index += 1;
                                        }
                                        // change key
                                        parent_branch_mut.update_key(current_index, new_key);
                                    }
                                }
                            }
                            // use prev obj to rebalance or merge
                            else {
                                let prev_oid = parent_branch.children[current_index - 1];
                                // hack to get two mut ref at one time
                                unsafe {
                                    let prev_branch_ptr = self
                                        .obj_modify
                                        .get_mut(prev_oid)
                                        .unwrap()
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    let current_branch_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)
                                        .unwrap()
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    // merge is possible
                                    if Branch::should_merge(&*prev_branch_ptr, &*current_branch_ptr)
                                    {
                                        let prev_branch_mut = &mut *prev_branch_ptr;
                                        let current_branch_mut = &mut *current_branch_ptr;
                                        let next_key = self
                                            .obj_modify
                                            .get_ref(current_branch_mut.children[0])
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        prev_branch_mut.merge(current_branch_mut, next_key);
                                        // remove cur oid in obj table
                                        self.obj_modify.remove(current_oid);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        //  remove cur oid in parent
                                        let current_obj_tup =
                                            parent_branch_mut.remove_index(current_index - 1);
                                        assert_eq!(current_obj_tup.1, current_oid);
                                        if parent_branch_mut.keys.is_empty() {
                                            // parent mut be root , non-root branch at least has 3 child (4K page, 255 max key size)
                                            assert!(
                                                parent_branch_mut.children.len() == 1
                                                    && parent_oid == self.root_oid
                                            );
                                            self.obj_modify.remove(parent_oid);
                                            self.root_oid = prev_oid;
                                            // restart from current_oid
                                            parent_oid = prev_oid;
                                        }
                                        current_oid = prev_oid;
                                        current_index -= 1;
                                    }
                                    // rebalance is possible
                                    else {
                                        let prev_branch_mut = &mut *prev_branch_ptr;
                                        let current_branch_mut = &mut *current_branch_ptr;
                                        let next_key = self
                                            .obj_modify
                                            .get_ref(current_branch_mut.children[0])
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        let new_key =
                                            prev_branch_mut.rebalance(current_branch_mut, next_key);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        if new_key.as_slice() > key.borrow() {
                                            current_oid = prev_oid;
                                            current_index -= 1;
                                        }
                                        // change key
                                        parent_branch_mut.update_key(current_index - 1, new_key)
                                    }
                                }
                            }
                        } else {
                            let (oid, index) = branch.search(key);
                            parent_oid = current_oid;
                            current_oid = oid;
                            current_index = index;
                        }
                    }
                }
            }
        } else {
            None
        }
    }

    pub fn get_oid<K: Borrow<[u8]>>(&mut self, key: &K) -> Option<ObjectId> {
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

    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Option<&Entry> {
        if let Some(oid) = self.get_oid(key) {
            self.obj_modify
                .get_ref(oid)
                .map(|obj| obj.get_ref::<Entry>())
        } else {
            None
        }
    }

    pub fn update_with_ts(&mut self, oid: ObjectId, new_pos: ObjectPos, ts: TimeStamp) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::MutObjectCache;
    use crate::object::*;
    use crate::storage::{BlockDev, Dummy};
    #[test]
    fn test_tree_writer() {
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
        let mut tree_writer = TreeWriter {
            obj_modify: obj_mod,
            root_oid: UNUSED_OID,
        };
        tree_writer.insert(vec![0; 255], vec![1]);
        assert_eq!(tree_writer.get(&vec![0; 255]).unwrap().val, vec![1]);
        assert_eq!(tree_writer.remove(&vec![0; 255]), Some((vec![0; 255], 1)));
        assert_eq!(tree_writer.get(&vec![0; 255]), None);

        for i in 0..255 {
            tree_writer.insert(vec![i; 255], vec![i]);
        }
        for i in 0..255 {
            assert_eq!(tree_writer.get(&vec![i; 255]).unwrap().val, vec![i]);
        }

        for i in 0..255 {
            tree_writer.remove(&vec![i; 255]);
        }

        for i in 0..255 {
            assert_eq!(tree_writer.get(&vec![i; 255]), None);
        }

        for i in 0..255 {
            tree_writer.insert(vec![i; 255], vec![i]);
        }

        for i in (0..255).rev() {
            tree_writer.remove(&vec![i; 255]);
        }

        for i in 0..255 {
            assert_eq!(tree_writer.get(&vec![i; 255]), None);
        }

        for i in 0..255 {
            for j in 0..255 {
                let mut key = vec![j; 255];
                key[0] = i;
                let val = vec![i, j];
                tree_writer.insert(key, val);
            }
        }

        for i in 0..255 {
            for j in 0..255 {
                let mut key = vec![j; 255];
                key[0] = i;
                tree_writer.remove(&key);
            }
        }

        for i in 0..255 {
            for j in 0..255 {
                let mut key = vec![j; 255];
                key[0] = i;
                assert_eq!(tree_writer.get(&key), None);
            }
        }

        for i in 0..255 {
            for j in 0..255 {
                let mut key = vec![j; 255];
                key[0] = i;
                let val = vec![i, j];
                tree_writer.insert(key, val);
            }
        }

        for i in 0..255 {
            for j in 0..255 {
                let mut key = vec![j; 255];
                key[0] = i;
                tree_writer.remove(&key);
            }
        }

        for i in (0..255).rev() {
            for j in (0..255).rev() {
                let mut key = vec![j; 255];
                key[0] = i;
                assert_eq!(tree_writer.get(&key), None);
            }
        }
    }
}
