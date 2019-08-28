use super::TimeStamp;
use crate::cache::ImMutCache;
use crate::database::Context;
use crate::error::TdbError;
use crate::meta::{CheckPoint, ObjectAllocater, ObjectModify, ObjectTable, PageId};
use crate::object::{Object, ObjectId, UNUSED_OID};
use crate::storage::{DataLogFilwWriter, Dev, MetaLogFileWriter, MetaTableFileWriter};
use crate::tree::{Branch, Entry, Key, Leaf, Val, MAX_KEY_LEN};
use log::debug;
use std::borrow::Borrow;
use std::collections::{HashSet, VecDeque};
use std::mem;
use std::sync::{Arc, Weak};

pub struct MutContext {
    root_oid: ObjectId,
    obj_modify: ObjectModify,
    meta_log_writer: MetaLogFileWriter,
    meta_table_writer: MetaTableFileWriter,
    data_log_writer: DataLogFilwWriter,
    gc_ctx: VecDeque<(Weak<Context>, TimeStamp, Vec<ObjectId>)>,
    dev: Dev,
}

impl MutContext {
    pub fn new_empty(dev: Dev) -> Result<(Self, Arc<ObjectTable>, ImMutCache), TdbError> {
        let data_log_reader = dev.get_data_log_reader()?;
        let meta_log_writer = dev.get_meta_log_writer(0)?;
        let meta_table_writer = dev.get_meta_table_writer(0)?;
        let data_log_writer = dev.get_data_log_writer(0)?;
        let mut_ctx = Self {
            root_oid: UNUSED_OID,
            obj_modify: ObjectModify::new_empty(data_log_reader),
            meta_log_writer: meta_log_writer,
            meta_table_writer: meta_table_writer,
            data_log_writer: data_log_writer,
            gc_ctx: VecDeque::default(),
            dev: dev,
        };
        let obj_table = mut_ctx.obj_modify.obj_table.clone();
        let cache = mut_ctx.obj_modify.cache.clone();
        Ok((mut_ctx, obj_table, cache))
    }
    pub fn new(
        dev: Dev,
        cp: &CheckPoint,
        obj_table: ObjectTable,
        obj_allocater: ObjectAllocater,
        dirty_pages: HashSet<PageId>,
    ) -> Result<(Self, Arc<ObjectTable>, ImMutCache), TdbError> {
        let data_log_reader = dev.get_data_log_reader()?;
        let meta_log_writer = dev.get_meta_log_writer(cp.meta_log_total_len as usize)?;
        let meta_table_writer = dev.get_meta_table_writer(cp.obj_tablepage_nums)?;
        let data_log_writer = dev.get_data_log_writer(cp.data_log_len as usize)?;
        let mut_ctx = Self {
            root_oid: cp.root_oid,
            obj_modify: ObjectModify::new(data_log_reader, obj_table, obj_allocater, dirty_pages),
            meta_log_writer: meta_log_writer,
            meta_table_writer: meta_table_writer,
            data_log_writer: data_log_writer,
            gc_ctx: VecDeque::default(),
            dev: dev,
        };
        let obj_table = mut_ctx.obj_modify.obj_table.clone();
        let cache = mut_ctx.obj_modify.cache.clone();
        Ok((mut_ctx, obj_table, cache))
    }

    #[inline]
    pub fn increase_ts(&mut self) {
        self.obj_modify.ts += 1;
    }
    pub fn insert<K: Into<Key>, V: Into<Val>>(&mut self, key: K, val: V) -> Result<(), TdbError> {
        let key: Key = key.into();
        // assert!(key.len() <= MAX_KEY_LEN);
        let val: Val = val.into();
        if let Some(oid) = self.get_obj(&key)? {
            debug!("get oid {:?}", oid);
            // make oid dirty
            if let Some(obj_mut) = self.obj_modify.get_mut(oid)? {
                debug!("obj_mut is {:?}", obj_mut);
                assert!(obj_mut.is::<Entry>());
                let entry_mut = obj_mut.get_mut::<Entry>();
                assert!(entry_mut.key == key);
                entry_mut.update(val);
                return Ok(());
            } else {
                return Err(TdbError::NotFindObject);
            }
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
                let current_obj = match self.obj_modify.get_ref(current_oid)? {
                    Some(obj) => obj,
                    None => return Err(TdbError::NotFindObject),
                };

                match current_obj {
                    Object::E(_) => unreachable!(),
                    Object::L(_) => {
                        let obj_mut = self
                            .obj_modify
                            .get_mut(current_oid)?
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
                                    .get_mut(parent_oid)?
                                    .unwrap()
                                    .get_mut::<Branch>();
                                parent_branch.insert_non_full(
                                    current_index,
                                    split_key,
                                    new_leaf_oid,
                                );
                            }
                        }
                        return Ok(());
                    }
                    Object::B(branch) => {
                        if branch.should_split() {
                            let obj_mut = self
                                .obj_modify
                                .get_mut(current_oid)?
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
                                    .get_mut(parent_oid)?
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

    pub fn remove<K: Borrow<[u8]>>(
        &mut self,
        key: &K,
    ) -> Result<Option<(Key, ObjectId)>, TdbError> {
        if let Some(entry_oid) = self.get_obj(key)? {
            let mut current_oid = self.root_oid;
            let mut current_index = 0;
            let mut parent_oid = self.root_oid;
            loop {
                let current_obj = self.obj_modify.get_ref(current_oid)?.unwrap();
                match current_obj {
                    Object::E(_) => unreachable!(),
                    Object::L(_) => {
                        let obj_mut = self
                            .obj_modify
                            .get_mut(current_oid)?
                            .unwrap()
                            .get_mut::<Leaf>();
                        // remove entry
                        let result = obj_mut.remove(key);
                        assert_eq!(result.as_ref().unwrap().1, entry_oid);
                        if obj_mut.should_rebalance_merge() {
                            // leaf is root, don't merge
                            if self.root_oid == current_oid {
                                return Ok(result);
                            }
                            // leaf is not root
                            let parent_branch = self
                                .obj_modify
                                .get_ref(parent_oid)?
                                .unwrap()
                                .get_ref::<Branch>();
                            // use next obj to rebalance or merge
                            if current_index + 1 < parent_branch.children.len() {
                                let next_oid = parent_branch.children[current_index + 1];
                                // hack to get two mut ref at one time
                                unsafe {
                                    let next_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(next_oid)?
                                        .unwrap()
                                        .get_mut::<Leaf>()
                                        as *mut _;
                                    let current_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)?
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
                                            .get_mut(parent_oid)?
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
                                            .get_mut(parent_oid)?
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
                                        .get_mut(prev_oid)?
                                        .unwrap()
                                        .get_mut::<Leaf>()
                                        as *mut _;
                                    let current_leaf_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)?
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
                                            .get_mut(parent_oid)?
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
                                            .get_mut(parent_oid)?
                                            .unwrap()
                                            .get_mut::<Branch>();
                                        // change key
                                        parent_branch_mut.update_key(current_index - 1, new_key);
                                    }
                                }
                            }
                        }
                        return Ok(result);
                    }
                    Object::B(branch) => {
                        // leaf is root, don't merge
                        if branch.should_rebalance_merge() && self.root_oid != current_oid {
                            // leaf is not root
                            let parent_branch = self
                                .obj_modify
                                .get_ref(parent_oid)?
                                .unwrap()
                                .get_ref::<Branch>();

                            if current_index + 1 < parent_branch.children.len() {
                                let next_oid = parent_branch.children[current_index + 1];
                                unsafe {
                                    let next_branch_ptr = self
                                        .obj_modify
                                        .get_mut(next_oid)?
                                        .unwrap()
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    let current_branch_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)?
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
                                            .get_ref(next_branch_mut.children[0])?
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        current_branch_mut.merge(next_branch_mut, next_key);
                                        // remove next oid in obj table
                                        self.obj_modify.remove(next_oid);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)?
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
                                            .get_ref(next_branch_mut.children[0])?
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        let new_key =
                                            current_branch_mut.rebalance(next_branch_mut, next_key);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)?
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
                                        .get_mut(prev_oid)?
                                        .unwrap()
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    let current_branch_ptr = self
                                        .obj_modify
                                        .get_mut(current_oid)?
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
                                            .get_ref(current_branch_mut.children[0])?
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        prev_branch_mut.merge(current_branch_mut, next_key);
                                        // remove cur oid in obj table
                                        self.obj_modify.remove(current_oid);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)?
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
                                            .get_ref(current_branch_mut.children[0])?
                                            .unwrap()
                                            .get_key()
                                            .to_vec();
                                        let new_key =
                                            prev_branch_mut.rebalance(current_branch_mut, next_key);
                                        let parent_branch_mut = self
                                            .obj_modify
                                            .get_mut(parent_oid)?
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
            Ok(None)
        }
    }

    fn get_obj<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<ObjectId>, TdbError> {
        debug!("root oid is {:?}", self.root_oid);
        // tree is empty
        if self.root_oid == UNUSED_OID {
            return Ok(None);
        }
        let mut current_oid = self.root_oid;
        loop {
            let current_obj = self
                .obj_modify
                .get_ref(current_oid)?
                .ok_or(TdbError::NotFindObject)?;
            debug!("obj is {:?}", current_obj);
            match current_obj {
                Object::E(_) => unreachable!(),
                Object::L(leaf) => match leaf.search(key) {
                    Ok(oid) => return Ok(Some(oid)),
                    Err(_) => return Ok(None),
                },
                Object::B(branch) => {
                    let (oid, _) = branch.search(key);
                    current_oid = oid;
                }
            }
        }
    }

    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<&Entry>, TdbError> {
        if let Some(oid) = self.get_obj(key)? {
            Ok(self
                .obj_modify
                .get_ref(oid)?
                .map(|obj| obj.get_ref::<Entry>()))
        } else {
            Ok(None)
        }
    }

    pub fn commit(&mut self) -> Result<Arc<Context>, TdbError> {
        debug!("mut context commit start");
        if !self.obj_modify.commit() {
            let ctx = Arc::new(Context {
                ts: self.obj_modify.ts,
                root_oid: self.root_oid,
            });
            self.gc_ctx.push_back((
                Arc::downgrade(&ctx),
                ctx.ts,
                mem::replace(&mut self.obj_modify.current_gc_ctx, Vec::default()),
            ));
            return Ok(ctx);
        }
        // write data log
        debug!("data log write start");
        self.data_log_writer.write_obj_log(
            &self.obj_modify.add_index_objs,
            &self.obj_modify.add_entry_objs,
        )?;
        debug!("make new checkpoint start");
        // make new checkpoint
        let cp = CheckPoint::new(
            self.obj_modify.obj_allocater.data_log_remove_len,
            self.obj_modify.obj_allocater.data_log_len,
            self.root_oid,
            self.meta_log_writer.size as u32,
            self.meta_table_writer.obj_tablepage_nums,
            mem::replace(&mut self.obj_modify.meta_logs, Vec::with_capacity(0)),
        );
        debug!("checkpoint at {:?}", &cp);
        // write checkpoint
        match self.meta_log_writer.write_cp(&cp) {
            Ok(()) => {}
            Err(TdbError::NoSpace) => {
                // apply meta log
                let mut dirty_cache: Vec<ObjectId> = self.obj_modify.dirty_pages.drain().collect();
                dirty_cache.sort_unstable();
                for pid in dirty_cache {
                    let page = self.obj_modify.obj_table.get_page(pid);
                    self.meta_table_writer.write_page(pid, page)?;
                }
                self.meta_table_writer.flush()?;
                self.meta_log_writer
                    .write_cp_rename(&cp, &self.dev.meta_log_file_path)?;
            }
            Err(e) => return Err(e),
        }
        // gc
        loop {
            if let Some((ctx, _, _)) = self.gc_ctx.front() {
                if ctx.strong_count() == 0 {
                    // clear drop read ctx
                    let (_, ts, oids) = self.gc_ctx.pop_front().unwrap();
                    for oid in oids.iter() {
                        self.obj_modify.obj_table.try_gc(*oid, ts);
                    }
                    self.obj_modify.min_ts = ts;
                }
            }
            break;
        }
        // push current ctx to gc ctx
        let ctx = Arc::new(Context {
            ts: self.obj_modify.ts,
            root_oid: self.root_oid,
        });
        self.gc_ctx.push_back((
            Arc::downgrade(&ctx),
            ctx.ts,
            mem::replace(&mut self.obj_modify.current_gc_ctx, Vec::default()),
        ));
        Ok(ctx)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::cache::MutObjectCache;
//     use crate::object::*;
//     use crate::storage::Dummy;
//     use crate::tree::Entry;
//     #[test]
//     fn test_object_modify() {
//         let dummy = Dummy {};
//         let dev = BlockDev::new(dummy);
//         let obj_table = ObjectTable::with_capacity(1 << 16);
//         let mut obj_allocater = ObjectAllocater::with_capacity(1 << 16);
//         let mut cache = MutObjectCache::with_capacity(512);
//         let mut obj_mod = ObjectModify {
//             ts: 0,
//             dev: &dev,
//             obj_table: &obj_table,
//             obj_allocater: &mut obj_allocater,
//             dirty_cache: &mut cache,
//         };
//         assert_eq!(obj_mod.insert(Object::E(Entry::default())), 0);
//         assert!(obj_mod.get_ref(0).is_some());
//         obj_mod.get_mut(0).unwrap().get_mut::<Entry>().key = vec![1];
//         assert_eq!(obj_mod.get_ref(0).unwrap().get_ref::<Entry>().key, vec![1]);
//         assert!(obj_mod.dirty_cache.get_mut(0).unwrap().is_new());
//         assert!(obj_mod.remove(0).is_some());
//         assert!(obj_mod.dirty_cache.insert(1, MutObject::Del).is_none());
//         assert!(obj_mod.get_ref(0).is_none());
//         assert!(obj_mod.get_ref(1).is_none());
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::cache::MutObjectCache;
//     use crate::meta::ObjectTable;
//     use crate::object::*;
//     use crate::storage::{BlockDev, Dummy};
//     #[test]
//     fn test_tree_writer() {
//         let dummy = Dummy {};
//         let dev = BlockDev::new(dummy);
//         let obj_table = ObjectTable::with_capacity(1 << 16);
//         let mut obj_allocater = ObjectAllocater::with_capacity(1 << 16);
//         let mut cache = MutObjectCache::with_capacity(512);
//         let mut obj_mod = ObjectModify {
//             ts: 0,
//             dev: &dev,
//             obj_table: &obj_table,
//             obj_allocater: &mut obj_allocater,
//             dirty_cache: &mut cache,
//         };
//         let mut tree_writer = TreeWriter {
//             obj_modify: obj_mod,
//             root_oid: UNUSED_OID,
//         };
//         tree_writer.insert(vec![0; 255], vec![1]);
//         assert_eq!(tree_writer.get(&vec![0; 255]).unwrap().val, vec![1]);
//         assert_eq!(tree_writer.remove(&vec![0; 255]), Some((vec![0; 255], 1)));
//         assert_eq!(tree_writer.get(&vec![0; 255]), None);

//         for i in 0..255 {
//             tree_writer.insert(vec![i; 255], vec![i]);
//         }
//         for i in 0..255 {
//             assert_eq!(tree_writer.get(&vec![i; 255]).unwrap().val, vec![i]);
//         }

//         for i in 0..255 {
//             tree_writer.remove(&vec![i; 255]);
//         }

//         for i in 0..255 {
//             assert_eq!(tree_writer.get(&vec![i; 255]), None);
//         }

//         for i in 0..255 {
//             tree_writer.insert(vec![i; 255], vec![i]);
//         }

//         for i in (0..255).rev() {
//             tree_writer.remove(&vec![i; 255]);
//         }

//         for i in 0..255 {
//             assert_eq!(tree_writer.get(&vec![i; 255]), None);
//         }

//         for i in 0..255 {
//             for j in 0..255 {
//                 let mut key = vec![j; 255];
//                 key[0] = i;
//                 let val = vec![i, j];
//                 tree_writer.insert(key, val);
//             }
//         }

//         for i in 0..255 {
//             for j in 0..255 {
//                 let mut key = vec![j; 255];
//                 key[0] = i;
//                 tree_writer.remove(&key);
//             }
//         }

//         for i in 0..255 {
//             for j in 0..255 {
//                 let mut key = vec![j; 255];
//                 key[0] = i;
//                 assert_eq!(tree_writer.get(&key), None);
//             }
//         }

//         for i in 0..255 {
//             for j in 0..255 {
//                 let mut key = vec![j; 255];
//                 key[0] = i;
//                 let val = vec![i, j];
//                 tree_writer.insert(key, val);
//             }
//         }

//         for i in 0..255 {
//             for j in 0..255 {
//                 let mut key = vec![j; 255];
//                 key[0] = i;
//                 tree_writer.remove(&key);
//             }
//         }

//         for i in (0..255).rev() {
//             for j in (0..255).rev() {
//                 let mut key = vec![j; 255];
//                 key[0] = i;
//                 assert_eq!(tree_writer.get(&key), None);
//             }
//         }
//         println!("{:?}", tree_writer.root_oid);
//     }
// }
