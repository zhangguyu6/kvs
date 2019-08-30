use super::TimeStamp;
use crate::cache::ImMutCache;
use crate::error::TdbError;
use crate::kv::Context;
use crate::meta::{CheckPoint, InnerTable, MutTable};
use crate::object::{
    AsObject, Branch, Entry, Key, Leaf, Object, ObjectId, Val, MAX_KEY_SIZE, MAX_OBJ_SIZE,
    UNUSED_OID,
};
use crate::storage::{DataFilwWriter, Dev, MetaFileWriter, TableFileWriter};
use log::debug;
use std::borrow::Borrow;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Weak};

pub struct MutContext {
    root_oid: ObjectId,
    ts: TimeStamp,
    table: MutTable,
    meta_writer: MetaFileWriter,
    table_writer: TableFileWriter,
    data_writer: DataFilwWriter,
    gc_ctx: VecDeque<(Weak<Context>, TimeStamp, Vec<ObjectId>)>,
    dev: Dev,
}

impl MutContext {
    // Find no cheakpoint
    pub fn new_empty(dev: Dev) -> Result<(Self, Arc<InnerTable>, ImMutCache), TdbError> {
        let data_log_reader = dev.get_data_reader()?;
        let meta_writer = dev.get_meta_writer(0)?;
        let table_writer = dev.get_table_writer(0)?;
        let data_writer = dev.get_data_writer(0, 0)?;
        let mut_ctx = Self {
            root_oid: UNUSED_OID,
            ts: 0,
            table: MutTable::new_empty(data_log_reader),
            meta_writer,
            table_writer,
            data_writer,
            gc_ctx: VecDeque::default(),
            dev,
        };
        let table = mut_ctx.table.get_inner_table();
        let cache = mut_ctx.table.get_immut_cache();
        Ok((mut_ctx, table, cache))
    }
    // Find at least one checkpoint
    pub fn new(
        dev: Dev,
        cp: CheckPoint,
    ) -> Result<(Self, Arc<InnerTable>, ImMutCache), TdbError> {
        let data_log_reader = dev.get_data_reader()?;
        let meta_writer = dev.get_meta_writer(cp.meta_size as usize)?;
        let table_writer = dev.get_table_writer(cp.tablepage_nums)?;
        let data_writer = dev.get_data_writer(cp.data_size, cp.data_removed_size)?;
        let (table,bitmap) = dev.get_table_reader()?.read_table(&cp)?;
        let dirty_pages = cp.get_dirty_pages();
        let mut_ctx = Self {
            root_oid: cp.root_oid,
            ts: 0,
            table: MutTable::new(data_log_reader, table, bitmap, dirty_pages),
            meta_writer: meta_writer,
            table_writer: table_writer,
            data_writer: data_writer,
            gc_ctx: VecDeque::default(),
            dev: dev,
        };
        let table = mut_ctx.table.get_inner_table();
        let cache = mut_ctx.table.get_immut_cache();
        Ok((mut_ctx, table, cache))
    }

    #[inline]
    pub fn increase_ts(&mut self) {
        self.ts += 1;
    }
    pub fn insert<K: Into<Key>, V: Into<Val>>(&mut self, key: K, val: V) -> Result<(), TdbError> {
        let key: Key = key.into();
        let val: Val = val.into();
        if key.len() > MAX_KEY_SIZE as usize
            || Entry::get_header_size() + key.len() + val.len() > MAX_OBJ_SIZE as usize
        {
            return Err(TdbError::ObjectTooBig);
        }
        if let Some(oid) = self.get_oid(&key)? {
            debug!("get oid {:?}", oid);
            // make oid dirty
            let obj_mut = self.table.get_mut(oid, self.ts)?;
            debug!("obj_mut is {:?}", obj_mut);
            assert!(obj_mut.is::<Entry>());
            let entry_mut = obj_mut.get_mut::<Entry>();
            assert!(entry_mut.key == key);
            entry_mut.update(val);
            return Ok(());
        } else {
            // create empty leaf if tree is empty
            if self.root_oid == UNUSED_OID {
                let new_leaf = Leaf::default();
                self.root_oid = self.table.insert(Object::L(new_leaf));
            }
            let mut current_oid = self.root_oid;
            let mut current_index = 0;
            let mut parent_oid = self.root_oid;
            // allocate new node
            let entry_obj = Object::E(Entry::new(key.clone(), val));
            assert!(entry_obj.get_pos().get_len() <= MAX_OBJ_SIZE);
            let entry_oid = self.table.insert(entry_obj);
            loop {
                let current_obj = self.table.get_ref(current_oid, self.ts)?;
                match current_obj {
                    Object::E(_) => unreachable!(),
                    Object::L(_) => {
                        let obj_mut = self.table.get_mut(current_oid, self.ts)?.get_mut::<Leaf>();
                        let insert_index = obj_mut.search(&key).unwrap_err();
                        obj_mut.insert_non_full(insert_index, key, entry_oid);
                        // split if leaf is full
                        if obj_mut.should_split() {
                            let (split_key, new_leaf) = obj_mut.split();
                            let new_leaf_oid = self.table.insert(Object::L(new_leaf));
                            // leaf is root
                            if current_oid == self.root_oid {
                                let branch = Branch::new(split_key, current_oid, new_leaf_oid);
                                self.root_oid = self.table.insert(Object::B(branch));
                            }
                            // insert parent branch
                            else {
                                let parent_branch =
                                    self.table.get_mut(parent_oid, self.ts)?.get_mut::<Branch>();
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
                                .table
                                .get_mut(current_oid, self.ts)?
                                .get_mut::<Branch>();
                            let (split_key, new_branch) = obj_mut.split();
                            let new_branch_oid = self.table.insert(Object::B(new_branch));
                            let val_in_left = split_key <= key;
                            // leaf is root
                            if current_oid == self.root_oid {
                                // new  root
                                let branch = Branch::new(split_key, current_oid, new_branch_oid);
                                self.root_oid = self.table.insert(Object::B(branch));
                            }
                            // insert parent branch
                            else {
                                let parent_branch =
                                    self.table.get_mut(parent_oid, self.ts)?.get_mut::<Branch>();
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

    pub fn remove<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<(Key, Val)>, TdbError> {
        if let Some(entry_oid) = self.get_oid(key)? {
            let mut current_oid = self.root_oid;
            let mut current_index = 0;
            let mut parent_oid = self.root_oid;
            loop {
                let current_obj = self.table.get_ref(current_oid, self.ts)?;
                match current_obj {
                    Object::E(entry) => {
                        let kv = entry.get_key_val();
                        self.table.remove(current_oid, self.ts)?;
                        return Ok(Some(kv));
                    }
                    Object::L(_) => {
                        let obj_mut = self.table.get_mut(current_oid, self.ts)?.get_mut::<Leaf>();
                        // remove entry
                        let (_key, _oid) = obj_mut.remove(key).unwrap();
                        assert_eq!(_oid, entry_oid);
                        if obj_mut.should_rebalance_merge() {
                            // leaf is root, don't merge
                            if self.root_oid == current_oid {
                                current_oid = _oid;
                                continue;
                            }
                            // leaf is not root
                            let parent_branch =
                                self.table.get_ref(parent_oid, self.ts)?.get_ref::<Branch>();
                            // use next obj to rebalance or merge
                            if current_index + 1 < parent_branch.children.len() {
                                let next_oid = parent_branch.children[current_index + 1];
                                // hack to get two mut ref at one time
                                unsafe {
                                    let next_leaf_ptr =
                                        self.table.get_mut(next_oid, self.ts)?.get_mut::<Leaf>()
                                            as *mut _;
                                    let current_leaf_ptr =
                                        self.table.get_mut(current_oid, self.ts)?.get_mut::<Leaf>()
                                            as *mut _;
                                    // merge is possible
                                    if Leaf::should_merge(&*current_leaf_ptr, &*next_leaf_ptr) {
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        let next_leaf_mut = &mut *next_leaf_ptr;
                                        current_leaf_mut.merge(next_leaf_mut);
                                        // remove next oid in obj table
                                        self.table.remove(next_oid, self.ts)?;
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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
                                            self.table.remove(parent_oid, self.ts)?;
                                            self.root_oid = current_oid;
                                        }
                                    }
                                    // rebalance is possible
                                    else {
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        let next_leaf_mut = &mut *next_leaf_ptr;
                                        let new_key = current_leaf_mut.rebalance(next_leaf_mut);
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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
                                    let prev_leaf_ptr =
                                        self.table.get_mut(prev_oid, self.ts)?.get_mut::<Leaf>()
                                            as *mut _;
                                    let current_leaf_ptr =
                                        self.table.get_mut(current_oid, self.ts)?.get_mut::<Leaf>()
                                            as *mut _;
                                    // merge is possible
                                    if Leaf::should_merge(&*prev_leaf_ptr, &*current_leaf_ptr) {
                                        let prev_leaf_mut = &mut *prev_leaf_ptr;
                                        let current_leaf_mut = &mut *current_leaf_ptr;
                                        prev_leaf_mut.merge(current_leaf_mut);
                                        // remove current oid in obj table
                                        self.table.remove(current_oid, self.ts)?;
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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
                                            self.table.remove(parent_oid, self.ts)?;
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
                                            .table
                                            .get_mut(parent_oid, self.ts)?
                                            .get_mut::<Branch>();
                                        // change key
                                        parent_branch_mut.update_key(current_index - 1, new_key);
                                    }
                                }
                            }
                        }
                        current_oid = _oid;
                    }
                    Object::B(branch) => {
                        // leaf is root, don't merge
                        if branch.should_rebalance_merge() && self.root_oid != current_oid {
                            // leaf is not root
                            let parent_branch =
                                self.table.get_ref(parent_oid, self.ts)?.get_ref::<Branch>();

                            if current_index + 1 < parent_branch.children.len() {
                                let next_oid = parent_branch.children[current_index + 1];
                                unsafe {
                                    let next_branch_ptr =
                                        self.table.get_mut(next_oid, self.ts)?.get_mut::<Branch>()
                                            as *mut _;
                                    let current_branch_ptr = self
                                        .table
                                        .get_mut(current_oid, self.ts)?
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    // merge is possible
                                    if Branch::should_merge(&*current_branch_ptr, &*next_branch_ptr)
                                    {
                                        let current_branch_mut = &mut *current_branch_ptr;
                                        let next_branch_mut = &mut *next_branch_ptr;
                                        let next_key = self
                                            .table
                                            .get_ref(next_branch_mut.children[0], self.ts)?
                                            .get_key()
                                            .to_vec();
                                        current_branch_mut.merge(next_branch_mut, next_key);
                                        // remove next oid in obj table
                                        self.table.remove(next_oid, self.ts)?;
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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
                                            self.table.remove(parent_oid, self.ts)?;
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
                                            .table
                                            .get_ref(next_branch_mut.children[0], self.ts)?
                                            .get_key()
                                            .to_vec();
                                        let new_key =
                                            current_branch_mut.rebalance(next_branch_mut, next_key);
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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
                                    let prev_branch_ptr =
                                        self.table.get_mut(prev_oid, self.ts)?.get_mut::<Branch>()
                                            as *mut _;
                                    let current_branch_ptr = self
                                        .table
                                        .get_mut(current_oid, self.ts)?
                                        .get_mut::<Branch>()
                                        as *mut _;
                                    // merge is possible
                                    if Branch::should_merge(&*prev_branch_ptr, &*current_branch_ptr)
                                    {
                                        let prev_branch_mut = &mut *prev_branch_ptr;
                                        let current_branch_mut = &mut *current_branch_ptr;
                                        let next_key = self
                                            .table
                                            .get_ref(current_branch_mut.children[0], self.ts)?
                                            .get_key()
                                            .to_vec();
                                        prev_branch_mut.merge(current_branch_mut, next_key);
                                        // remove cur oid in obj table
                                        self.table.remove(current_oid, self.ts)?;
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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
                                            self.table.remove(parent_oid, self.ts)?;
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
                                            .table
                                            .get_ref(current_branch_mut.children[0], self.ts)?
                                            .get_key()
                                            .to_vec();
                                        let new_key =
                                            prev_branch_mut.rebalance(current_branch_mut, next_key);
                                        let parent_branch_mut = self
                                            .table
                                            .get_mut(parent_oid, self.ts)?
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

    fn get_oid<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<ObjectId>, TdbError> {
        debug!("root oid is {:?}", self.root_oid);
        // tree is empty
        if self.root_oid == UNUSED_OID {
            return Ok(None);
        }
        let mut current_oid = self.root_oid;
        loop {
            let current_obj = self.table.get_ref(current_oid, self.ts)?;
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

    pub fn get_entry<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<&Entry>, TdbError> {
        if let Some(oid) = self.get_oid(key)? {
            Ok(Some(self.table.get_ref(oid, self.ts)?.get_ref::<Entry>()))
        } else {
            Ok(None)
        }
    }

    fn gc(&mut self) -> TimeStamp {
        let mut clear_oids = HashSet::new();
        let mut min_ts = 0;
        loop {
            if let Some((w_ptr, _, _)) = self.gc_ctx.front() {
                if w_ptr.strong_count() == 0 {
                    let (_, ts, oids) = self.gc_ctx.pop_front().unwrap();
                    if ts > min_ts {
                        min_ts = ts;
                    }
                    clear_oids.extend(oids);
                    continue;
                }
            }
            break;
        }
        self.table.gc(clear_oids, min_ts);
        min_ts
    }

    pub fn commit(&mut self) -> Result<Arc<Context>, TdbError> {
        debug!("mut context commit start");
        let min_ts = self.gc();
        // write objs to data file
        let (data_size, data_removed_size) =
            self.data_writer.write_objs(self.table.obj_iter_mut())?;
        // apply obj change to table
        let (cur_gc_ctx, obj_changes) = self.table.apply(self.ts, min_ts);
        debug!("make new checkpoint start");
        // make new checkpoint
        let mut cp = CheckPoint::new(
            data_removed_size,
            data_size,
            self.root_oid,
            0,
            self.table_writer.used_page_num as u32,
            obj_changes,
        );
        debug!("checkpoint at {:?}", &cp);
        // write checkpoint
        if self.meta_writer.write_cp(&mut cp)? {
            // apply checkpoint if meta file is overflow
            let dirty_pages = self.table.drain_dirty_pages();
            // write table file
            for pid in dirty_pages.iter() {
                if *pid < cp.tablepage_nums {
                    let page = self.table.get_page(*pid);
                    self.table_writer.write_page(*pid, page)?;
                }
            }
            self.table_writer.flush()?;
            cp.obj_changes.clear();
            cp.tablepage_nums = self.table_writer.used_page_num;
            // write new appiled checkpoint to temp and rename
            self.meta_writer
                .write_cp_rename(cp, &self.dev.meta_log_file_path)?;
        }
        // push current ctx to gc ctx
        let ctx = Arc::new(Context {
            ts: self.ts,
            root_oid: self.root_oid,
        });
        self.gc_ctx
            .push_back((Arc::downgrade(&ctx), ctx.ts, cur_gc_ctx));
        Ok(ctx)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::cache::ObjectStateCache;
//     use crate::object::*;
//     use crate::storage::Dummy;
//     use crate::tree::Entry;
//     #[test]
//     fn test_object_modify() {
//         let dummy = Dummy {};
//         let dev = BlockDev::new(dummy);
//         let table = ObjectTable::with_capacity(1 << 16);
//         let mut bitmap = ObjectAllocater::with_capacity(1 << 16);
//         let mut cache = ObjectStateCache::with_capacity(512);
//         let mut obj_mod = ObjectModify {
//             ts: 0,
//             dev: &dev,
//             table: &table,
//             bitmap: &mut bitmap,
//             dirty_cache: &mut cache,
//         };
//         assert_eq!(obj_mod.insert(Object::E(Entry::default())), 0);
//         assert!(obj_mod.get_ref(0).is_some());
//         obj_mod.get_mut(0).unwrap().get_mut::<Entry>().key = vec![1];
//         assert_eq!(obj_mod.get_ref(0).unwrap().get_ref::<Entry>().key, vec![1]);
//         assert!(obj_mod.dirty_cache.get_mut(0).unwrap().is_new());
//         assert!(obj_mod.remove(0).is_some());
//         assert!(obj_mod.dirty_cache.insert(1, ObjectState::Del).is_none());
//         assert!(obj_mod.get_ref(0).is_none());
//         assert!(obj_mod.get_ref(1).is_none());
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::cache::ObjectStateCache;
//     use crate::meta::ObjectTable;
//     use crate::object::*;
//     use crate::storage::{BlockDev, Dummy};
//     #[test]
//     fn test_tree_writer() {
//         let dummy = Dummy {};
//         let dev = BlockDev::new(dummy);
//         let table = ObjectTable::with_capacity(1 << 16);
//         let mut bitmap = ObjectAllocater::with_capacity(1 << 16);
//         let mut cache = ObjectStateCache::with_capacity(512);
//         let mut obj_mod = ObjectModify {
//             ts: 0,
//             dev: &dev,
//             table: &table,
//             bitmap: &mut bitmap,
//             dirty_cache: &mut cache,
//         };
//         let mut tree_writer = TreeWriter {
//             table: obj_mod,
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
