use super::TimeStamp;
use crate::cache::{BackgroundCache, MutObjectCache};
use crate::meta::{CheckPoint, ObjectAllocater, ObjectTable,OBJECT_TABLE_ENTRY_PRE_PAGE};
use crate::object::{ObjectId,Object,MutObject,UNUSED_OID};
use crate::storage::{DataLogFile, MetaLogFile, MetaTableFile};
use crate::error::TdbError;
use crate::tree::{Branch, Entry, Leaf,Key,Val,MAX_KEY_LEN};
use std::sync::Arc;
use std::borrow::Borrow;

pub struct MutContext<'a> {
    pub ts: TimeStamp,
    pub root_oid: ObjectId,
    pub obj_table: Arc<ObjectTable>,
    pub obj_allocater: &'a mut ObjectAllocater,
    pub dirty_cache: &'a mut MutObjectCache,
    pub cp: &'a mut CheckPoint,
    pub data_file: DataLogFile,
    pub meta_file: MetaLogFile,
    pub meta_table_file: MetaTableFile,
}

impl<'a> MutContext<'a> {
    // Return reference of New/Insert/Ondisk object, None for del object
    // try to find object_table if not found
    fn get_oid_ref(&mut self, oid: ObjectId) -> Result<Option<&Object>, TdbError >{
        if !self.dirty_cache.contain(oid) {
            if let Some(arc_obj) = self.obj_table.get(oid, self.ts, &mut self.data_file)? {
                self.dirty_cache.insert(oid, MutObject::Readonly(arc_obj));
            }
        }
        if let Some(mut_obj) = self.dirty_cache.get_mut(oid) {
            if let Some(obj_ref) = mut_obj.get_ref() {
                return Ok(Some(obj_ref));
            }
        }
        Ok(None)
    }
    // Return mut reference of New/Insert/Ondisk object
    // Not allow to update removed object
    fn get_oid_mut(&mut self, oid: ObjectId) ->Result<Option<&mut Object>, TdbError>{
        if !self.dirty_cache.contain(oid) {
            if let Some(arc_obj) = self.obj_table.get(oid, self.ts,  &mut self.data_file)? {
                self.dirty_cache.insert(oid, MutObject::Readonly(arc_obj));
            }
        }
        if let Some(mut_obj) = self.dirty_cache.get_mut_dirty(oid) {
            if let Some(obj_mut) = mut_obj.get_mut() {
                return Ok(Some(obj_mut));
            }
        }
        Ok(None)
    }
    // Insert Del tag if object is ondisk, otherwise just remove it
    fn remove_oid(&mut self, oid: ObjectId) -> Option<Arc<Object>> {
        if let Some(mut_obj) = self.dirty_cache.remove(oid) {
            match mut_obj {
                // object is del, do nothing
                MutObject::Del => {
                    self.dirty_cache.insert(oid, mut_obj);
                    None
                }
                // object is new allcated, just remove it and free oid
                MutObject::New(obj) => {
                    // reuse oid
                    self.obj_allocater.free(oid);
                    Some(obj)
                }
                // object is on disk, insert remove tag
                MutObject::Readonly(obj) | MutObject::Dirty(obj) => {
                    // reuse oid
                    self.dirty_cache.insert(oid, MutObject::Del);
                    self.obj_allocater.free(oid);
                    Some(obj)
                }
            }
        } else {
            // object is on disk, insert remove tag
            self.dirty_cache.insert(oid, MutObject::Del);
            None
        }
    }

    // Insert New object to dirty cache and Return allocated oid
    fn insert_oid(&mut self, mut obj: Object) -> ObjectId {
        let oid = match self.obj_allocater.allocate() {
            Some(oid) => oid,
            None => {
                self.obj_allocater.extend(OBJECT_TABLE_ENTRY_PRE_PAGE);
                self.obj_table.extend(OBJECT_TABLE_ENTRY_PRE_PAGE);
                self.obj_allocater.allocate().unwrap()
            }
        };
        obj.get_object_info_mut().oid = oid;
        if let Some(mut_obj) = self.dirty_cache.remove(oid) {
            match mut_obj {
                MutObject::Del | MutObject::Dirty(_) => {self.dirty_cache.insert(oid, MutObject::Dirty(Arc::new(obj)));},
                _ => { self.dirty_cache.insert(oid, MutObject::New(Arc::new(obj)));}
            }
        } else {
            self.dirty_cache.insert(oid, MutObject::New(Arc::new(obj)));
        }
        oid
    }

    pub fn insert<K: Into<Key>, V: Into<Val>>(&mut self, key: K, val: V) -> Result<(),TdbError> {
        let key: Key = key.into();
        assert!(key.len() <= MAX_KEY_LEN);
        let val: Val = val.into();
        if let Some(oid) = self.get_oid(&key) {
            // make oid dirty
            if let Some(obj_mut) = self.get_oid_mut(oid)? {
            assert!(obj_mut.is::<Entry>());
            let entry_mut = obj_mut.get_mut::<Entry>();
            assert!(entry_mut.key == key);
            entry_mut.update(val);
            return Ok(());
            }
            else {return Err(TdbError::NotFindObject);}
        } else {
            // create empty leaf if tree is empty
            if self.root_oid == UNUSED_OID {
                let new_leaf = Leaf::default();
                self.root_oid = self.insert_oid(Object::L(new_leaf));
            }
            let mut current_oid = self.root_oid;
            let mut current_index = 0;
            let mut parent_oid = self.root_oid;
            // allocate new node
            let entry_obj = Object::E(Entry::new(key.clone(), val, UNUSED_OID));
            let entry_oid = self.insert_oid(entry_obj);
            loop {
                let current_obj = match self.get_oid_ref(current_oid)? {
                    Some(obj) => obj,
                    None => return Err(TdbError::NotFindObject),
                };

                match current_obj {
                    Object::E(_) => unreachable!(),
                    Object::L(_) => {
                        let obj_mut = self
                            .get_oid_mut(current_oid)?
                            .unwrap()
                            .get_mut::<Leaf>();
                        let insert_index = obj_mut.search(&key).unwrap_err();
                        obj_mut.insert_non_full(insert_index, key, entry_oid);
                        // split if leaf is full
                        if obj_mut.should_split() {
                            let (split_key, new_leaf) = obj_mut.split();
                            let new_leaf_oid = self.insert_oid(Object::L(new_leaf));
                            // leaf is root
                            if current_oid == self.root_oid {
                                let branch = Branch::new(split_key, current_oid, new_leaf_oid);
                                self.root_oid = self.insert_oid(Object::B(branch));
                            }
                            // insert parent branch
                            else {
                                let parent_branch = self
                                    .get_oid_mut(parent_oid)?
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


}