use crate::cache::{ImMutCache, MutObjectCache};
use crate::error::TdbError;
use crate::meta::{ObjectAllocater, ObjectTable, PageId, OBJECT_TABLE_ENTRY_PRE_PAGE};
use crate::object::{MutObject, Object, ObjectId, ObjectRef};
use crate::storage::{DataLogFileReader, ObjectPos};
use crate::transaction::TimeStamp;
use crate::tree::Entry;
use std::collections::HashSet;
use std::sync::Arc;

pub struct ObjectAccess {
    ts: TimeStamp,
    obj_table: Arc<ObjectTable>,
    data_log_reader: DataLogFileReader,
    cache: ImMutCache,
}

impl ObjectAccess {
    pub fn new(
        ts: TimeStamp,
        obj_table: Arc<ObjectTable>,
        data_log_reader: DataLogFileReader,
        cache: ImMutCache,
    ) -> Self {
        Self {
            ts,
            obj_table,
            data_log_reader,
            cache,
        }
    }
    pub fn get_arc_obj(&mut self, oid: ObjectId) -> Result<Option<Arc<Object>>, TdbError> {
        match self
            .obj_table
            .get(oid, self.ts, &mut self.data_log_reader)?
        {
            Some((pos, obj)) => {
                // only cache index node
                if !obj.is::<Entry>() {
                    self.cache.insert(pos, obj.clone());
                }
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }
}

pub struct ObjectModify {
    dirty_cache: MutObjectCache,
    data_log_reader: DataLogFileReader,
    pub cache: ImMutCache,
    pub ts: TimeStamp,
    pub min_ts: TimeStamp,
    pub obj_table: Arc<ObjectTable>,
    pub dirty_pages: HashSet<PageId>,
    pub obj_allocater: ObjectAllocater,
    pub add_index_objs: Vec<(ObjectId, Object)>,
    pub add_entry_objs: Vec<(ObjectId, Object)>,
    pub del_objs: Vec<ObjectId>,
    pub meta_logs: Vec<(ObjectId, ObjectPos)>,
    pub current_gc_ctx: Vec<ObjectId>,
}

impl ObjectModify {
    pub fn new_empty(file: DataLogFileReader) -> Self {
        Self::new(
            file,
            ObjectTable::default(),
            ObjectAllocater::default(),
            HashSet::default(),
        )
    }

    pub fn new(
        file: DataLogFileReader,
        obj_table: ObjectTable,
        obj_allocater: ObjectAllocater,
        dirty_pages: HashSet<PageId>,
    ) -> Self {
        let cache = ImMutCache::default();
        let dirty_cache = MutObjectCache::default();
        let ts = 0;
        let min_ts = 0;
        let obj_table = Arc::new(obj_table);
        let add_index_objs = Vec::default();
        let add_entry_objs = Vec::default();
        let del_objs = Vec::default();
        let meta_logs = Vec::default();
        let current_gc_ctx = Vec::default();
        Self {
            cache,
            dirty_cache,
            data_log_reader: file,
            ts,
            min_ts,
            obj_table,
            dirty_pages,
            obj_allocater,
            add_index_objs,
            add_entry_objs,
            del_objs,
            meta_logs,
            current_gc_ctx,
        }
    }
    // Return reference of New/Insert/Ondisk object, None for del object
    // try to find object_table if not found
    pub fn get_ref(&mut self, oid: ObjectId) -> Result<Option<&Object>, TdbError> {
        if !self.dirty_cache.contain(oid) {
            if let Some((pos, arc_obj)) =
                self.obj_table
                    .get(oid, self.ts, &mut self.data_log_reader)?
            {
                self.dirty_cache
                    .insert(oid, MutObject::Readonly(arc_obj.clone()));
                self.cache.insert(pos, arc_obj);
            }
        }
        Ok(self.dirty_cache.get_ref(oid))
    }
    // Return mut reference of New/Insert/Ondisk object
    // Not allow to update removed object
    pub fn get_mut(&mut self, oid: ObjectId) -> Result<Option<&mut Object>, TdbError> {
        if !self.dirty_cache.contain(oid) {
            if let Some((pos, arc_obj)) =
                self.obj_table
                    .get(oid, self.ts, &mut self.data_log_reader)?
            {
                self.dirty_cache
                    .insert(oid, MutObject::Readonly(arc_obj.clone()));
                self.cache.insert(pos, arc_obj);
            }
        }
        Ok(self.dirty_cache.get_mut(oid))
    }
    // Insert Del tag if object is ondisk, otherwise just remove it
    pub fn remove(&mut self, oid: ObjectId) -> Option<MutObject> {
        if let Some(mut_obj) = self.dirty_cache.remove(oid) {
            match mut_obj {
                // object is del, do nothing
                MutObject::Del => {
                    self.dirty_cache.insert(oid, MutObject::Del);
                    self.dirty_cache.insert(oid, mut_obj)
                }
                // object is new allcated, just remove it and free oid
                MutObject::New(_) => {
                    // reuse oid
                    self.obj_allocater.free_oid(oid);
                    Some(mut_obj)
                }
                // object is on disk, insert remove tag and free oid
                MutObject::Readonly(_) | MutObject::Dirty(_) => {
                    self.dirty_cache.insert(oid, MutObject::Del);
                    // reuse oid
                    self.obj_allocater.free_oid(oid);
                    Some(mut_obj)
                }
            }
        } else {
            // object is on disk, insert remove tag
            self.dirty_cache.insert(oid, MutObject::Del);
            None
        }
    }

    // Insert New object to dirty cache and Return allocated oid
    pub fn insert(&mut self, mut obj: Object) -> ObjectId {
        let oid = match self.obj_allocater.allocate_oid() {
            Some(oid) => oid,
            None => {
                self.obj_allocater.extend(OBJECT_TABLE_ENTRY_PRE_PAGE);
                self.obj_table.extend(OBJECT_TABLE_ENTRY_PRE_PAGE);
                self.obj_allocater
                    .allocate_oid()
                    .expect("no enough oid for object")
            }
        };
        obj.get_object_info_mut().oid = oid;
        if let Some(mut_obj) = self.dirty_cache.remove(oid) {
            match mut_obj {
                // object is on disk
                MutObject::Del | MutObject::Dirty(_) | MutObject::Readonly(_) => {
                    self.dirty_cache.insert(oid, MutObject::Dirty(obj));
                }
                _ => {
                    self.dirty_cache.insert(oid, MutObject::New(obj));
                }
            }
        } else {
            self.dirty_cache.insert(oid, MutObject::New(obj));
        }
        oid
    }

    pub fn commit(&mut self) -> () {
        let mut changes = self.dirty_cache.drain();
        for (oid, obj) in changes.drain(..) {
            match obj {
                MutObject::Dirty(obj) | MutObject::New(obj) => {
                    if obj.is::<Entry>() {
                        self.add_entry_objs.push((oid, obj));
                    } else {
                        self.add_index_objs.push((oid, obj));
                    }
                }
                MutObject::Del => {
                    self.del_objs.push(oid);
                }
                MutObject::Readonly(_) => {}
            }
        }
        // insert branch leaf
        for (oid, obj) in self.add_index_objs.iter() {
            let obj_pos = self.obj_allocater.allocate_obj_pos(obj);
            let obj_ref = ObjectRef::on_disk(obj_pos, self.ts);
            match self.obj_table.insert(*oid, obj_ref, self.min_ts) {
                Ok(()) => {}
                Err(oid) => self.current_gc_ctx.push(oid),
            }
            self.meta_logs.push((*oid, obj_pos));
        }
        // insert entry
        for (oid, obj) in self.add_entry_objs.iter() {
            let obj_pos = self.obj_allocater.allocate_obj_pos(obj);
            let obj_ref = ObjectRef::on_disk(obj_pos, self.ts);
            match self.obj_table.insert(*oid, obj_ref, self.min_ts) {
                Ok(()) => {}
                Err(oid) => self.current_gc_ctx.push(oid),
            }
            self.meta_logs.push((*oid, obj_pos));
        }
        // del
        for oid in self.del_objs.drain(..) {
            match self.obj_table.remove(oid, self.ts, self.min_ts) {
                Ok(()) => {}
                Err(oid) => self.current_gc_ctx.push(oid),
            }
            self.meta_logs.push((oid, ObjectPos::default()));
        }
        // insert dirty meta log
        for (oid, _) in self.meta_logs.iter() {
            let pid = self.obj_table.get_page_id(*oid);
            self.dirty_pages.insert(pid);
        }
    }
}
