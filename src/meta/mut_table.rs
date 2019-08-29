use crate::cache::{ImMutCache, MutCache};
use crate::error::TdbError;
use crate::meta::{InnerTable, PageId, MAX_PAGE_NUM, OBJ_PRE_PAGE};
use crate::object::{MutObject, Object, ObjectId, ObjectRef,Entry};
use crate::storage::{DataLogFileReader, ObjectPos};
use crate::transaction::TimeStamp;
use crate::utils::BitMap;
use log::debug;
use std::collections::HashSet;
use std::sync::Arc;
use std::collections::{hash_map::IterMut} ;

pub struct MutTable {
    dirty_cache: MutCache,
    data_reader: DataLogFileReader,
    cache: ImMutCache,
    table: Arc<InnerTable>,
    dirty_pages: HashSet<PageId>,
    bitmap: BitMap,
    last_oid: ObjectId,
}

impl MutTable {
    pub fn new_empty(data_reader: DataLogFileReader) -> Self {
        Self::new(
            data_reader,
            InnerTable::default(),
            BitMap::default(),
            HashSet::default(),
        )
    }

    pub fn new(
        data_reader: DataLogFileReader,
        table: InnerTable,
        bitmap: BitMap,
        dirty_pages: HashSet<PageId>,
    ) -> Self {
        let cache = ImMutCache::default();
        let dirty_cache = MutCache::default();
        let table = Arc::new(table);
        Self {
            dirty_cache,
            data_reader,
            cache,
            table,
            dirty_pages,
            bitmap,
            last_oid:0
        }
    }
    /// Return reference of New/Insert/Ondisk object, None for del object
    /// try to find object_table if not found
    pub fn get_ref(&mut self, oid: ObjectId, ts: TimeStamp) -> Result<&Object, TdbError> {
        if !self.dirty_cache.contain(oid) {
            let (pos, obj) = self.table.get(oid, ts, &mut self.data_reader)?;

            debug!(
                "obj is {:?} offset {:?} len {:?} tag {:?}",
                obj,
                pos.get_pos(),
                pos.get_len(),
                pos.get_tag(),
            );
            self.dirty_cache
                .insert(oid, MutObject::Readonly(obj.clone()));
            if !obj.is::<Entry>() {
            self.cache.insert(pos, obj);
            }
        }
        if let Some(obj_ref) = self.dirty_cache.get_ref(oid) {
            Ok(obj_ref)
        } else {
            Err(TdbError::NotFindObject)
        }
    }
    /// Return mut reference of New/Insert/Ondisk object
    /// Not allow to update removed object
    pub fn get_mut(&mut self, oid: ObjectId, ts: TimeStamp) -> Result<&mut Object, TdbError> {
        if !self.dirty_cache.contain(oid) {
            let (pos, obj) = self.table.get(oid, ts, &mut self.data_reader)?;

            debug!(
                "obj is {:?} offset {:?} len {:?} tag {:?} ",
                obj,
                pos.get_pos(),
                pos.get_len(),
                pos.get_tag(),
            );
            self.dirty_cache
                .insert(oid, MutObject::Readonly(obj.clone()));
            if !obj.is::<Entry>() {
            self.cache.insert(pos, obj);
            }
        }
        if let Some(obj_mut) = self.dirty_cache.get_mut_dirty(oid) {
            Ok(obj_mut)
        } else {
            Err(TdbError::NotFindObject)
        }
    }
    /// Free oid
    /// # Panics
    /// Panics if oid has been released
    #[inline]
    fn free_oid(&mut self, oid: ObjectId) {
        assert_eq!(self.bitmap.get_bit(oid as usize), true);
        self.bitmap.set_bit(oid as usize, false);
    }
    /// Allocate unused oid
    /// Return None if bitmap is full
    #[inline]
    fn allocate_oid(&mut self) -> Option<ObjectId> {
        if let Some(oid) = self.bitmap.first_zero_with_hint_set(self.last_oid as usize) {
            self.last_oid = oid as ObjectId;
            Some(oid as ObjectId)
        } else {
            None
        }
    }

    /// Remove object if object in dirty_cache and insert Del tag if object is ondisk
    /// Return old object
    /// # Notes
    /// this fn just remove object in dirty cache, not remove it in table
    pub fn remove(&mut self, oid: ObjectId) -> Option<MutObject> {
        if let Some(mut_obj) = self.dirty_cache.remove(oid) {
            match mut_obj {
                // object is del, do nothing
                MutObject::Del => {
                    self.dirty_cache.insert(oid, MutObject::Del);
                }
                // object is new allcated, just remove it and free oid
                MutObject::New(_) => {
                    // reuse oid
                    self.free_oid(oid);
                }
                // object is on disk, insert remove tag and free oid
                MutObject::Readonly(_) | MutObject::Dirty(_) => {
                    self.dirty_cache.insert(oid, MutObject::Del);
                    // reuse oid
                    self.free_oid(oid);
                }
            }
            Some(mut_obj)
        } else {
            // object is on disk, insert remove tag
            self.dirty_cache.insert(oid, MutObject::Del);
            // reuse oid
            self.free_oid(oid);
            None
        }
    }

    /// Insert object to dirty cache
    /// Return allocated oid
    /// # Panics
    /// Panics if there is no unused oid
    pub fn insert(&mut self, mut obj: Object) -> ObjectId {
        let oid = match self.allocate_oid() {
            Some(oid) => oid,
            None => {
                let used_page_num = self.table.get_page_num();
                if used_page_num == MAX_PAGE_NUM {
                    panic!("allocated page overflow");
                }
                self.table.extend_to(used_page_num as PageId + 1);
                self.bitmap.extend_to((used_page_num + 1) * OBJ_PRE_PAGE);
                debug!("obj num extend to {:?}", (used_page_num + 1) * OBJ_PRE_PAGE);
                self.allocate_oid().expect("no enough oid for object")
            }
        };
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

    /// Apply object change to inner table
    /// Return (oids need to gc next time,chaneged objs)
    pub fn apply(&mut self, ts: TimeStamp, min_ts: TimeStamp) -> (Vec<ObjectId>,Vec<(ObjectId,ObjectPos)>) {
        let mut changes = self.dirty_cache.drain();
        let mut gc_ctx = vec![];
        let mut obj_changes = vec![];
        for (oid, obj) in changes {
            // insert dirty pageid by oid
            self.dirty_pages.insert(InnerTable::get_page_id(oid));
            match obj {
                MutObject::Dirty(obj) | MutObject::New(obj) => {
                    let version = ObjectRef::on_disk(obj.get_pos().clone(), ts);
                    obj_changes.push((oid,obj.get_pos().clone()));
                    match self.table.insert(oid, version, min_ts) {
                        Ok(()) => {}
                        Err(oid) => gc_ctx.push(oid),
                    };
                }
                MutObject::Del => {
                    obj_changes.push((oid,ObjectPos::default()));
                    match self.table.remove(oid, ts,min_ts) {
                        Ok(()) => {}
                        Err(oid) => gc_ctx.push(oid),
                    };
                }
                _ => {}
            }
        }
        (gc_ctx,obj_changes)
    }

    /// Free object if no immut context will see it  
    pub fn gc(&mut self,oids:Vec<ObjectId>,min_ts:TimeStamp) {
        for oid in oids.iter() {
            self.table.try_gc(*oid,min_ts);
        }
    }

    /// Return toal remove size in this write transcation
    #[inline]
    pub fn get_removed_size(&self) -> u64 {
        self.dirty_cache.get_removed_size()
    }

    /// Return all changed obj in mut iter, DataFileWriter should used this iter and change obj's pos 
    #[inline]
    pub fn obj_iter_mut(&mut self) -> IterMut<ObjectId,MutObject> {
        self.dirty_cache.iter_mut()
    }


}

