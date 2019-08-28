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

pub struct MutTable {
    dirty_cache: MutCache,
    data_reader: DataLogFileReader,
    cache: ImMutCache,
    table: Arc<InnerTable>,
    dirty_pages: HashSet<PageId>,
    bitmap: BitMap,
    last_oid: ObjectId,
    // add_index_objs: Vec<(ObjectId, Object)>,
    // add_entry_objs: Vec<(ObjectId, Object)>,
    // del_objs: Vec<ObjectId>,
    // meta_logs: Vec<(ObjectId, ObjectPos)>,
    // current_gc_ctx: Vec<ObjectId>
}

impl MutTable {
    // pub fn new_empty(file: DataLogFileReader) -> Self {
    //     Self::new(
    //         file,
    //         ObjectTable::default(),
    //         ObjectAllocater::default(),
    //         HashSet::default(),
    //     )
    // }

    // pub fn new(
    //     file: DataLogFileReader,
    //     obj_table: ObjectTable,
    //     obj_allocater: ObjectAllocater,
    //     dirty_pages: HashSet<PageId>,
    // ) -> Self {
    //     let cache = ImMutCache::default();
    //     let dirty_cache = MutObjectCache::default();
    //     let ts = 0;
    //     let min_ts = 0;
    //     let obj_table = Arc::new(obj_table);
    //     let add_index_objs = Vec::default();
    //     let add_entry_objs = Vec::default();
    //     let del_objs = Vec::default();
    //     let meta_logs = Vec::default();
    //     let current_gc_ctx = Vec::default();
    //     Self {
    //         cache,
    //         dirty_cache,
    //         data_log_reader: file,
    //         obj_table,
    //         dirty_pages,
    //         obj_allocater,
    //         add_index_objs,
    //         add_entry_objs,
    //         del_objs,
    //         meta_logs,
    //         current_gc_ctx,
    //     }
    // }
    // Return reference of New/Insert/Ondisk object, None for del object
    // try to find object_table if not found
    pub fn get_ref(&mut self, oid: ObjectId, ts: TimeStamp) -> Result<&Object, TdbError> {
        if !self.dirty_cache.contain(oid) {
            let (pos, arc_obj) = self.table.get(oid, ts, &mut self.data_reader)?;

            debug!(
                "obj is {:?} offset {:?} len {:?} tag {:?}",
                arc_obj,
                pos.get_pos(),
                pos.get_len(),
                pos.get_tag(),
            );
            self.dirty_cache
                .insert(oid, MutObject::Readonly(arc_obj.clone()));
            self.cache.insert(pos, arc_obj);
        }
        if let Some(obj_ref) = self.dirty_cache.get_ref(oid) {
            Ok(obj_ref)
        } else {
            Err(TdbError::NotFindObject)
        }
    }
    // Return mut reference of New/Insert/Ondisk object
    // Not allow to update removed object
    pub fn get_mut(&mut self, oid: ObjectId, ts: TimeStamp) -> Result<&mut Object, TdbError> {
        if !self.dirty_cache.contain(oid) {
            let (pos, arc_obj) = self.table.get(oid, ts, &mut self.data_reader)?;

            debug!(
                "obj is {:?} offset {:?} len {:?} tag {:?} ",
                arc_obj,
                pos.get_pos(),
                pos.get_len(),
                pos.get_tag(),
            );
            self.dirty_cache
                .insert(oid, MutObject::Readonly(arc_obj.clone()));
            self.cache.insert(pos, arc_obj);
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
    ///  
    pub fn apply(&mut self, ts: TimeStamp, min_ts: TimeStamp) -> Vec<ObjectId> {
        let mut changes = self.dirty_cache.drain();
        let mut gc_ctx = vec![];
        for (oid, obj) in changes {
            match obj {
                MutObject::Dirty(obj) | MutObject::New(obj) => {
                    let version = ObjectRef::on_disk(obj.get_pos().clone(), ts);
                    match self.table.insert(oid, version, min_ts) {
                        Ok(()) => {}
                        Err(oid) => gc_ctx.push(oid),
                    };
                }
                MutObject::Del => {
                    match self.table.remove(oid, ts,min_ts) {
                        Ok(()) => {}
                        Err(oid) => gc_ctx.push(oid),
                    };
                }
                _ => {}
            }
        }
        gc_ctx
    }

    pub fn gc(&mut self,oids:Vec<ObjectId>,min_ts:TimeStamp) {
        for oid in oids.iter() {
            self.table.try_gc(*oid,min_ts);
        }
    }

    // pub fn commit(&mut self) -> bool {
    //     let mut changes = self.dirty_cache.drain();
    //     if changes.is_empty() {
    //         return false;
    //     }
    //     for (oid, obj) in changes.drain(..) {
    //         match obj {
    //             MutObject::Dirty(obj) | MutObject::New(obj) => {
    //                 if obj.is::<Entry>() {
    //                     self.add_entry_objs.push((oid, obj));
    //                 } else {
    //                     self.add_index_objs.push((oid, obj));
    //                 }
    //             }
    //             MutObject::Del => {
    //                 self.del_objs.push(oid);
    //             }
    //             MutObject::Readonly(_) => {}
    //         }
    //     }
    //     // insert branch leaf
    //     for (oid, obj) in self.add_index_objs.iter() {
    //         let obj_pos = self.obj_allocater.allocate_obj_pos(obj);
    //         debug!("pos {:?}", obj_pos.get_pos());
    //         let obj_ref = ObjectRef::on_disk(obj_pos, self.ts);
    //         match self.obj_table.insert(*oid, obj_ref, self.min_ts) {
    //             Ok(()) => {}
    //             Err(oid) => self.current_gc_ctx.push(oid),
    //         }
    //         self.meta_logs.push((*oid, obj_pos));
    //     }
    //     // insert entry
    //     for (oid, obj) in self.add_entry_objs.iter() {
    //         debug!("entry is {:?}", obj);
    //         let obj_pos = self.obj_allocater.allocate_obj_pos(obj);
    //         debug!("pos {:?} tag {:?}", obj_pos.get_pos(), obj_pos.get_tag());
    //         let obj_ref = ObjectRef::on_disk(obj_pos, self.ts);
    //         match self.obj_table.insert(*oid, obj_ref, self.min_ts) {
    //             Ok(()) => {}
    //             Err(oid) => self.current_gc_ctx.push(oid),
    //         }
    //         self.meta_logs.push((*oid, obj_pos));
    //     }
    //     // del
    //     for oid in self.del_objs.drain(..) {
    //         match self.obj_table.remove(oid, self.ts, self.min_ts) {
    //             Ok(()) => {}
    //             Err(oid) => self.current_gc_ctx.push(oid),
    //         }
    //         self.meta_logs.push((oid, ObjectPos::default()));
    //     }
    //     // insert dirty meta log
    //     for (oid, _) in self.meta_logs.iter() {
    //         let pid = self.obj_table.get_page_id(*oid);
    //         self.dirty_pages.insert(pid);
    //     }
    //     true
    // }
}

#[cfg(test)]
mod tests {

}
