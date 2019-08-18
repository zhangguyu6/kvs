use crate::error::TdbError;
use crate::object::{Object, ObjectId, ObjectTag, Versions};
use crate::storage::{MetaLogFile, ObjectPos};
use crate::transaction::{TimeStamp, MAX_TS};
use crate::utils::RadixTree;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::mem;
use std::sync::{
    atomic::{AtomicPtr, Ordering},
    Arc, Weak,
};

// 1024 * 1024
pub const OBJECT_TABLE_DEFAULT_PAGE_NUM: usize = 1 << 20;
// 4K
pub const OBJECT_TABLE_PAGE_SIZE: usize = 1 << 12;
// 510
pub const OBJECT_TABLE_ENTRY_PRE_PAGE: usize = (OBJECT_TABLE_PAGE_SIZE
    - mem::size_of::<TimeStamp>()
    - mem::size_of::<ObjectId>()
    - mem::size_of::<u32>())
    / mem::size_of::<u64>();

#[derive(Clone)]
pub struct ObjectRef {
    // don't own obj, just get ref from cache
    pub obj_ref: Weak<Object>,
    pub obj_pos: ObjectPos,
}

#[derive(Clone)]
pub struct ObjectTablePage(TimeStamp, ObjectId, Vec<ObjectRef>);

impl ObjectTablePage {
    pub fn new(oid: ObjectId, ts: TimeStamp) -> Self {
        Self(ts, oid, Vec::with_capacity(OBJECT_TABLE_ENTRY_PRE_PAGE))
    }
    pub fn get(&self, oid: ObjectId) -> Option<&ObjectRef> {
        let index = (oid - self.1) as usize;
        self.2.get(index)
    }
    pub fn get_mut(&mut self, oid: ObjectId) -> Option<&mut ObjectRef> {
        let index = (oid - self.1) as usize;
        self.2.get_mut(index)
    }
}

pub struct ObjectTablePageVesion {
    page: ObjectTablePage,
    start_ts: TimeStamp,
    next_version: AtomicPtr<ObjectTablePageVesion>,
}

impl ObjectTablePageVesion {
    pub fn new(page: ObjectTablePage, ts: TimeStamp) -> Self {
        Self {
            page: page,
            start_ts: ts,
            next_version: AtomicPtr::default(),
        }
    }

    pub fn get_obj_ref(&self, oid: ObjectId, ts: TimeStamp) -> Option<&ObjectRef> {
        let mut version_ref = self;
        loop {
            if version_ref.start_ts <= ts {
                return version_ref.page.get(oid);
            } else {
                let version_ptr = version_ref.next_version.load(Ordering::SeqCst);
                // this version shouldn't gc
                assert!(!version_ptr.is_null());
                version_ref = unsafe { &*version_ptr };
            }
        }
    }

    pub fn try_clear(&self, min_ts: TimeStamp) {
        let mut version_ref = self;
        let mut end_ts = MAX_TS;
        loop {
            if version_ref.start_ts > min_ts {
                end_ts = version_ref.start_ts;
                let version_ptr = version_ref.next_version.load(Ordering::SeqCst);
                if version_ptr.is_null() {
                    break;
                } else {
                    version_ref = unsafe { &*version_ptr };
                }
            } else if version_ref.start_ts == min_ts {
                let version_ptr = version_ref.next_version.load(Ordering::SeqCst);
                if !version_ptr.is_null() {
                    unsafe {
                        Box::from_raw(version_ptr);
                    }
                }
                break;
            } else {
                if end_ts <= min_ts {
                    let version_ptr = version_ref.next_version.load(Ordering::SeqCst);
                    if !version_ptr.is_null() {
                        unsafe {
                            Box::from_raw(version_ptr);
                        }
                    }
                    break;
                } else {
                    end_ts = version_ref.start_ts;
                    let version_ptr = version_ref.next_version.load(Ordering::SeqCst);
                    if version_ptr.is_null() {
                        break;
                    } else {
                        version_ref = unsafe { &*version_ptr };
                    }
                }
            }
        }
    }
}

// pub struct ObjectTable {
//     obj_table_pages:Vec<RwLock<Weak<ObjectTablePage>>>
// }

// impl ObjectTable {
//     pub fn with_capacity(cap: usize) -> Self {
//         let mut obj_table_pages = Vec::with_capacity(cap);

//         ObjectTable {
//             tree: RadixTree::with_capacity(cap).unwrap(),
//         }
//     }
//     pub fn get(
//         &self,
//         oid: ObjectId,
//         ts: TimeStamp,
//         file: &MetaLogFile,
//     ) -> Option<Arc<Object>> {
//         if let Some(read_versions) = self.tree.get_readlock(oid) {
//             if let Some(obj_ref) = read_versions.find_obj_ref(ts) {
//                 if let Some(arc_node) = obj_ref.obj_ref.upgrade() {
//                     return Some(arc_node);
//                 } else {
//                     let pos = obj_ref.obj_pos.clone();
//                     let tag = obj_ref.obj_info.tag.clone();
//                     // drop because read from disk may waste many time
//                     drop(read_versions);
//                     let node = file.sync_read_node(&pos, &tag).unwrap();
//                     let mut write_versions = self.tree.get_writelock(oid).unwrap();
//                     let mut arc_node = Arc::new(node);
//                     let obj_mut = write_versions.find_obj_mut(ts).unwrap();
//                     if obj_mut.obj_ref.strong_count() == 0 {
//                         obj_mut.obj_ref = Arc::downgrade(&arc_node);
//                     } else {
//                         arc_node = obj_mut.obj_ref.upgrade().unwrap();
//                     }
//                     return Some(arc_node);
//                 }
//             }
//         }
//         None
//     }

//     // Return Ok if no need to gc, Err(oid) for next gc
//     pub fn insert(
//         &self,
//         oid: ObjectId,
//         version: ObjectRef,
//         min_ts: TimeStamp,
//     ) -> Result<(), ObjectId> {
//         let mut versions = self.tree.get_writelock(oid).unwrap();
//         if !versions.history.is_empty() {
//             versions.try_clear(min_ts);
//         }
//         versions.add(version);
//         if versions.history.len() == 1 {
//             Ok(())
//         } else {
//             Err(oid)
//         }
//     }

//     // Remove node from radixtree
//     // if versions is empty ,free and return None,
//     // else return None and try remove next time
//     pub fn remove(&self, oid: ObjectId, ts: TimeStamp, min_ts: TimeStamp) -> Result<(), ObjectId> {
//         let mut versions = self.tree.get_writelock(oid).unwrap();
//         versions.obsolete_newest(min_ts);
//         versions.try_clear(min_ts);
//         if versions.history.len() == 0 {
//             Ok(())
//         } else {
//             Err(oid)
//         }
//     }

//     // Try to clean Object after insert and remove
//     // Return Ok() if Object is clean  else Err(oid)
//     pub fn try_gc(&self, oid: ObjectId, min_ts: TimeStamp) -> Result<(), ObjectId> {
//         let mut versions = self.tree.get_writelock(oid).unwrap();
//         versions.try_clear(min_ts);
//         if versions.history.len() == 0 {
//             Ok(())
//         } else {
//             Err(oid)
//         }
//     }
//     // Try to extend object table
//     // Return current cap
//     pub fn extend(&self, extend: usize) -> Result<usize, TdbError> {
//         self.tree.extend(extend)
//     }
// }
