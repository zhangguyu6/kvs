use crate::error::TdbError;
use crate::object::{Object, ObjectId, ObjectRef, Versions};
use crate::storage::{DataLogFileReader, ObjectPos};
use crate::transaction::TimeStamp;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::mem;
use std::sync::{
    atomic::{AtomicPtr, AtomicU32, Ordering},
    Arc,
};
use std::u32;

// 4K
pub const TABLE_PAGE_SIZE: usize = 1 << 12;
// 512
pub const OBJ_PRE_PAGE: usize = TABLE_PAGE_SIZE / mem::size_of::<u64>();
// 1 << 20
const MAX_PAGE_NUM: usize = u32::MAX as usize / OBJ_PRE_PAGE;

pub type PageId = u32;

/// Manage OBJ_PRE_PAGE objects, each object may have differernt version and protected by rwlock
pub struct TablePage {
    children: Vec<RwLock<Versions>>,
}

impl Default for TablePage {
    fn default() -> Self {
        let mut children = Vec::with_capacity(OBJ_PRE_PAGE);
        for _ in 0..children.capacity() {
            children.push(RwLock::default());
        }
        Self { children }
    }
}

pub struct InnerTable {
    pages: Vec<AtomicPtr<TablePage>>,
    used_page_num: AtomicU32,
}

impl InnerTable {
    /// New will allocate MAX_PAGE_NUM null_ptr, but only initialize page_num TablePage
    pub fn new(pnum: usize) -> Self {
        assert!(pnum <= MAX_PAGE_NUM);
        // pre allocated all page
        let mut pages = Vec::with_capacity(MAX_PAGE_NUM);
        for _ in 0..pnum {
            let page_ptr = Box::into_raw(Box::new(TablePage::default()));
            pages.push(AtomicPtr::new(page_ptr));
        }
        Self {
            pages,
            used_page_num: AtomicU32::new(pnum as u32),
        }
    }

    #[inline]
    fn get_page_ptr(&self, pid: PageId) -> &AtomicPtr<TablePage> {
        &self.pages[pid as usize]
    }

    #[inline]
    pub fn get_page_id(oid: ObjectId) -> PageId {
        oid / OBJ_PRE_PAGE as PageId
    }

    #[inline]
    pub fn get_table_index(oid: ObjectId) -> usize {
        oid as usize % OBJ_PRE_PAGE
    }

    /// Locks object by oid with shared read access
    /// # Panics
    /// Panics if the oid overflows max allocated oid
    fn get_readlock(&self, oid: ObjectId) -> RwLockReadGuard<Versions> {
        let pid = Self::get_page_id(oid);
        let page_ptr = self.get_page_ptr(pid).load(Ordering::SeqCst);
        assert!(!page_ptr.is_null());
        let index = Self::get_table_index(oid);
        let page_ref = unsafe { page_ptr.as_ref() }.unwrap();
        page_ref.children[index].read()
    }

    /// Locks object by oid with exclusive write access
    /// # Panics
    /// Panics if the oid overflows max allocated oid
    fn get_writelock(&self, oid: u32) -> RwLockWriteGuard<Versions> {
        let page_id = Self::get_page_id(oid);
        let page_ptr = self.get_page_ptr(page_id).load(Ordering::SeqCst);
        assert!(!page_ptr.is_null());
        let index = Self::get_table_index(oid);
        let page_ref = unsafe { page_ptr.as_ref() }.unwrap();
        page_ref.children[index].write()
    }

    /// Initialize table page less than or equal to pid
    /// # Panics
    /// Panics if pid overflow or pid less than initialize page oid
    /// Return old used page num
    pub fn extend_to(&self, pid: PageId) -> u32 {
        let used_page_num = self.used_page_num.load(Ordering::Relaxed);
        let new_page_num = pid + 1;
        assert!(pid as usize + 1 <= MAX_PAGE_NUM && pid >= used_page_num);
        for pid in used_page_num..new_page_num {
            let page_ptr = self.get_page_ptr(pid).load(Ordering::SeqCst);
            assert!(page_ptr.is_null());
            let page_ptr = Box::into_raw(Box::new(TablePage::default()));
            self.get_page_ptr(pid).store(page_ptr, Ordering::SeqCst);
        }
        self.used_page_num.store(new_page_num, Ordering::SeqCst);
        used_page_num
    }
    #[inline]
    pub fn get_page_num(&self) -> usize {
        self.used_page_num.load(Ordering::SeqCst) as usize
    }

    /// Get object by oid
    /// # Errors
    /// Return error if object is not find or I/O error
    pub fn get(
        &self,
        oid: ObjectId,
        ts: TimeStamp,
        file: &mut DataLogFileReader,
    ) -> Result<(ObjectPos, Arc<Object>), TdbError> {
        let read_versions = self.get_readlock(oid);
        if let Some(obj_ref) = read_versions.find_obj_ref(ts) {
            let pos = obj_ref.obj_pos.clone();
            if let Some(arc_obj) = obj_ref.obj_ref.upgrade() {
                return Ok((pos, arc_obj));
            } else {
                drop(read_versions);
                let mut write_versions = self.get_writelock(oid);
                let obj_mut = write_versions.find_obj_mut(ts).unwrap();
                if let Some(arc_obj) = obj_mut.obj_ref.upgrade() {
                    return Ok((pos, arc_obj));
                } else {
                    let obj = file.read_obj(&pos)?;
                    let arc_obj = Arc::new(obj);
                    obj_mut.obj_ref = Arc::downgrade(&arc_obj);
                    return Ok((pos, arc_obj));
                }
            }
        }
        Err(TdbError::NotFindObject)
    }

    /// Insert object and try to free old version
    /// # Errors
    /// Return Err(oid) if object version must be clear next time
    pub fn insert(
        &self,
        oid: ObjectId,
        version: ObjectRef,
        min_ts: TimeStamp,
    ) -> Result<(), ObjectId> {
        let mut versions = self.get_writelock(oid);
        versions.add(version);
        if !versions.is_clear() {
            versions.try_clear(min_ts);
        }
        if versions.history.len() == 1 {
            Ok(())
        } else {
            Err(oid)
        }
    }

    /// Remove object from table
    /// # Errors
    /// Return Err(oid) if object version must be clear next time
    pub fn remove(&self, oid: ObjectId, ts: TimeStamp, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.get_writelock(oid);
        versions.obsolete_newest(ts);
        versions.try_clear(min_ts);
        if versions.is_clear() {
            Ok(())
        } else {
            Err(oid)
        }
    }

    /// Try to free old version after insert and remove
    /// # Errors
    /// Return Err(oid) if object version must be clear next time
    pub fn try_gc(&self, oid: ObjectId, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.get_writelock(oid);
        versions.try_clear(min_ts);
        if versions.is_clear() {
            Ok(())
        } else {
            Err(oid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Dev;
    use crate::tree::Entry;
    use std::env;
    #[test]
    fn test_table() {
        let dev = Dev::open(env::current_dir().unwrap()).unwrap();
        let mut data_file = dev.get_data_log_reader().unwrap();
        let table = InnerTable::new(1);
        assert!(table.get(0, 0, &mut data_file).is_err());
        let entry = Entry::default();
        let obj = Object::E(entry);
        let arc_obj = Arc::new(obj);
        let obj_ref = ObjectRef::new(&arc_obj, ObjectPos::default(), 0);
        assert_eq!(table.insert(0, obj_ref, 0), Ok(()));
        assert!(table.get(0, 0, &mut data_file).is_ok());
        let obj_ref = ObjectRef::new(&arc_obj, ObjectPos::default(), 2);
        assert_eq!(table.insert(0, obj_ref, 1), Err(0));
        assert_eq!(table.try_gc(0, 2), Ok(()));
        assert!(table.get(0, 0, &mut data_file).is_err());
    }

}
