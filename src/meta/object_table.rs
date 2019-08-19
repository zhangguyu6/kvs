use crate::object::{Object, ObjectId, ObjectRef, Versions};
use crate::storage::{DataLogFile, ObjectPos};
use crate::transaction::TimeStamp;
use crate::utils::{Node, RadixTree};
use parking_lot::RwLock;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::u32;

const MAX_CAP: u32 = u32::MAX;

pub const OBJECT_TABLE_DEFAULT_PAGE_NUM: usize = MAX_CAP as usize / OBJECT_TABLE_ENTRY_PRE_PAGE;
// 4K
pub const OBJECT_TABLE_PAGE_SIZE: usize = 1 << 12;
// 511
pub const OBJECT_TABLE_ENTRY_PRE_PAGE: usize =
    (OBJECT_TABLE_PAGE_SIZE - mem::size_of::<ObjectId>() - mem::size_of::<u32>())
        / mem::size_of::<u64>();

#[derive(Clone)]
pub struct ObjectTablePage(u32, Vec<Option<ObjectPos>>);

impl Into<Node<Versions>> for ObjectTablePage {
    fn into(mut self) -> Node<Versions> {
        let mut versions = Vec::with_capacity(self.1.len());
        for obj_pos in self.1.drain(..) {
            if let Some(obj_pos) = obj_pos {
                let obj_ref = ObjectRef::on_disk(obj_pos, 0);
                versions.push(RwLock::new(Versions::new_only(obj_ref)));
            }
        }
        Node { children: versions }
    }
}

pub struct ObjectTable {
    obj_table_pages: RadixTree<Versions>,
}

impl ObjectTable {
    pub fn new(cap: usize) -> Self {
        Self {
            obj_table_pages: RadixTree::new(
                cap as u32,
                OBJECT_TABLE_DEFAULT_PAGE_NUM as u32,
                OBJECT_TABLE_ENTRY_PRE_PAGE as u32,
            ),
        }
    }
    pub fn len(&self) -> usize {
        self.obj_table_pages.get_len() as usize
    }

    pub fn get(&self, oid: ObjectId, ts: TimeStamp, file: &mut DataLogFile) -> Option<Arc<Object>> {
        if let Some(read_versions) = self.obj_table_pages.get_readlock(oid) {
            if let Some(obj_ref) = read_versions.find_obj_ref(ts) {
                if let Some(arc_obj) = obj_ref.obj_ref.upgrade() {
                    return Some(arc_obj);
                } else {
                    let pos = obj_ref.obj_pos.clone();
                    drop(read_versions);
                    let mut write_versions = self.obj_table_pages.get_writelock(oid).unwrap();
                    let obj_mut = write_versions.find_obj_mut(ts).unwrap();
                    if let Some(arc_obj) = obj_mut.obj_ref.upgrade() {
                        return Some(arc_obj);
                    } else {
                        let obj = file.sync_read_obj(&pos).unwrap();
                        let arc_obj = Arc::new(obj);
                        obj_mut.obj_ref = Arc::downgrade(&arc_obj);
                        return Some(arc_obj);
                    }
                }
            }
        }
        None
    }

    // Return Ok if no need to gc, Err(oid) for next gc
    pub fn insert(
        &self,
        oid: ObjectId,
        version: ObjectRef,
        min_ts: TimeStamp,
    ) -> Result<(), ObjectId> {
        let mut versions = self.obj_table_pages.get_writelock(oid).unwrap();
        if !versions.is_clear() {
            versions.try_clear(min_ts);
        }
        versions.add(version);
        if versions.history.len() == 1 {
            Ok(())
        } else {
            Err(oid)
        }
    }

    // Remove node from radixtree
    // if versions is empty ,free and return None,
    // else return None and try remove next time
    pub fn remove(&self, oid: ObjectId, ts: TimeStamp, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.obj_table_pages.get_writelock(oid).unwrap();
        versions.obsolete_newest(ts);
        versions.try_clear(min_ts);
        if versions.is_clear() {
            Ok(())
        } else {
            Err(oid)
        }
    }

    // Try to clean Object after insert and remove
    // Return Ok() if Object is clean  else Err(oid)
    pub fn try_gc(&self, oid: ObjectId, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.obj_table_pages.get_writelock(oid).unwrap();
        versions.try_clear(min_ts);
        if versions.is_clear() {
            Ok(())
        } else {
            Err(oid)
        }
    }
    // Try to extend object table
    // Return old len
    pub fn extend(&self, extend: usize) -> usize {
        self.obj_table_pages.extend(extend as u32) as usize
    }

    pub fn append_page(&self, obj_table_page: ObjectTablePage) {
        let old_len = self.obj_table_pages.add_len(obj_table_page.1.len() as u32);
        assert_eq!(
            old_len,
            OBJECT_TABLE_ENTRY_PRE_PAGE as u32 * obj_table_page.0
        );
        let node_ptr = self.obj_table_pages.get_node_ptr(obj_table_page.0 as usize);
        let node: Node<Versions> = obj_table_page.into();
        let old_ptr = node_ptr.swap(Box::into_raw(Box::new(node)), Ordering::SeqCst);
        assert!(old_ptr.is_null());
    }

    pub fn get_page(&self, page_id: u32) -> ObjectTablePage {
        let node_ptr = self
            .obj_table_pages
            .get_node_ptr(page_id as usize)
            .load(Ordering::SeqCst);
        assert!(!node_ptr.is_null());
        let mut page = Vec::with_capacity(OBJECT_TABLE_ENTRY_PRE_PAGE);
        let node_ref = unsafe { &*node_ptr };
        for versions in node_ref.children.iter() {
            let read_versions = versions.read();
            page.push(read_versions.get_newest_objpos());
        }
        ObjectTablePage(page_id, page)
    }

    pub fn get_page_id(&self, oid: ObjectId) -> u32 {
        self.obj_table_pages.get_level1_index(oid) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DataLogFile;
    use crate::tree::Entry;
    #[test]
    fn test_object_table() {
        let obj_table = ObjectTable::new(0);
        assert_eq!(obj_table.len(), 0);
        assert_eq!(obj_table.extend(OBJECT_TABLE_ENTRY_PRE_PAGE), 0);
        let entry = Entry::default();
        let obj = Object::E(entry);
        let arc_obj = Arc::new(obj);
        let obj_ref = ObjectRef::new(&arc_obj, ObjectPos::default(), 0);
        assert_eq!(obj_table.insert(0, obj_ref, 0), Ok(()));
        let mut data_file = DataLogFile::default();
        assert_eq!(obj_table.get(0, 0, &mut data_file), Some(arc_obj.clone()));
        assert_eq!(obj_table.get(0, 1, &mut data_file), Some(arc_obj.clone()));
        assert_eq!(obj_table.remove(0, 1, 0), Err(0));
        assert_eq!(obj_table.get(0, 0, &mut data_file), Some(arc_obj.clone()));
        assert_eq!(obj_table.get(0, 1, &mut data_file), None);
        assert_eq!(obj_table.try_gc(0, 1), Ok(()));
        assert_eq!(obj_table.get(0, 0, &mut data_file), None);
        assert_eq!(obj_table.get(0, 1, &mut data_file), None);
        assert_eq!(
            obj_table.insert(0, ObjectRef::new(&arc_obj, ObjectPos::default(), 0), 0),
            Ok(())
        );
        assert_eq!(obj_table.len(), 511);
        assert_eq!(obj_table.get_page_id(0), 0);
        assert_eq!(obj_table.get_page_id(510), 0);
        assert_eq!(obj_table.get_page_id(511), 1);
        let obj_table_page = obj_table.get_page(0);
        assert_eq!(obj_table_page.0, 0);
        assert_eq!(obj_table_page.1.len(), 511);
        assert_eq!(obj_table_page.1[0], Some(ObjectPos::default()));
        for i in 1..511 {
            assert_eq!(obj_table_page.1[i], None);
        }
    }
}
