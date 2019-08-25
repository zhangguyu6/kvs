use crate::error::TdbError;
use crate::object::{Object, ObjectId, ObjectRef, Versions};
use crate::storage::{DataLogFileReader, Deserialize, ObjectPos, Serialize};
use crate::transaction::TimeStamp;
use crate::utils::{Node, RadixTree};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::RwLock;
use std::io::{Read, Write};
use std::mem;
use std::sync::atomic::{Ordering,AtomicPtr};
use std::sync::Arc;

pub const OBJECT_TABLE_DEFAULT_PAGE_NUM: usize = 1 << 21;
// 4K
pub const OBJECT_TABLE_PAGE_SIZE: usize = 1 << 12;
// 512
pub const OBJECT_TABLE_ENTRY_PRE_PAGE: usize = OBJECT_TABLE_PAGE_SIZE / mem::size_of::<u64>();
// 1 << 30
pub const OBJECT_NUM: usize = OBJECT_TABLE_DEFAULT_PAGE_NUM * OBJECT_TABLE_ENTRY_PRE_PAGE;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ObjectTablePage(pub Vec<ObjectPos>);

pub type PageId = u32;

impl Deserialize for ObjectTablePage {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        let mut obj_poss = Vec::with_capacity(OBJECT_TABLE_ENTRY_PRE_PAGE);
        for _ in 0..OBJECT_TABLE_ENTRY_PRE_PAGE {
            let obj_pos = reader.read_u64::<LittleEndian>()?;
            obj_poss.push(ObjectPos(obj_pos));
        }
        Ok(Self(obj_poss))
    }
}

impl Serialize for ObjectTablePage {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), TdbError> {
        for i in 0..OBJECT_TABLE_ENTRY_PRE_PAGE {
            writer.write_u64::<LittleEndian>(self.0[i].0)?;
        }
        Ok(())
    }
}

impl Into<Node<Versions>> for ObjectTablePage {
    fn into(mut self) -> Node<Versions> {
        let mut versions = Vec::with_capacity(self.0.len());
        for obj_pos in self.0.drain(..) {
            let obj_ref = ObjectRef::on_disk(obj_pos, 0);
            versions.push(RwLock::new(Versions::new_only(obj_ref)));
        }
        Node { children: versions }
    }
}

pub struct ObjectTable {
    obj_table_pages: RadixTree<Versions>,
}

impl Default for ObjectTable {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ObjectTable {
    pub fn new(len: usize) -> Self {
        Self {
            obj_table_pages: RadixTree::new(
                len as u32,
                OBJECT_TABLE_DEFAULT_PAGE_NUM as u32,
                OBJECT_TABLE_ENTRY_PRE_PAGE as u32,
            ),
        }
    }
    pub fn len(&self) -> usize {
        self.obj_table_pages.get_len() as usize
    }

    pub fn get(
        &self,
        oid: ObjectId,
        ts: TimeStamp,
        file: &mut DataLogFileReader,
    ) -> Result<Option<(ObjectPos,Arc<Object>)>, TdbError> {
        if let Some(read_versions) = self.obj_table_pages.get_readlock(oid) {
            if let Some(obj_ref) = read_versions.find_obj_ref(ts) {
                let pos = obj_ref.obj_pos.clone();
                if let Some(arc_obj) = obj_ref.obj_ref.upgrade() {
                    return Ok(Some((pos,arc_obj)));
                } else {
                    drop(read_versions);
                    let mut write_versions = self.obj_table_pages.get_writelock(oid).unwrap();
                    let obj_mut = write_versions.find_obj_mut(ts).unwrap();
                    if let Some(arc_obj) = obj_mut.obj_ref.upgrade() {
                        return Ok(Some((pos,arc_obj)));
                    } else {
                        let obj = file.read_obj(&pos)?;
                        let arc_obj = Arc::new(obj);
                        obj_mut.obj_ref = Arc::downgrade(&arc_obj);
                        return Ok(Some((pos,arc_obj)));
                    }
                }
            }
        }
        Ok(None)
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

    pub fn get_page(&self, pid: PageId) -> ObjectTablePage {
        let node_ptr = self
            .obj_table_pages
            .get_node_ptr(pid as usize)
            .load(Ordering::SeqCst);
        assert!(!node_ptr.is_null());
        let mut page = Vec::with_capacity(OBJECT_TABLE_ENTRY_PRE_PAGE);
        let node_ref = unsafe { &*node_ptr };
        for versions in node_ref.children.iter() {
            let read_versions = versions.read();
            page.push(read_versions.get_newest_objpos());
        }
        ObjectTablePage(page)
    }

    pub fn get_page_ptr(&self,pid:PageId ) -> &AtomicPtr<Node<Versions>> {
        self.obj_table_pages.get_node_ptr(pid as usize)
    }

    pub fn get_page_id(&self, oid: ObjectId) -> PageId {
        self.obj_table_pages.get_level1_index(oid) as PageId
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DataLogFileReader;
    use crate::tree::Entry;
    // #[test]
    // fn test_object_table() {
    //     let obj_table = ObjectTable::new(0);
    //     assert_eq!(obj_table.len(), 0);
    //     assert_eq!(obj_table.extend(OBJECT_TABLE_ENTRY_PRE_PAGE), 0);
    //     let entry = Entry::default();
    //     let obj = Object::E(entry);
    //     let arc_obj = Arc::new(obj);
    //     let obj_ref = ObjectRef::new(&arc_obj, ObjectPos::default(), 0);
    //     assert_eq!(obj_table.insert(0, obj_ref, 0), Ok(()));
    //     let mut data_file = DataLogFile::default();
    //     assert_eq!(
    //         obj_table.get(0, 0, &mut data_file),
    //         Ok(Some(arc_obj.clone()))
    //     );
    //     assert_eq!(
    //         obj_table.get(0, 1, &mut data_file),
    //         Ok(Some(arc_obj.clone()))
    //     );
    //     assert_eq!(obj_table.remove(0, 1, 0), Err(0));
    //     assert_eq!(
    //         obj_table.get(0, 0, &mut data_file),
    //         Ok(Some(arc_obj.clone()))
    //     );
    //     assert_eq!(obj_table.get(0, 1, &mut data_file), Ok(None));
    //     assert_eq!(obj_table.try_gc(0, 1), Ok(()));
    //     assert_eq!(obj_table.get(0, 0, &mut data_file), Ok(None));
    //     assert_eq!(obj_table.get(0, 1, &mut data_file), Ok(None));
    //     assert_eq!(
    //         obj_table.insert(0, ObjectRef::new(&arc_obj, ObjectPos::default(), 0), 0),
    //         Ok(())
    //     );
    //     assert_eq!(obj_table.len(), 511);
    //     assert_eq!(obj_table.get_page_id(0), 0);
    //     assert_eq!(obj_table.get_page_id(510), 0);
    //     assert_eq!(obj_table.get_page_id(511), 1);
    //     let obj_table_page = obj_table.get_page(0);
    //     assert_eq!(obj_table_page.0, 0);
    //     assert_eq!(obj_table_page.2.len(), 511);
    //     assert_eq!(obj_table_page.2[0], Some(ObjectPos::default()));
    //     for i in 1..511 {
    //         assert_eq!(obj_table_page.2[i], None);
    //     }
    // }

    #[test]
    fn test_object_table_serialize_deserialize() {
        let mut buf: [u8; 4096] = [0; 4096];
        let obj_page1 = ObjectTablePage(vec![ObjectPos::default(); OBJECT_TABLE_ENTRY_PRE_PAGE]);
        let mut slice = &mut buf[..];
        assert!(obj_page1.serialize(&mut slice).is_ok());
        let obj_page2 = ObjectTablePage::deserialize(&mut (&buf[..])).unwrap();
        assert_eq!(obj_page1, obj_page2);
        let mut obj_page1 =
            ObjectTablePage(vec![ObjectPos::default(); OBJECT_TABLE_ENTRY_PRE_PAGE]);
        obj_page1.0[0] = ObjectPos(1);
        assert!(obj_page1.serialize(&mut &mut buf[..]).is_ok());
        let obj_page2 = ObjectTablePage::deserialize(&mut (&buf[..])).unwrap();
        assert_eq!(obj_page1, obj_page2);
    }
}
