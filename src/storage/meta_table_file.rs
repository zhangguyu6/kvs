use crate::error::TdbError;
use crate::meta::{
    CheckPoint, ObjectAllocater, ObjectTable, ObjectTablePage, PageId, OBJECT_TABLE_ENTRY_PRE_PAGE,
    OBJECT_TABLE_PAGE_SIZE,
};
use crate::object::Versions;
use crate::storage::{Deserialize, Serialize};
use crate::utils::Node;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::sync::atomic::Ordering;

pub struct MetaTableFileWriter {
    writer: BufWriter<File>,
}

impl From<File> for MetaTableFileWriter {
    fn from(file: File) -> Self {
        Self {
            writer: BufWriter::new(file),
        }
    }
}

impl MetaTableFileWriter {
    pub fn write_page(&mut self, pid: PageId, page: ObjectTablePage) -> Result<(), TdbError> {
        self.writer
            .seek(SeekFrom::Start(pid as u64 * OBJECT_TABLE_PAGE_SIZE as u64))?;
        page.serialize(&mut self.writer)
    }
}

pub struct MetaTableFileReader {
    reader: BufReader<File>,
}

impl From<File> for MetaTableFileReader {
    fn from(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
        }
    }
}

impl MetaTableFileReader {
    pub fn read_table(
        &mut self,
        cp: &CheckPoint,
    ) -> Result<(ObjectTable, ObjectAllocater), TdbError> {
        self.reader.seek(SeekFrom::Start(0))?;
        let obj_table =
            ObjectTable::new(cp.obj_tablepage_nums as usize * OBJECT_TABLE_ENTRY_PRE_PAGE);
        let mut obj_allocater = ObjectAllocater::new(
            cp.obj_tablepage_nums as usize * OBJECT_TABLE_ENTRY_PRE_PAGE,
            cp.data_log_remove_len,
            cp.data_log_len,
        );
        for pid in 0..cp.obj_tablepage_nums {
            let obj_table_page = ObjectTablePage::deserialize(&mut self.reader)?;
            for index in 0..obj_table_page.0.len() {
                if !obj_table_page.0[index].is_empty() {
                    obj_allocater.set_bit(pid as usize * OBJECT_TABLE_ENTRY_PRE_PAGE + index, true);
                }
            }
            let node: Node<Versions> = obj_table_page.into();
            let page_ptr = obj_table.get_page_ptr(pid);
            let old_ptr = page_ptr.swap(Box::into_raw(Box::new(node)), Ordering::SeqCst);
            assert!(old_ptr.is_null());
        }
        Ok((obj_table, obj_allocater))
    }
}
