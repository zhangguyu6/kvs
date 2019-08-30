use crate::error::TdbError;
use crate::meta::{CheckPoint, InnerTable, PageId, TablePage, OBJ_PRE_PAGE, TABLE_PAGE_SIZE};
use crate::object::ObjectRef;
use crate::storage::{Deserialize, Serialize};
use crate::utils::BitMap;
use byteorder::{LittleEndian, ReadBytesExt};
use log::debug;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

const DEFAULT_BUF_SIZE: usize = 4096 * 2;
pub struct TableFileWriter {
    writer: BufWriter<File>,
    pub used_page_num: u32,
}

impl TableFileWriter {
    pub fn new(file: File, used_page_num: u32) -> Self {
        TableFileWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
            used_page_num,
        }
    }
    pub fn write_page(&mut self, pid: PageId, page: &TablePage) -> Result<(), TdbError> {
        if pid + 1 > self.used_page_num {
            self.used_page_num = pid + 1;
        }
        self.writer
            .seek(SeekFrom::Start(pid as u64 * TABLE_PAGE_SIZE as u64))?;
        page.serialize(&mut self.writer)?;
        Ok(())
    }
    pub fn flush(&mut self) -> Result<(), TdbError> {
        self.writer.flush()?;
        Ok(())
    }
}

pub struct TableFileReader {
    reader: File,
}

impl From<File> for TableFileReader {
    fn from(file: File) -> Self {
        TableFileReader { reader: file }
    }
}

impl TableFileReader {
    pub fn new(file: File) -> Self {
        TableFileReader { reader: file }
    }
    pub fn read_table(&mut self, cp: &CheckPoint) -> Result<(InnerTable, BitMap), TdbError> {
        debug!("start read table, checkpoint is {:?}", cp);
        self.reader.seek(SeekFrom::Start(0))?;
        let table = InnerTable::with_capacity(cp.tablepage_nums as usize);
        let mut bitmap = BitMap::with_capacity(cp.tablepage_nums as usize * OBJ_PRE_PAGE);
        let mut buf: [u8; TABLE_PAGE_SIZE] = [0; TABLE_PAGE_SIZE];
        for pid in 0..cp.tablepage_nums {
            self.reader.read_exact(&mut buf)?;
            let table_page = TablePage::deserialize(&mut &buf[..])?;
            let _pid = table.append_page(table_page);
            assert_eq!(_pid, pid);
            let buf_reader = &mut &buf[..];
            for i in 0..OBJ_PRE_PAGE {
                let pos = buf_reader.read_u64::<LittleEndian>()?;
                if pos != 0 {
                    bitmap.set_bit(pid as usize * OBJ_PRE_PAGE + i, true);
                }
            }
        }
        if let Some((oid, _)) = cp.obj_changes.last() {
            let last_pid = InnerTable::get_page_id(*oid);
            if last_pid >= cp.tablepage_nums {
                table.extend_to(last_pid);
                bitmap.extend_to((last_pid + 1) as usize * OBJ_PRE_PAGE);
            }
        }
        for (oid, obj_pos) in cp.obj_changes.iter() {
            if obj_pos.is_empty() {
                table.remove(*oid, 0, 0).unwrap();
                bitmap.set_bit(*oid as usize, false);
            } else {
                let version = ObjectRef::on_disk(obj_pos.clone(), 0);
                table.insert(*oid, version, 0).unwrap();
                bitmap.set_bit(*oid as usize, true);
            }
        }
        Ok((table, bitmap))
    }
}
