use crate::error::TdbError;
use crate::meta::{
    CheckPoint,  PageId,TablePage,TABLE_PAGE_SIZE,InnerTable,OBJ_PRE_PAGE
};
use crate::storage::{Deserialize, Serialize};
use crate::utils::BitMap;
use byteorder::{LittleEndian, ReadBytesExt};
use log::debug;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write,Read};

const DEFAULT_BUF_SIZE: usize = 4096 * 2;
pub struct TableFileWriter {
    writer: BufWriter<File>,
}

impl TableFileWriter {
    pub fn new(file: File) -> Self {
        TableFileWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
        }
    }
    pub fn write_page(&mut self, pid: PageId, page: &TablePage) -> Result<(), TdbError> {
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
        TableFileReader {
            reader: file,
        }
    }
}

impl TableFileReader {
    pub fn new(file: File) -> Self {
        TableFileReader {
            reader: file,
        }
    }
    pub fn read_table(
        &mut self,
        cp: &CheckPoint,
    ) -> Result<(InnerTable, BitMap), TdbError> {
        debug!("start read table, checkpoint is {:?}",cp);
        self.reader.seek(SeekFrom::Start(0))?;
        let table =InnerTable::new(cp.tablepage_nums as usize);
        let mut bitmap = BitMap::with_capacity(cp.tablepage_nums as usize * OBJ_PRE_PAGE );
        let mut buf:[u8;TABLE_PAGE_SIZE] = [0;TABLE_PAGE_SIZE];
        for pid in 0..cp.tablepage_nums {
            self.reader.read_exact(&mut buf);
            let table_page = TablePage::deserialize(&mut &buf[..])?;
            let _pid = table.append_page(table_page);
            assert_eq!(_pid ,pid);
            let buf_reader = &mut &buf[..];
            for i in 0..OBJ_PRE_PAGE {
                let pos = buf_reader.read_u64::<LittleEndian>()?;
                if pos != 0 {
                    bitmap.set_bit(pid as usize * OBJ_PRE_PAGE + i , true);
                }
            }
        }
        Ok((table, bitmap))
    }
}
