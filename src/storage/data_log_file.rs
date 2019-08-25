use super::ObjectPos;
use crate::meta::{CheckPoint, ObjectTablePage, PageId, OBJECT_TABLE_PAGE_SIZE};
use crate::storage::{Deserialize, Serialize, StaticSized};
use crate::tree::{Branch, Entry, Leaf};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectLog, ObjectTag, META_DATA_ALIGN},
};
use byteorder::WriteBytesExt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_BUF_SIZE: usize = 4096 * 2;

pub struct DataLogFileReader {
    reader: BufReader<File>,
}

impl DataLogFileReader {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::with_capacity(DEFAULT_BUF_SIZE, file),
        }
    }

    pub fn read_obj(&mut self, obj_pos: &ObjectPos) -> Result<Object, TdbError> {
        self.reader.seek(obj_pos.clone().into())?;
        let obj_tag = obj_pos.get_tag();
        match obj_tag {
            ObjectTag::Leaf => Ok(Object::L(Leaf::deserialize(&mut self.reader)?)),
            ObjectTag::Branch => Ok(Object::B(Branch::deserialize(&mut self.reader)?)),
            ObjectTag::Entry => Ok(Object::E(Entry::deserialize(&mut self.reader)?)),
        }
    }
}

pub struct DataLogFilwWriter {
    writer: BufWriter<File>,
}

impl DataLogFilwWriter {
    pub fn new(file: File) -> Self {
        Self {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
        }
    }
    pub fn write_obj_log(
        &mut self,
        index_objs: &Vec<(ObjectId, Object)>,
        entry_objs: &Vec<(ObjectId, Object)>,
    ) -> Result<(), TdbError> {
        let mut size = 0;
        for (_, arc_obj) in index_objs.iter() {
            size += arc_obj.static_size();
            arc_obj.write(&mut self.writer)?;
        }
        for (_, arc_entry) in entry_objs.iter() {
            size += arc_entry.static_size();
            arc_entry.write(&mut self.writer)?;
        }
        if size % META_DATA_ALIGN != 0 {
            for _ in (size % META_DATA_ALIGN)..META_DATA_ALIGN {
                self.writer.write_u8(0)?;
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}
