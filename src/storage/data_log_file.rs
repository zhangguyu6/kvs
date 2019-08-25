use super::ObjectPos;
use crate::storage::{Deserialize, StaticSized};
use crate::tree::{Branch, Entry, Leaf};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectTag, META_DATA_ALIGN},
};
use byteorder::WriteBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

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
        let pos: SeekFrom = obj_pos.clone().into();
        println!("pos is {:?}", pos);
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
            size += arc_obj.len();
            arc_obj.write(&mut self.writer)?;
        }
        for (_, arc_entry) in entry_objs.iter() {
            let current_pos = self.writer.seek(SeekFrom::Current(0))?;
            size += arc_entry.len();
            arc_entry.write(&mut self.writer)?;
            let end_pos = self.writer.seek(SeekFrom::Current(0))?;
            assert_eq!(arc_entry.len() as u64, end_pos - current_pos);
        }
        self.writer.flush()?;
        Ok(())
    }
}
