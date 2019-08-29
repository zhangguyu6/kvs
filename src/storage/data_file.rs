use super::ObjectPos;
use crate::storage::{Deserialize};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectTag, META_DATA_ALIGN,Branch, Entry, Leaf,MutObject},
};
use byteorder::WriteBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::collections::hash_map::IterMut;

const DEFAULT_BUF_SIZE: usize = 4096 * 2;

pub struct DataFileReader {
    reader: BufReader<File>,
}

impl DataFileReader {
    pub fn new(file: File) -> Self {
        DataFileReader {
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

pub struct DataFilwWriter {
    writer: BufWriter<File>,
    size:usize,
}

impl DataFilwWriter {
    pub fn new(file: File,size:usize) -> Self {
        DataFilwWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
            size,
        }
    }
    pub fn write_objs(
        &mut self,
        objs:IterMut<ObjectId,MutObject>
    ) -> Result<usize, TdbError> {
        for (oid,mut_obj) in objs {
            match mut_obj {
                MutObject::Dirty(obj) | MutObject::New(obj) => {
                        obj.get_pos_mut().set_pos(self.size as u64);
                        self.size += obj.write(&mut self.writer)?;
                }
                _ => {}
            }
        }
        if self.size % META_DATA_ALIGN != 0 {
            for _ in self.size & META_DATA_ALIGN .. META_DATA_ALIGN {
                self.writer.write_u8(0)?;
                self.size += 1;
            }
        }
        Ok(self.size)
    }
}
