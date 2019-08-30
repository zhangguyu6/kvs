use super::ObjectPos;
use crate::storage::Deserialize;
use crate::{
    error::TdbError,
    object::{Branch, Entry, Leaf, MutObject, Object, ObjectId, ObjectTag, META_DATA_ALIGN},
};
use byteorder::WriteBytesExt;
use std::collections::hash_map::IterMut;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

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
    size: u64,
    removed_size:u64,
}

impl DataFilwWriter {
    pub fn new(file: File, size: u64,removed_size:u64) -> Self {
        DataFilwWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
            size,
            removed_size
        }
    }
    pub fn write_objs(
        &mut self,
        mut objs: IterMut<ObjectId, MutObject>,
    ) -> Result<(u64,u64), TdbError> {
        // write branch and entry, align to 4k
        for (oid, mut_obj) in &mut objs {
            match mut_obj {
                MutObject::Dirty(obj, _) | MutObject::New(obj) => {
                    if !obj.is::<Entry>() {
                        obj.get_pos_mut().set_pos(self.size as u64);
                        self.size += obj.write(&mut self.writer)? as u64;
                        assert!(self.size % META_DATA_ALIGN as u64 == 0);
                    }
                }
                _ => {}
            }
        }
        // write entry, not align
        for (oid, mut_obj) in &mut objs {
            match mut_obj {
                MutObject::Dirty(obj, _) | MutObject::New(obj) => {
                    if obj.is::<Entry>() {
                        obj.get_pos_mut().set_pos(self.size as u64);
                        self.size += obj.write(&mut self.writer)? as u64;
                    }
                }
                _ => {}
            }
        }
        // make commit align to 4K
        if self.size % META_DATA_ALIGN as u64 != 0 {
            for _ in self.size & META_DATA_ALIGN as u64..META_DATA_ALIGN as u64 {
                self.writer.write_u8(0)?;
                self.size += 1;
            }
        }
        for (oid, mut_obj) in &mut objs {
            match mut_obj {
                MutObject::Dirty(_,arc_obj) | MutObject::Del(arc_obj) => {
                        self.removed_size +=  arc_obj.get_pos().get_len() as u64;
                }
                _ => {}
            }
        }
        Ok((self.size,self.removed_size))
    }
}
