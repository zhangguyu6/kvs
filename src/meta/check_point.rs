// use super::{SegementId, SegementInfo};
use crate::error::TdbError;
use crate::object::{Object, ObjectId};
use crate::storage::{Deserialize, ObjectPos, Serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::mem;
use std::sync::Arc;
pub struct CheckPoint {
    // current checkpoint size,
    check_point_len: u32,
    // crc fast
    crc: u32,
    // for gc
    data_log_remove_len: u64,
    data_log_len: u64,
    root_oid: ObjectId,
    // meta log area used size, 0 mean meta log is apply
    meta_log_total_len: u32,
    obj_changes: Vec<(ObjectId, Option<ObjectPos>)>,
}

impl CheckPoint {
    pub fn len(&self) -> usize {
        // check_point_len
        mem::size_of::<u32>()
        // crc 32
        + mem::size_of::<u32>()
        // data_log_remove_len
            + mem::size_of::<u64>()
            // data_log_len
            + mem::size_of::<u64>()
            // root_oid
            + mem::size_of::<u32>()
            // meta_log_total_len
            + mem::size_of::<u32>()
            // obj_changes len
            + mem::size_of::<u32>()
            // obj_changes
            + self.obj_changes.len() * (mem::size_of::<ObjectId>() + mem::size_of::<u64>())
    }

    pub fn min_len() -> usize {
        // check_point_len
        mem::size_of::<u32>()
        // crc 32
        + mem::size_of::<u32>()
        // data_log_remove_len
            + mem::size_of::<u64>()
            // data_log_len
            + mem::size_of::<u64>()
            // root_oid
            + mem::size_of::<u32>()
            // meta_log_total_len
            + mem::size_of::<u32>()
    }

    pub fn malloc_obj(&mut self, obj: &Object) -> ObjectPos {
        let obj_tag = obj.get_object_info().tag;
        let obj_size = obj.get_object_info().size;
        let obj_pos = ObjectPos::new(self.data_log_len, obj_size, obj_tag);
        self.data_log_len += obj_size as u64;
        obj_pos
    }

    pub fn free_obj(&mut self, obj_pos: &ObjectPos) {
        self.data_log_remove_len += obj_pos.get_len() as u64;
    }
}
impl Serialize for CheckPoint {
    fn serialize(&self, mut writer: &mut [u8]) -> Result<(), TdbError> {
        assert!(writer.len() == self.len());
        writer.write_u32::<LittleEndian>(self.check_point_len)?;
        writer.write_u32::<LittleEndian>(self.crc)?;
        writer.write_u64::<LittleEndian>(self.data_log_remove_len)?;
        writer.write_u64::<LittleEndian>(self.data_log_len)?;
        writer.write_u32::<LittleEndian>(self.root_oid)?;
        writer.write_u32::<LittleEndian>(self.meta_log_total_len)?;
        writer.write_u32::<LittleEndian>(self.obj_changes.len() as u32)?;
        for i in 0..self.obj_changes.len() {
            writer.write_u32::<LittleEndian>(self.obj_changes[i].0)?;
            if let Some(obj_pos) = self.obj_changes[i].1.clone() {
                writer.write_u64::<LittleEndian>(obj_pos.0)?
            } else {
                writer.write_u64::<LittleEndian>(0)?
            }
        }
        Ok(())
    }
}

impl Deserialize for CheckPoint {
    fn deserialize(mut reader: &[u8]) -> Result<Self, TdbError> {
        assert!(reader.len() > Self::min_len());
        let check_point_len = reader.read_u32::<LittleEndian>()?;
        let crc = reader.read_u32::<LittleEndian>()?;
        let data_log_remove_len = reader.read_u64::<LittleEndian>()?;
        let data_log_len = reader.read_u64::<LittleEndian>()?;
        let root_oid = reader.read_u32::<LittleEndian>()?;
        let meta_log_total_len = reader.read_u32::<LittleEndian>()?;
        let obj_change_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut obj_changes = Vec::with_capacity(obj_change_len);
        for _ in 0..obj_change_len {
            let oid = reader.read_u32::<LittleEndian>()?;
            let obj_pos = reader.read_u64::<LittleEndian>()?;
            if obj_pos == 0 {
                obj_changes.push((oid, None));
            } else {
                obj_changes.push((oid, Some(ObjectPos(obj_pos))));
            }
        }
        Ok(Self {
            check_point_len,
            crc,
            data_log_remove_len,
            data_log_len,
            root_oid,
            meta_log_total_len,
            obj_changes,
        })
    }
}
