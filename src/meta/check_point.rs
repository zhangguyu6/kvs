// use super::{SegementId, SegementInfo};
use crate::error::TdbError;
use crate::object::{Object, ObjectId, UNUSED_OID};
use crate::storage::{Deserialize, ObjectPos, Serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::mem;
use std::u32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckPoint {
    // current checkpoint size,
    pub check_point_len: u32,
    // crc fast
    pub crc: u32,
    // for gc
    pub data_log_remove_len: u64,
    pub data_log_len: u64,
    pub root_oid: ObjectId,
    // meta log area used size, 0 mean meta log is apply
    pub meta_log_total_len: u32,
    // meta file len = allocated_obj_nums * 4096
    pub allocated_obj_nums: u32,
    pub obj_changes: Vec<(ObjectId, Option<ObjectPos>)>,
}

impl Default for CheckPoint {
    fn default() -> Self {
        Self {
            // current checkpoint size,
            check_point_len: CheckPoint::min_len() as u32,
            // crc fast
            crc: u32::MAX,
            // for gc
            data_log_remove_len: 0,
            data_log_len: 0,
            root_oid: UNUSED_OID,
            // meta log area used size, 0 mean meta log is apply
            meta_log_total_len: 0,
            // meta file len = allocated_obj_nums * 4096
            allocated_obj_nums: 0,
            obj_changes: Vec::with_capacity(0),
        }
    }
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
            // allocated_obj_nums 
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
            // allocated_obj_nums
            + mem::size_of::<u32>()
              // obj_changes len
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
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), TdbError> {
        writer.write_u32::<LittleEndian>(self.check_point_len)?;
        writer.write_u32::<LittleEndian>(self.crc)?;
        writer.write_u64::<LittleEndian>(self.data_log_remove_len)?;
        writer.write_u64::<LittleEndian>(self.data_log_len)?;
        writer.write_u32::<LittleEndian>(self.root_oid)?;
        writer.write_u32::<LittleEndian>(self.meta_log_total_len)?;
        writer.write_u32::<LittleEndian>(self.allocated_obj_nums)?;
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
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        let check_point_len = reader.read_u32::<LittleEndian>()?;
        if check_point_len == 0 {
            return Err(TdbError::SerializeError);
        }
        let crc = reader.read_u32::<LittleEndian>()?;
        if crc != u32::MAX {
            return Err(TdbError::SerializeError);
        }
        let data_log_remove_len = reader.read_u64::<LittleEndian>()?;
        let data_log_len = reader.read_u64::<LittleEndian>()?;
        let root_oid = reader.read_u32::<LittleEndian>()?;
        let meta_log_total_len = reader.read_u32::<LittleEndian>()?;
        let allocated_obj_nums = reader.read_u32::<LittleEndian>()?;
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
            allocated_obj_nums,
            obj_changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_checkpoint_size() {
        let mut cp = CheckPoint::default();
        assert_eq!(cp.len(), 4 + 4 + 8 + 8 + 4 + 4 + 4 + 4);
        assert_eq!(cp.len(), CheckPoint::min_len());
        cp.obj_changes.push((1, None));
        assert_eq!(cp.len(), CheckPoint::min_len() + 4 + 8);
    }

    #[test]
    fn test_cp_serialize_deserialize() {
        let mut cp0 = CheckPoint::default();
        let mut buf = [0; 4096];
        assert!(cp0.serialize(&mut &mut buf[..]).is_ok());
        let cp1 = CheckPoint::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(cp0, cp1);
        assert!(CheckPoint::deserialize(&mut &buf[..8]).is_err());
        // println!("{:?}", CheckPoint::deserialize(&mut &buf[..8]));
        cp0.obj_changes.push((1, None));
        assert!(cp0.serialize(&mut &mut buf[..]).is_ok());
        let cp1 = CheckPoint::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(cp0, cp1);
    }
}
