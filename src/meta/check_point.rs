use crate::error::TdbError;
use crate::meta::{PageId, OBJECT_TABLE_ENTRY_PRE_PAGE};
use crate::object::{ObjectId, UNUSED_OID};
use crate::storage::{Deserialize, ObjectPos, Serialize, StaticSized};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{HashMap, HashSet};
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
    // meta log area used size
    pub meta_log_total_len: u32,
    // meta file len = obj_tablepage_nums  * 4096
    pub obj_tablepage_nums: u32,
    pub obj_changes: Vec<(ObjectId, ObjectPos)>,
}

impl CheckPoint {
    pub fn new(
        data_log_remove_len: u64,
        data_log_len: u64,
        root_oid: ObjectId,
        meta_log_total_len: u32,
        obj_tablepage_nums: u32,
        obj_changes: Vec<(ObjectId, ObjectPos)>,
    ) -> Self {
        let mut cp = Self {
            // current checkpoint size,
            check_point_len: 0,
            // crc fast
            crc: u32::MAX,
            // for gc
            data_log_remove_len,
            data_log_len,
            root_oid,
            meta_log_total_len,
            obj_tablepage_nums,
            obj_changes,
        };
        let cp_len = cp.len();
        cp.check_point_len = cp_len as u32;
        cp.meta_log_total_len += cp_len as u32;
        cp
    }

    pub fn merge(cps: &Vec<CheckPoint>) -> (Vec<(ObjectId, ObjectPos)>, HashSet<PageId>) {
        let mut changes: HashMap<ObjectId, ObjectPos> = HashMap::default();
        let mut dirty_pages = HashSet::default();
        for cp in cps.iter() {
            for (oid, obj_pos) in cp.obj_changes.iter() {
                changes.insert(*oid, *obj_pos);
            }
        }
        let mut changes: Vec<(ObjectId, ObjectPos)> = changes.drain().collect();
        changes.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for (oid, _) in changes.iter() {
            dirty_pages.insert(oid / OBJECT_TABLE_ENTRY_PRE_PAGE as u32);
        }
        (changes, dirty_pages)
    }
}

impl Default for CheckPoint {
    fn default() -> Self {
        let mut cp = Self {
            // current checkpoint size,
            check_point_len: 0,
            // crc fast
            crc: u32::MAX,
            // for gc
            data_log_remove_len: 0,
            data_log_len: 0,
            root_oid: UNUSED_OID,
            // meta log area used size
            meta_log_total_len: 0,
            // meta file len = obj_tablepage_nums * 4096
            obj_tablepage_nums: 0,
            obj_changes: Vec::with_capacity(0),
        };
        let cp_len = cp.len();
        cp.check_point_len = cp_len as u32;
        cp.meta_log_total_len = cp_len as u32;
        cp
    }
}

impl StaticSized for CheckPoint {
    #[inline]
    fn len(&self) -> usize {
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
            // obj_tablepage_nums 
            + mem::size_of::<u32>()
            // obj_changes len
            + mem::size_of::<u32>()
            // obj_changes
            + self.obj_changes.len() * (mem::size_of::<ObjectId>() + mem::size_of::<u64>())
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
        writer.write_u32::<LittleEndian>(self.obj_tablepage_nums)?;
        writer.write_u32::<LittleEndian>(self.obj_changes.len() as u32)?;
        for i in 0..self.obj_changes.len() {
            writer.write_u32::<LittleEndian>(self.obj_changes[i].0)?;
            writer.write_u64::<LittleEndian>((self.obj_changes[i].1).0)?
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
        let obj_tablepage_nums = reader.read_u32::<LittleEndian>()?;
        let obj_change_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut obj_changes = Vec::with_capacity(obj_change_len);
        for _ in 0..obj_change_len {
            let oid = reader.read_u32::<LittleEndian>()?;
            let obj_pos = reader.read_u64::<LittleEndian>()?;
            obj_changes.push((oid, ObjectPos(obj_pos)));
        }
        Ok(Self {
            check_point_len,
            crc,
            data_log_remove_len,
            data_log_len,
            root_oid,
            meta_log_total_len,
            obj_tablepage_nums,
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
        cp.obj_changes.push((1, ObjectPos::default()));
        assert_eq!(cp.len(), 4 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 8);
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
        cp0.obj_changes.push((1, ObjectPos::default()));
        assert!(cp0.serialize(&mut &mut buf[..]).is_ok());
        let cp1 = CheckPoint::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(cp0, cp1);
    }
}
