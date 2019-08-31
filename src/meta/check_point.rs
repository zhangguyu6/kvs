use crate::error::TdbError;
use crate::meta::{InnerTable, PageId};
use crate::object::{ObjectId, UNUSED_OID};
use crate::storage::{Deserialize, ObjectPos, Serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::mem;

const MAGIC_NUM: u32 = 0xfAfAfAfA;

/// Meta data redo log
/// Write to Meta file every write transcation
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckPoint {
    // current checkpoint size,
    pub size: u32,
    // magic
    pub magic: u32,
    // for gc
    pub data_removed_size: u64,
    pub data_size: u64,
    pub root_oid: ObjectId,
    // meta log area used size
    pub meta_size: u32,
    // meta file len = tablepage_nums  * 4096
    pub tablepage_nums: u32,
    pub obj_changes: Vec<(ObjectId, ObjectPos)>,
}

impl CheckPoint {
    pub fn new(
        data_removed_size: u64,
        data_size: u64,
        root_oid: ObjectId,
        meta_size: u32,
        tablepage_nums: u32,
        obj_changes: Vec<(ObjectId, ObjectPos)>,
    ) -> Self {
        let mut cp = Self {
            // current checkpoint size,
            size: 0,
            // magic
            magic: MAGIC_NUM,
            // for gc
            data_removed_size,
            data_size,
            root_oid,
            meta_size,
            tablepage_nums,
            obj_changes,
        };
        let cp_len = cp.len();
        cp.size = cp_len as u32;
        cp.meta_size += cp_len as u32;
        cp
    }

    pub fn merge(mut cps: Vec<CheckPoint>) -> CheckPoint {
        assert!(cps.len() > 0);
        let mut changes: HashMap<ObjectId, ObjectPos> = HashMap::default();

        for cp in cps.iter() {
            for (oid, obj_pos) in cp.obj_changes.iter() {
                changes.insert(*oid, *obj_pos);
            }
        }
        let mut changes: Vec<(ObjectId, ObjectPos)> = changes.drain().collect();
        changes.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut last_cp = cps.pop().unwrap();
        last_cp.obj_changes = changes;
        last_cp
    }

    pub fn get_dirty_pages(&self) -> HashSet<PageId> {
        let mut dirty_pages = HashSet::default();
        for (oid, _) in self.obj_changes.iter() {
            dirty_pages.insert(InnerTable::get_page_id(*oid));
        }
        dirty_pages
    }

    #[inline]
    pub fn len(&self) -> usize {
        // size
        mem::size_of::<u32>()
        // crcmagic 32
        + mem::size_of::<u32>()
        // data_removed_size
            + mem::size_of::<u64>()
            // datasizen
            + mem::size_of::<u64>()
            // root_oid
            + mem::size_of::<u32>()
            // meta_size
            + mem::size_of::<u32>()
            // tablepage_nums 
            + mem::size_of::<u32>()
            // obj_changes len
            + mem::size_of::<u32>()
            // obj_changes
            + self.obj_changes.len() * (mem::size_of::<ObjectId>() + mem::size_of::<u64>())
    }
}

impl Default for CheckPoint {
    fn default() -> Self {
        let mut cp = Self {
            // current checkpoint size,
            size: 0,
            // magic
            magic: MAGIC_NUM,
            // for gc
            data_removed_size: 0,
            data_size: 0,
            root_oid: UNUSED_OID,
            // meta log area used size
            meta_size: 0,
            // meta file len = tablepage_nums * 4096
            tablepage_nums: 0,
            obj_changes: Vec::with_capacity(0),
        };
        let cp_len = cp.len();
        cp.size = cp_len as u32;
        cp.meta_size = cp_len as u32;
        cp
    }
}

impl Serialize for CheckPoint {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, TdbError> {
        writer.write_u32::<LittleEndian>(self.size)?;
        writer.write_u32::<LittleEndian>(self.magic)?;
        writer.write_u64::<LittleEndian>(self.data_removed_size)?;
        writer.write_u64::<LittleEndian>(self.data_size)?;
        writer.write_u32::<LittleEndian>(self.root_oid)?;
        writer.write_u32::<LittleEndian>(self.meta_size)?;
        writer.write_u32::<LittleEndian>(self.tablepage_nums)?;
        writer.write_u32::<LittleEndian>(self.obj_changes.len() as u32)?;
        for i in 0..self.obj_changes.len() {
            writer.write_u32::<LittleEndian>(self.obj_changes[i].0)?;
            writer.write_u64::<LittleEndian>((self.obj_changes[i].1).0)?
        }
        Ok(self.len())
    }
}

impl Deserialize for CheckPoint {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        let size = reader.read_u32::<LittleEndian>()?;
        if size == 0 {
            return Err(TdbError::SerializeError);
        }
        let magic = reader.read_u32::<LittleEndian>()?;
        if magic != MAGIC_NUM {
            return Err(TdbError::SerializeError);
        }
        let data_removed_size = reader.read_u64::<LittleEndian>()?;
        let data_size = reader.read_u64::<LittleEndian>()?;
        let root_oid = reader.read_u32::<LittleEndian>()?;
        let meta_size = reader.read_u32::<LittleEndian>()?;
        let tablepage_nums = reader.read_u32::<LittleEndian>()?;
        let obj_change_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut obj_changes = Vec::with_capacity(obj_change_len);
        for _ in 0..obj_change_len {
            let oid = reader.read_u32::<LittleEndian>()?;
            let obj_pos = reader.read_u64::<LittleEndian>()?;
            obj_changes.push((oid, ObjectPos(obj_pos)));
        }
        Ok(Self {
            size,
            magic,
            data_removed_size,
            data_size,
            root_oid,
            meta_size,
            tablepage_nums,
            obj_changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::ObjectTag;
    #[test]
    fn test_cp_size() {
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

    #[test]
    fn test_cp_merge() {
        let cp0 = CheckPoint::new(
            0,
            4096,
            1,
            123,
            10,
            vec![(0, ObjectPos::default()), (2, ObjectPos::default())],
        );
        let mut cp1 = CheckPoint::new(
            1,
            4096,
            2,
            234,
            9,
            vec![
                (0, ObjectPos::new(1, 1, ObjectTag::Entry)),
                (1, ObjectPos::new(1, 1, ObjectTag::Entry)),
            ],
        );
        let cp2 = CheckPoint::merge(vec![cp0.clone(), cp1.clone()]);
        cp1.obj_changes.push((2, ObjectPos::default()));
        assert_eq!(cp1, cp2);
    }

}
