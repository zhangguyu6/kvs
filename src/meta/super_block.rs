// use super::{SegementId, SegementInfo};
use crate::error::TdbError;
// use crate::object::ObjectId;
use crate::storage::{
    Deserialize, ObjectPos, Serialize
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// use std::mem;
use crate::transaction::TimeStamp;
pub const CHECK_POINT_SIZE:usize = 4096;
// pub const OBJECT_TABLE_PAGE_SIZE: usize = BLOCK_SIZE;
// pub const OBJECTPOS_PER_PAGE: usize =
//     (OBJECT_TABLE_PAGE_SIZE - mem::size_of::<ObjectId>() - mem::size_of::<u32>())
//         / mem::size_of::<u64>();

// pub const SEGEMENT_TABLE_PAGE_SIZE: usize = BLOCK_SIZE;
// pub const SEGEMENT_PER_PAGE: usize =
//     (BLOCK_SIZE - mem::size_of::<u16>() - mem::size_of::<u16>()) / mem::size_of::<u32>();

// #[derive(Clone, Debug)]
// pub struct ObjectTablePage(ObjectId, Vec<ObjectPos>);

// impl ObjectTablePage {
//     fn get_obj_pos(&self, oid: ObjectId) -> Option<&ObjectPos> {
//         self.1.get((oid - self.0) as usize)
//     }
//     fn get_obj_pos_mut(&mut self, oid: ObjectId) -> Option<&mut ObjectPos> {
//         self.1.get_mut((oid - self.0) as usize)
//     }
// }

// impl Serialize for ObjectTablePage {
//     fn serialize(&self, mut writer: &mut [u8]) -> Result<(), TdbError> {
//         writer.write_u32::<LittleEndian>(self.0)?;
//         writer.write_u32::<LittleEndian>(self.1.len() as u32)?;
//         for obj_pos in self.1.iter() {
//             writer.write_u64::<LittleEndian>(obj_pos.0)?;
//         }
//         Ok(())
//     }
// }

// impl Deserialize for ObjectTablePage {
//     fn deserialize(mut reader: &[u8]) -> Result<Self, TdbError> {
//         let oid = reader.read_u32::<LittleEndian>()?;
//         let len = reader.read_u32::<LittleEndian>()? as usize;
//         let mut obj_poss = Vec::with_capacity(len);
//         for _ in 0..len {
//             obj_poss.push(ObjectPos(reader.read_u64::<LittleEndian>()?));
//         }
//         Ok(Self(oid, obj_poss))
//     }
// }

// #[derive(Clone, Debug)]
// pub struct SegementPage(SegementId, Vec<SegementInfo>);

// impl SegementPage {
//     fn get_segement(&self, sid: SegementId) -> Option<&SegementInfo> {
//         self.1.get((sid - self.0) as usize)
//     }
//     fn get_segement_mut(&mut self, sid: SegementId) -> Option<&mut SegementInfo> {
//         self.1.get_mut((sid - self.0) as usize)
//     }
// }


pub struct CheckPoint {
     ts: TimeStamp,
     meta_path:String,
     meta_log_file_path:String,
     meta_log_used_size:u64,
     data_log_file_path:String,
     data_log_used_size:u64,
     obj_table_pos:ObjectPos,
     obj_table_cap:usize,
}

impl Serialize for CheckPoint {
    fn serialize(&self, mut writer: &mut [u8]) -> Result<(), TdbError> {
        // writer.write_u64::<LittleEndian>(self.ts)?;
        // writer.write_u16::<LittleEndian>(self.meta_path.as_ref().len() as u16)?;
        // writer.write_all(self.meta_path.as_ref())?;
        unimplemented!()
    }
}

impl Deserialize for CheckPoint {
    fn deserialize(mut reader: &[u8]) -> Result<Self, TdbError> {
        unimplemented!()
    }
}
// impl SuperBlock {
//     pub fn get_bid_from_sid(&self, sid: SegementId) -> Option<BlockId> {
//         Some(self.segement_bid_start + (sid as u32 / SEGEMENT_PER_PAGE as u32))
//     }

//     pub fn get_bid_from_oid(&self, oid: ObjectId) -> Option<BlockId> {
//         Some(self.object_table_bid_start + (oid as u32 / OBJECTPOS_PER_PAGE as u32))
//     }
// }
