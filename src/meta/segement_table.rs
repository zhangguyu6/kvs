use super::SuperBlock;
use crate::error::TdbError;
use crate::storage::{BlockId, Deserialize, ObjectPos, Serialize, BLOCK_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::mem;

pub type SegementId = u16;

pub const SEGEMENT_SIZE: usize = 1 << 31;
pub const MAX_GC_SEGEMENT_SIZE: usize = SEGEMENT_SIZE / 2;
pub const SEGEMENT_PAGE_SIZE: usize = BLOCK_SIZE;
pub const SEGEMENT_PER_PAGE: usize =
    (BLOCK_SIZE - mem::size_of::<u16>() - mem::size_of::<u16>()) / mem::size_of::<u32>();

// used size , 0 for empty
#[derive(Clone, Debug)]
pub struct SegementInfo {
    size: u32,
    used: bool,
}

impl From<u32> for SegementInfo {
    fn from(num: u32) -> Self {
        let size = num >> 1;
        let used = (num & 0b1) == 0b1;
        Self { size, used }
    }
}

impl Into<u32> for SegementInfo {
    fn into(self) -> u32 {
        if self.used {
            (self.size << 1 | 0b1)
        } else {
            (self.size << 1 & (!0b1))
        }
    }
}

#[derive(Clone, Debug)]
pub struct SegementPage(SegementId, Vec<SegementInfo>);

impl SegementPage {
    fn get_segement(&self, sid: SegementId) -> Option<&SegementInfo> {
        self.1.get((sid - self.0) as usize)
    }

    fn get_segement_mut(&mut self, sid: SegementId) -> Option<&mut SegementInfo> {
        self.1.get_mut((sid - self.0) as usize)
    }
}

impl Serialize for SegementPage {
    fn serialize(&self, mut writer: &mut [u8]) -> Result<(), TdbError> {
        assert!(writer.len() >= SEGEMENT_PAGE_SIZE);
        assert!(self.1.len() <= SEGEMENT_PER_PAGE);
        writer.write_u16::<LittleEndian>(self.0)?;
        writer.write_u16::<LittleEndian>(self.1.len() as u16)?;
        for segement_info in self.1.iter() {
            writer.write_u32::<LittleEndian>(segement_info.clone().into())?;
        }
        Ok(())
    }
}

impl Deserialize for SegementPage {
    fn deserialize(mut reader: &[u8]) -> Result<Self, TdbError> {
        let sid = reader.read_u16::<LittleEndian>()?;
        let len = reader.read_u16::<LittleEndian>()? as usize;
        let mut segements = Vec::with_capacity(len);
        for _ in 0..len {
            segements.push(SegementInfo::from(reader.read_u32::<LittleEndian>()?));
        }
        Ok(SegementPage(sid, segements))
    }
}

#[derive()]
pub struct SegementInfoTable<'a> {
    superblock: &'a SuperBlock,
    segements: Vec<SegementInfo>,
    active_segement: SegementId,
    // start block id of first segement
    start_block: BlockId,
    total_used_size: usize,
}

impl<'a> SegementInfoTable<'a> {
    fn allocate(&mut self, size: usize) -> Result<ObjectPos, TdbError> {
        if self.get_segement_size(self.active_segement) + size > SEGEMENT_SIZE {
            self.allocate_sid()?;
        }
        let obj_pos = ObjectPos::new(
            SEGEMENT_SIZE * self.active_segement as usize
                + self.get_segement_size(self.active_segement),
            size,
        );
        self.set_segement_size(
            self.active_segement,
            self.get_segement_size(self.active_segement) + size,
        );
        Ok(obj_pos)
    }
    fn allocate_sid(&mut self) -> Result<SegementId, TdbError> {
        for (sid, segement_info) in self.segements.iter().enumerate() {
            if !segement_info.used && segement_info.size == 0 {
                self.active_segement = sid as SegementId;
                self.segements[sid].used = true;
                return Ok(sid as SegementId);
            }
        }
        Err(TdbError::NoSpace)
    }
    fn free(&mut self, obj_pos: &ObjectPos) {
        let sid = self.get_segementid(obj_pos.get_bid());
        let size = self.get_segement_size(sid);
        self.set_segement_size(sid, size - obj_pos.get_len());
    }

    fn get_blockid(&self, sid: SegementId) -> BlockId {
        ((self.start_block as usize * BLOCK_SIZE + sid as usize * SEGEMENT_SIZE) / BLOCK_SIZE)
            as u32
    }

    fn get_segementid(&self, bid: BlockId) -> SegementId {
        ((bid - self.start_block) as usize * BLOCK_SIZE / SEGEMENT_SIZE) as u16
    }

    fn set_segement_size(&mut self, sid: SegementId, size: usize) {
        self.segements[sid as usize].size = size as u32;
    }

    fn get_segement_size(&self, sid: SegementId) -> usize {
        self.segements[sid as usize].size as usize
    }

    fn choose_evict_segement(&self) -> Option<SegementId> {
        let mut sid_result = None;
        for (_sid, segement_info) in self.segements.iter().enumerate() {
            if segement_info.used && self.active_segement != _sid as SegementId {
                if let Some(sid) = sid_result {
                    if (self.segements[_sid].size as usize) < self.get_segement_size(sid) {
                        sid_result = Some(_sid as u16);
                    }
                } else {
                    if (self.segements[_sid].size as usize) < MAX_GC_SEGEMENT_SIZE {
                        sid_result = Some(_sid as u16);
                    }
                }
            }
        }
        sid_result
    }
}
