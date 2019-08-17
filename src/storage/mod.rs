mod block_allocater;
mod io;
// mod layout;
// mod segement;
use crate::error::TdbError;
pub use block_allocater::BlockAllocater;
pub use io::{BlockDev, Dummy, RawBlockDev};
use std::mem;

pub const BLOCK_SIZE: usize = 4096;
// 16T
pub const MAX_DEV_SIZE:usize = 1 << 44;
// 1M
pub const MAX_OBJ_SIZE:usize = 1 << 20;

pub type BlockId = u32;

pub const UNUSED_BLOCK_ID: u32 = 0;

#[derive(Eq, PartialEq, Clone)]
pub struct ObjectPos (pub u64);

impl ObjectPos {
    pub fn new(pos: usize, len: usize) -> Self {
        assert!(pos < MAX_DEV_SIZE);
        assert!(len < MAX_OBJ_SIZE);
        Self (((pos as u64) << 20) + len as u64)
    }
    #[inline]
    pub fn get_pos(&self) -> u64 {
        self.0 >> 20 << 20 
    }
    #[inline]
    pub fn get_bid(&self) -> BlockId {
        self.get_pos() as u32 / BLOCK_SIZE as u32
    }

    #[inline]
    pub fn get_inner_offset(&self) -> usize {
        self.get_pos() as usize % BLOCK_SIZE
    }

    #[inline]
    pub fn get_len(&self) -> usize {
        (self.0 & 0xfffff) as usize
    }

    #[inline]
    pub fn get_blk_len(&self) -> usize {
        let start = self.get_bid() as usize;
        let end = (self.get_pos() as usize+ self.get_len()  + BLOCK_SIZE - 1) / BLOCK_SIZE;
        (end - start ) * BLOCK_SIZE
    }
}


impl ObjectPos {
    #[inline]
    pub fn get_size() -> usize {
        mem::size_of::<u64>()
    }
}

impl Default for ObjectPos {
    fn default() -> Self {
        Self (0)
    }
}

impl ObjectPos {
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}


pub trait Serialize {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError>;
}

pub trait Deserialize: Sized {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError>;
}