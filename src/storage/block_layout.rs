use crate::{
    error::TdbError,
    index::{Branch, Leaf},
    transaction::TimeStamp,
    tree::NodeKind,
};
use std::io::{Read, Write};

pub const BLOCK_SIZE: usize = 4096;

pub type BlockId = u32;

// if page size is 4096, BlockOff must be aligned to 4096 / (1<<6) = 64 byte, 2 bit for node kind
#[derive(Copy, Debug, Clone)]
pub struct BlockOffKind(u8);

impl BlockOffKind {
    pub fn new(byte:u8) -> Self {
        Self(byte)
    }
    #[inline]
    pub fn get_offset(&self) -> usize {
        (self.0 >> 2) as usize
    }
    #[inline]
    pub fn get_kind(&self) -> NodeKind {
        NodeKind::from(self.0)
    }
}

pub trait BlockSerialize {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), TdbError>;
}

pub trait BlockDeserialize: Sized {
    fn deserialize<R: Read>(reader: &R) -> Result<Self, TdbError>;
}

pub trait AsBlock: BlockSerialize + BlockDeserialize {}
