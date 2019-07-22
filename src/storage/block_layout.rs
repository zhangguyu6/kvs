use crate::{
    error::TdbError,
    index::{Branch, Leaf},
    transaction::TimeStamp,
};

pub const BLOCK_SIZE: usize = 4096;

pub type BlockId = u64;

pub struct RawBlock([u8; BLOCK_SIZE]);

impl Default for RawBlock {
    fn default() -> Self {
        RawBlock([0; BLOCK_SIZE])
    }
}

pub enum Block {
    D(Tuple),
}

impl Block {
    pub fn as_ref<B: AsBlock>(&self) -> &B {
        B::get_ref(self)
    }
}

#[derive(Clone)]
pub struct Tuple {
    key: Vec<u8>,
    val: Vec<u8>,
}

#[repr(u8)]
pub enum BlockKind {
    T,
}

pub trait BlockSerialize {
    fn serialize(&self) -> Result<RawBlock, TdbError>;
}

pub trait BlockDeserialize: Sized {
    fn deserialize(raw_block: RawBlock) -> Result<Self, TdbError>;
}

pub trait AsBlock: BlockSerialize + BlockDeserialize + Into<Block> {
    fn get_ref(block: &Block) -> &Self;
    // fn get_kind(&self) -> BlockKind;
}
