use crate::error::TdbError;

pub const BLOCK_SIZE: usize = 4096;

pub type BlockId = u32;


pub trait BlockSerialize {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError>;
}

pub trait BlockDeserialize: Sized {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError>;
}

