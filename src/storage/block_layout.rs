use crate::{
    error::TxnError,
    index::{Branch, Leaf},
    transaction::TimeStamp,
};
pub const BLOCK_SIZE: usize = 4096;

pub type BlockId = u64;

pub struct RawBlock([u8; BLOCK_SIZE]);

impl Default for RawBlock {
    fn default() -> Self {
        RawBlock([0;BLOCK_SIZE])
    }
}
// enum Block {
//     D(BlockRef<Tuple>),
//     DIndex1(Vec<BlockRef<Tuple>>),
//     DIndex2(Vec<>),
//     L(BlockRef<Leaf>),
//     B(BlockRef<Branch>),
// }


