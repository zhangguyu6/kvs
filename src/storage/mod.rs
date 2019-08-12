mod block_allocater;
mod io;
// mod layout;
// mod segement;

pub use io::{BlockDev, RawBlockDev};
pub use block_allocater::BlockAllocater;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u32;

#[derive(Eq, PartialEq)]
pub struct ObjectPos {
    pub block_start: BlockId,
    pub block_len: u16,
    pub offset: u16,
}
