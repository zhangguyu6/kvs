mod block_allocater;
mod io;
// mod layout;
// mod segement;

pub use io::{BlockDev, RawBlockDev,Dummy};
pub use block_allocater::BlockAllocater;

pub const BLOCK_SIZE: usize = 4096;
pub type BlockId = u32;

#[derive(Eq, PartialEq,Clone)]
pub struct ObjectPos {
    pub block_start: BlockId,
    pub block_len: u16,
    pub offset: u16,
}

impl Default for ObjectPos {
    fn default() -> Self {
        Self {
            block_start:0,
            block_len:0,
            offset:0
        }
    }
}
