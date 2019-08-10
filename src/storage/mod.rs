mod allocater;
mod io;
mod layout;
mod segement;

pub use io::{BlockDev, RawBlockDev, G_DEV};
pub use layout::{BlockDeserialize, BlockId, BlockSerialize, BLOCK_SIZE};


#[derive(Eq, PartialEq)]
pub struct ObjectPos {
    pub block_start: BlockId,
    pub block_len: u16,
    pub offset: u16,
}
