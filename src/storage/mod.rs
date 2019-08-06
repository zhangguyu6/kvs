mod allocater;
mod io;
mod layout;
mod segement;

pub use io::{G_DEV,RawBlockDev,BlockDev};
pub use layout::{BlockDeserialize, BlockId, BlockSerialize, BLOCK_SIZE};

