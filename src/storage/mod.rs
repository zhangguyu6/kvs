mod io;
mod obj_pos;
mod dev;
use crate::error::TdbError;
pub use io::{BlockDev, Dummy, RawBlockDev};
pub use obj_pos::ObjectPos;


pub const BLOCK_SIZE: usize = 4096;
// 16T
pub const MAX_DEV_SIZE: usize = 1 << 44;
// 1M
pub const MAX_OBJ_SIZE: usize = 1 << 20;

pub type BlockId = u32;
 
pub const UNUSED_BLOCK_ID: u32 = 0;


pub trait Serialize {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError>;
}

pub trait Deserialize: Sized {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError>;
}