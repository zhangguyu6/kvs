mod obj_pos;
mod dev;
use crate::error::TdbError;
pub use dev::{DataLogFile,MetaTableFile,MetaLogFile,Dev};
pub use obj_pos::{ObjectPos,MAX_DATABASE_SIZE,MAX_OBJECT_SIZE};



 


pub trait Serialize {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError>;
}

pub trait Deserialize: Sized {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError>;
}