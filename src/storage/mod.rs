mod obj_pos;
mod dev;
use crate::error::TdbError;
pub use dev::{DataLogFile,MetaTableFile,MetaLogFile,Dev};
pub use obj_pos::{ObjectPos,MAX_DATABASE_SIZE,MAX_OBJECT_SIZE};
use std::io::{Read,Write};


 


pub trait Serialize {
    fn serialize<W:Write>(&self, writer: &mut W) -> Result<(), TdbError>;
}

pub trait Deserialize: Sized {
    fn deserialize<R:Read>(reader: &mut R) -> Result<Self, TdbError>;
}
