mod dev;
mod obj_pos;
use crate::error::TdbError;
pub use dev::{DataLogFileReader, DataLogFilwWriter, Dev, MetaLogFileWriter, MetaTableFileWriter};
pub use obj_pos::{ObjectPos, MAX_DATABASE_SIZE, MAX_OBJECT_SIZE};
use std::io::{Read, Write};

pub trait Serialize {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), TdbError>;
}

pub trait Deserialize: Sized {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError>;
}

pub trait StaticSized: Sized {
    fn len(&self) -> usize;
    fn static_size(&self) -> usize;
}
