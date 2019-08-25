mod dev;
mod obj_pos;
mod data_log_file;
mod meta_log_file;
mod meta_table_file;
use crate::error::TdbError;
pub use dev::{Dev};
pub use obj_pos::{ObjectPos, MAX_DATABASE_SIZE, MAX_OBJECT_SIZE};
pub use data_log_file::{DataLogFileReader,DataLogFilwWriter};
pub use meta_log_file::{MetaLogFileReader,MetaLogFileWriter};
pub use meta_table_file::{MetaTableFileReader,MetaTableFileWriter};


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
