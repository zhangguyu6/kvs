mod data_file;
mod dev;
mod meta_file;
mod obj_pos;
mod table_file;
use crate::error::TdbError;
pub use data_file::{DataFileReader, DataFilwWriter};
pub use dev::Dev;
pub use meta_file::{MetaFileWriter, MetaLogFileReader};
pub use obj_pos::ObjectPos;
pub use table_file::{TableFileReader, TableFileWriter};

use std::io::{Read, Write};

pub trait Serialize {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, TdbError>;
}

pub trait Deserialize: Sized {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError>;
}
