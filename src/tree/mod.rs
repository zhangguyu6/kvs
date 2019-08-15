mod branch;
mod entry;
mod leaf;
mod tree_reader;
mod tree_writer;
pub use branch::Branch;
pub use entry::Entry;
pub use leaf::Leaf;
use std::u8;
// 255 byte
pub const MAX_KEY_LEN: usize = u8::MAX as usize ;

type Key = Vec<u8>;

type Val = Vec<u8>;
