mod branch;
mod entry;
mod leaf;
mod tree_reader;
mod tree_writer;
pub use branch::Branch;
pub use entry::Entry;
pub use leaf::Leaf;
// 511 byte
const MAX_KEY_LEN: usize = 1 << 9 - 1;

type Key = Vec<u8>;

type Val = Vec<u8>;
