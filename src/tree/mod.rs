mod branch;
mod leaf;
mod entry;
pub use branch::Branch;
pub use leaf::Leaf;
pub use entry::Entry;
// 511 byte
const MAX_KEY_LEN: usize = 1 << 9 - 1;

type Key = Vec<u8>;

type Val = Vec<u8>;
