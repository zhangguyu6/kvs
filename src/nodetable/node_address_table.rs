use crate::utils::RadixTree;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

struct NodeAddressTable {
    tree: RadixTree<u32>,
}

impl NodeAddressTable {
    pub fn get(&self) -> RwLockReadGuard<'_, u32> {
        unimplemented!()
    }
}
