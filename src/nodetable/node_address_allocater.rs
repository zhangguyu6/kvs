use super::NodeId;
use crate::utils::BitMap;

pub struct NodeAddressAllocater {
    bitmap: BitMap<u32>,
    last_used: usize,
}

impl NodeAddressAllocater {
    pub fn allocate(&mut self) -> Option<NodeId> {
        if let Some(new_node_id) = self.bitmap.first_zero_with_hint_set(self.last_used) {
            self.last_used = new_node_id;
            Some(new_node_id as u32)
        } else {
            None
        }
    }
    pub fn free(&mut self, node_id: NodeId) {
        self.bitmap.set_bit(node_id as usize, false);
        self.last_used = node_id as usize;
    }
}
 