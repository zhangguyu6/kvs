use crate::storage::BlockId;
use crate::transaction::TimeStamp;
use crate::tree::Node;
use std::collections::VecDeque;
use std::sync::atomic::AtomicPtr;

pub struct NodeRef {
    node_ptr: AtomicPtr<Node>,
    block_id: BlockId,
    commit_ts: TimeStamp,
}

impl NodeRef {
    fn is_del(&self) -> bool {
        self.block_id == 0
    }
}

pub struct Vesions {
    history: VecDeque<NodeRef>,
}
















