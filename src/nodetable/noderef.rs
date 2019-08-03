use crate::storage::{BlockId, BlockOffKind};
use crate::transaction::{TimeStamp, LOCAL_TS};
use crate::tree::{Node,NodeKind};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicPtr, Ordering};

pub struct NodeRef {
    pub node_ptr: AtomicPtr<Node>,
    pub block_id: BlockId,
    pub block_offset_kind: BlockOffKind,
    // commit_ts don't write to disk,but the time when read from dsik/new create
    pub commit_ts: TimeStamp,
}

impl NodeRef {
    pub fn is_del(&self) -> bool {
        self.block_id == 0 && self.block_offset_kind.get_kind() == NodeKind::Del
    }
    pub fn del() -> Self {
        Self {
            node_ptr: AtomicPtr::default(),
            block_id: 0,
            block_offset_kind: BlockOffKind::new(0b11),
            commit_ts: LOCAL_TS.with(|ts| *ts.borrow()),
        }
    }
}

impl Drop for NodeRef {
    fn drop(&mut self) {
        let node_ptr = self.node_ptr.load(Ordering::SeqCst);
        if !node_ptr.is_null() {
            unsafe { Box::from_raw(node_ptr) };
        }
    }
}

pub struct Versions {
    pub history: VecDeque<NodeRef>,
}

impl Versions {
    pub fn find_node_ref(&self, ts: TimeStamp) -> Option<&NodeRef> {
        let mut index = self.history.len();
        for _index in 0..self.history.len() {
            if self.history[_index].commit_ts <= ts {
                index = _index;
            } else {
                break;
            }
        }
        if index == self.history.len() {
            None
        } else {
            self.history.get(index)
        }
    }

    pub fn find_node_mut(&mut self, ts: TimeStamp) -> Option<&mut NodeRef> {
        let mut index = self.history.len();
        for _index in 0..self.history.len() {
            if self.history[_index].commit_ts <= ts {
                index = _index;
            } else {
                break;
            }
        }
        if index == self.history.len() {
            None
        } else {
            self.history.get_mut(index)
        }
    }
}

impl Default for Versions {
    fn default() -> Self {
        Self {
            history: VecDeque::with_capacity(0),
        }
    }
}
