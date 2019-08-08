use crate::transaction::{TimeStamp, LOCAL_TS};
use crate::tree::{Node, NodeKind, NodePos};
use std::collections::VecDeque;
use std::sync::Weak;

pub struct NodeRef {
    // don't own node, just get ref from cache
    pub node_ptr: Weak<Node>,
    pub node_pos: NodePos,
    // commit_ts don't represent time write to disk,but time when from dsik/new create
    pub commit_ts: TimeStamp,
}

pub struct Versions {
    pub history: VecDeque<NodeRef>,
    pub node_kind: NodeKind,
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
            node_kind: NodeKind::default(),
        }
    }
}
