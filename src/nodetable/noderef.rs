use crate::transaction::{TimeStamp, LOCAL_TS};
use crate::tree::{Node, NodeKind, NodePos};
use std::collections::VecDeque;
use std::sync::Weak;

pub struct NodeRef {
    // don't own node, just get ref from cache
    pub node_ptr: Weak<Node>,
    pub node_pos: NodePos,
    // start_ts don't represent time write to disk,but time when read from dsik/new create
    pub start_ts: TimeStamp,
    pub end_ts: TimeStamp,
}

pub struct Versions {
    pub history: VecDeque<NodeRef>,
    pub node_kind: NodeKind,
}

impl Versions {
    pub fn find_node_ref(&self, ts: TimeStamp) -> Option<&NodeRef> {
        for node_ref in self.history.iter() {
            if node_ref.start_ts <= ts && node_ref.end_ts > ts {
                return Some(node_ref);
            }
        }
        None
    }

    pub fn find_node_mut(&mut self, ts: TimeStamp) -> Option<&mut NodeRef> {
        for node_mut in self.history.iter_mut() {
            if node_mut.start_ts <= ts && node_mut.end_ts > ts {
                return Some(node_mut);
            }
        }
        None
    }
    pub fn try_clear(&mut self, min_ts: TimeStamp) {
        loop {
            if let Some(version) = self.history.front() {
                if version.start_ts < min_ts {
                    let version = self.history.pop_front().unwrap();
                    drop(version);
                    continue;
                }
            }
            break;
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
