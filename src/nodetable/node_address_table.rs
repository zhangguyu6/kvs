use super::{
    noderef::{NodeRef, Versions},
    NodeId,
};
use crate::storage::{BlockDev, RawBlockDev};
use crate::transaction::{GLOBAL_MIN_TS, LOCAL_TS};
use crate::tree::{Node, NodeKind};
use crate::utils::RadixTree;
use std::sync::{atomic::Ordering, Arc};

pub struct NodeAddressTable<Dev> {
    tree: Arc<RadixTree<Versions>>,
    dev: Arc<BlockDev<Dev>>,
}

impl<Dev> Clone for NodeAddressTable<Dev> {
    fn clone(&self) -> Self {
        Self {
            tree: self.tree.clone(),
            dev: self.dev.clone(),
        }
    }
}

impl<Dev: RawBlockDev + Unpin> NodeAddressTable<Dev> {
    pub fn with_capacity(cap: usize, dev: Arc<BlockDev<Dev>>) -> Self {
        Self {
            tree: Arc::new(RadixTree::default()),
            dev: dev,
        }
    }
    pub fn get(&self, node_id: NodeId) -> Option<Arc<Node>> {
        if let Some(versions) = self.tree.get_readlock(node_id) {
            let node_ref = versions.find_node_ref(LOCAL_TS.with(|ts| *ts.borrow()));
            if let Some(node_ref) = node_ref {
                if let Some(arc_node) = node_ref.node_ptr.upgrade() {
                    return Some(arc_node);
                } else {
                    assert!(versions.node_kind != NodeKind::Del);
                    let node = self
                        .dev
                        .sync_read_node(&node_ref.node_pos, versions.node_kind)
                        .unwrap();
                    drop(versions);
                    let arc_node = Arc::new(node);
                    let mut versions = self.tree.get_writelock(node_id).unwrap();
                    let node_mut = versions
                        .find_node_mut(LOCAL_TS.with(|ts| *ts.borrow()))
                        .unwrap();
                    if node_mut.node_ptr.strong_count() == 0 {
                        node_mut.node_ptr = Arc::downgrade(&arc_node);
                    }
                    return Some(arc_node);
                }
            }
        }
        None
    }

    pub fn add(&self, node_id: NodeId, version: NodeRef) {
        let mut new_versions = self.tree.get_or_touchwritelock(node_id as u32);
        assert!(new_versions.history.is_empty());
        assert!(new_versions.node_kind == NodeKind::Del);
        // change node_kind from DEl to other
        new_versions.node_kind = version.node_ptr.upgrade().unwrap().get_kind();
        new_versions.history.push_back(version);
    }

    //  None if no need to gc, Some(node_id) for next gc
    pub fn append(&self, node_id: NodeId, version: NodeRef) -> Option<NodeId> {
        let mut versions = self.tree.get_writelock(node_id).unwrap();
        let min_ts = GLOBAL_MIN_TS.load(Ordering::SeqCst);
        loop {
            if let Some(version) = versions.history.front() {
                if version.commit_ts < min_ts {
                    let version = versions.history.pop_front().unwrap();
                    drop(version)
                }
            }
            break;
        }
        versions.history.push_back(version);
        if versions.history.len() == 1 {
            None
        } else {
            Some(node_id)
        }
    }

    // remove node from radixtree
    // if versions is empty return node_id and free,
    // else return None and try remove next time
    pub fn remove(&self, node_id: NodeId) -> Option<NodeId> {
        let mut versions = self.tree.get_writelock(node_id).unwrap();
        let min_ts = GLOBAL_MIN_TS.load(Ordering::SeqCst);
        loop {
            if let Some(version) = versions.history.front() {
                if version.commit_ts < min_ts {
                    let version = versions.history.pop_front().unwrap();
                    drop(version)
                }
            }
            break;
        }
        if versions.history.len() == 0 {
            versions.history.shrink_to_fit();
            versions.node_kind = NodeKind::Del;
            Some(node_id)
        } else {
            let version = NodeRef::del();
            versions.history.push_back(version);
            None
        }
    }
}
