use super::{
    noderef::{NodeRef, Versions},
    NodeId,
};
use crate::cache::GLOBAL_SENDER;
use crate::storage::G_DEV;
use crate::transaction::{GLOBAL_MIN_TS, LOCAL_TS};
use crate::tree::{Node, NodeKind};
use crate::utils::{BitMap, RadixTree};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::sync::{atomic::Ordering, Arc};

lazy_static! {
    pub static ref G_NAT: NodeAddressTable = NodeAddressTable::with_capacity(1 << 16);
}

pub struct NodeAddressTable {
    tree: RadixTree<Versions>,
    bitmap: RwLock<(BitMap<u32>, usize)>,
}

impl NodeAddressTable {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            tree: RadixTree::default(),
            bitmap: RwLock::new((BitMap::with_capacity(cap), 0)),
        }
    }
    pub fn get(&self, node_id: NodeId) -> Option<Arc<Node>> {
        if let Some(versions) = self.tree.get_readlock(node_id) {
            let node_ref = versions.find_node_ref(LOCAL_TS.with(|ts| *ts.borrow()));
            if let Some(node_ref) = node_ref {
                if let Some(arc_node) = node_ref.node_ptr.upgrade() {
                    GLOBAL_SENDER.try_send((node_id, arc_node.clone())).unwrap();
                    return Some(arc_node);
                } else {
                    assert!(versions.node_kind != NodeKind::Del);
                    let node = G_DEV
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
                    GLOBAL_SENDER.try_send((node_id, arc_node.clone())).unwrap();
                    return Some(arc_node);
                }
            }
        }
        None
    }
    pub fn allocate_node_id(&self) -> Option<NodeId> {
        let mut bitmap = self.bitmap.write();
        let last_index = bitmap.1;
        if let Some(new_node_id) = bitmap.0.first_zero_with_hint_set(last_index) {
            bitmap.1 = new_node_id;
            Some(new_node_id as u32)
        } else {
            None
        }
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
            let mut bitmap = self.bitmap.write();
            bitmap.0.set_bit(node_id as usize, false);
            bitmap.1 = node_id as usize;
            versions.node_kind = NodeKind::Del;
            None
        } else {
            let version = NodeRef::del();
            versions.history.push_back(version);
            Some(node_id)
        }
    }
}
