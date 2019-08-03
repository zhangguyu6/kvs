use super::{
    noderef::{NodeRef, Versions},
    NodeId,
};
use crate::transaction::{GLOBAL_MIN_TS, LOCAL_TS};
use crate::utils::{BitMap, RadixTree};
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use std::sync::atomic::Ordering;
struct NodeAddressTable {
    tree: RadixTree<Versions>,
    bitmap: RwLock<BitMap<u32>>,
    last_allocate_index: usize,
}

impl NodeAddressTable {
    pub fn get_read(&self, node_id: NodeId) -> Option<MappedRwLockReadGuard<'_, NodeRef>> {
        if let Some(versions) = self.tree.get_readlock(node_id) {
            let node_ref = RwLockReadGuard::try_map(versions, |versions| {
                versions.find_node_ref(LOCAL_TS.with(|ts| *ts.borrow()))
            });
            node_ref.ok()
        } else {
            None
        }
    }

    pub fn get_write(&self, node_id: NodeId) -> Option<MappedRwLockWriteGuard<'_, NodeRef>> {
        if let Some(versions) = self.tree.get_writelock(node_id) {
            let node_mut = RwLockWriteGuard::try_map(versions, |versions| {
                versions.find_node_mut(LOCAL_TS.with(|ts| *ts.borrow()))
            });
            node_mut.ok()
        } else {
            None
        }
    }
    pub fn add(&self, first_version: NodeRef) -> Option<NodeId> {
        if let Some(new_node_id) = self
            .bitmap
            .write()
            .first_zero_with_hint_set(self.last_allocate_index)
        {
            let mut new_node = self.tree.get_or_touchwritelock(new_node_id as u32);
            assert!(new_node.history.is_empty());
            new_node.history.push_back(first_version);
            Some(new_node_id as u32)
        } else {
            None
        }
    }
    pub fn update(&self, node_id: NodeId, version: NodeRef) -> Option<NodeId> {
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
            None
        } else {
            let version = NodeRef::del();
            versions.history.push_back(version);
            Some(node_id)
        }
    }
}
