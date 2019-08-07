use crate::cache::{DirtyNodeCache, LocalDirtyNodeCache, LocalNodeCache, ReadonlyNodeCache};
use crate::nodetable::{NodeAddressAllocater, NodeAddressTable, NodeId};
use crate::storage::RawBlockDev;
use crate::transaction::TimeStamp;
use crate::tree::Node;
use crate::utils::ArcCow;
use std::sync::Arc;

pub struct ReadonlyNodeManager<Dev, Cache = LocalNodeCache> {
    node_table: NodeAddressTable<Dev>,
    node_cache: Cache,
    ts: TimeStamp,
}

impl<Dev: RawBlockDev + Unpin, Cache: ReadonlyNodeCache> ReadonlyNodeManager<Dev, Cache> {
    pub fn get(&self, node_id: NodeId, ts: TimeStamp) -> Option<Arc<Node>> {
        if let Some(node) = self.node_cache.get(node_id, ts) {
            Some(node)
        } else {
            if let Some(node) = self.node_cache.get(node_id, ts) {
                self.node_cache.insert(node_id, ts, node.clone());
                Some(node)
            } else {
                None
            }
        }
    }
}

pub struct DirtyNodeManager<Dev, Dirty = LocalDirtyNodeCache,Cache = LocalNodeCache> {
    node_table: NodeAddressTable<Dev>,
    node_dirties: Dirty,
    node_cache : Cache,
    node_allocater: NodeAddressAllocater,
    ts: TimeStamp,
}

impl<Dev: RawBlockDev + Unpin, Cache: DirtyNodeCache> DirtyNodeManager<Dev, Cache> {
    pub fn get_ref(&self,node_id:NodeId,ts:TimeStamp) -> Option<ArcCow<Node>> {
        
        unimplemented!()
    }
}
