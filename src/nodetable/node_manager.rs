use crate::cache::{
    DirtyNode, DirtyNodeCache, LocalDirtyNodeCache, LocalNodeCache, ReadonlyNodeCache,
};
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
    pub fn get(&self, node_id: NodeId) -> Option<Arc<Node>> {
        if let Some(node) = self.node_cache.get(node_id, self.ts) {
            Some(node)
        } else {
            if let Some(node) = self.node_table.get(node_id, self.ts) {
                // not cache entry
                if !node.get_entry().is_some() {
                    self.node_cache.insert(node_id, self.ts, node.clone());
                }
                Some(node)
            } else {
                None
            }
        }
    }
}

pub struct DirtyNodeManager<Dev, Cache = LocalDirtyNodeCache> {
    node_table: NodeAddressTable<Dev>,
    node_cache: Cache,
    node_allocater: NodeAddressAllocater,
    ts: TimeStamp,
}

impl<Dev: RawBlockDev + Unpin, Cache: DirtyNodeCache> DirtyNodeManager<Dev, Cache> {
    pub fn get_ref(&mut self, node_id: NodeId) -> Option<ArcCow<Node>> {
        // first find in cache
        if !self.node_cache.contain(&node_id) {
            if let Some(arc_node) = self.node_table.get(node_id, self.ts) {
                // not cache entry
                if arc_node.get_entry().is_some() {
                    return Some(ArcCow::from(arc_node));
                }
                self.node_cache
                    .insert(node_id, DirtyNode::from(arc_node.clone()));
            } else {
                return None;
            }
        }
        if let Some(dirtynode) = self.node_cache.get_mut(&node_id) {
            if !dirtynode.is_del() {
                return Some(dirtynode.get_ref());
            }
        }
        None
    }
    pub fn get_mut(&mut self, node_id: NodeId) -> Option<&mut Node> {
        // not find in cache, read in disk
        if !self.node_cache.contain(&node_id) {
            if let Some(arc_node) = self.node_table.get(node_id, self.ts) {
                self.node_cache
                    .insert(node_id, DirtyNode::from(arc_node.clone()));
            } else {
                return None;
            }
        }
        self.node_cache
            .get_mut_dirty(&node_id)
            .map(|node| node.get_mut())
    }
    // allocate node from bitmap
    // only insert dirty cache
    // not change radix tree
    pub fn insert_new(&mut self, node: Node) -> NodeId {
        let node_id = self
            .node_allocater
            .allocate()
            .expect("no empty free for node allocate");
        self.node_cache.insert(node_id, DirtyNode::from(node));
        node_id
    }
    // only insert dirty cache
    // not change radix tree
    pub fn insert_del(&mut self, node_id: NodeId) -> Option<DirtyNode> {
        self.node_cache.insert(node_id, DirtyNode::Del)
    }

    pub fn commit(&mut self) -> (Vec<(NodeId, DirtyNode)>, Vec<NodeId>) {
        // for (node_id,dirtynode)  in  self.node_dirties.drain() {
        //     match dirtynode {
        //         DirtyNode::New(node) => 
        //     }
        // } 
        unimplemented!()
    }
}
