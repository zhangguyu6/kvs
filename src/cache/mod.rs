mod background_cache;
mod local_cache;
mod mut_cache;
// use crate::nodetable::NodeId;
// use crate::tree::Node;
// use crate::utils::ArcCow;


// use std::collections::HashMap;
// use std::mem;

// thread_local!(pub static LOCAL_CACHE: RefCell<Option<LruCache<NodeId,Arc<Node>>>> = RefCell::new(None));
// const MAX_LRUCACHE_SIZE: usize = 1 << 16;
// const MAX_LOCAL_CACHE_SIZE: usize = 512;
use crate::object::{Object, ObjectId,MutObject};
use crate::transaction::TimeStamp;
use std::sync::Arc;
pub trait IndexCache: Send {
    fn insert(&self, oid: ObjectId, ts: TimeStamp, arc_node: Arc<Object>);
    fn get(&self, oid: ObjectId, ts: TimeStamp) -> Option<Arc<Object>>;
    fn remove(&self, oid: ObjectId, ts: TimeStamp);
    fn clear(&self);
    fn close(&self);
}

pub trait MutObjectCache {
    fn insert(&mut self, oid: ObjectId, obj_mut: MutObject) -> Option<MutObject>;
    fn remove(&mut self, oid: ObjectId) -> Option<MutObject>;
    fn contain(&mut self, oid: ObjectId) -> bool;
    fn get_mut(&mut self, oid: ObjectId) -> Option<&mut MutObject>;
    fn get_mut_dirty(&mut self, oid:ObjectId) -> Option<&mut MutObject>;
    fn drain(&mut self) -> Box<dyn Iterator<Item = (MutObject, MutObject)>>;
}



// impl From<Arc<Node>> for DirtyNode {
//     fn from(arc_node: Arc<Node>) -> Self {
//         DirtyNode::Readonly(arc_node)
//     }
// }

// impl From<Node> for DirtyNode {
//     fn from(node: Node) -> Self {
//         DirtyNode::New(node)
//     }
// }

// impl Default for DirtyNode {
//     fn default() -> Self {
//         Self::Del
//     }
// }

// impl Clone for DirtyNode {
//     fn clone(&self) -> Self {
//         use DirtyNode::*;
//         match self {
//             Readonly(node) => Readonly(node.clone()),
//             Dirty(node) => Dirty(node.clone()),
//             New(node) => New(node.clone()),
//             Del => Del,
//         }
//     }
// }

// impl DirtyNode {
//     pub fn drain(self) -> Node {
//         match self {
//             DirtyNode::Dirty(node) => node,
//             _ => unreachable!(),
//         }
//     }
//     pub fn get_ref(&self) -> ArcCow<Node> {
//         match self {
//             DirtyNode::Dirty(node) => ArcCow::from(node),
//             DirtyNode::New(node) => ArcCow::from(node),
//             DirtyNode::Readonly(node) => ArcCow::from(node.clone()),
//             _ => unreachable!(),
//         }
//     }
//     pub fn get_mut(&mut self) -> &mut Node {
//         match self {
//             DirtyNode::Dirty(node) => node,
//             DirtyNode::New(node) => node,
//             _ => unreachable!(),
//         }
//     }
//     pub fn to_dirty(&mut self) {
//         assert!(self.is_readonly());
//         let node = self.get_ref().into_owned();
//         *self = Self::Dirty(node);
//     }
//     pub fn is_dirty(&self) -> bool {
//         match self {
//             DirtyNode::Dirty(_) => true,
//             _ => false,
//         }
//     }
//     pub fn is_new(&self) -> bool {
//         match self {
//             DirtyNode::New(_) => true,
//             _ => false,
//         }
//     }
//     pub fn is_del(&self) -> bool {
//         match self {
//             DirtyNode::Del => true,
//             _ => false,
//         }
//     }
//     pub fn is_readonly(&self) -> bool {
//         match self {
//             DirtyNode::Readonly(_) => true,
//             _ => false,
//         }
//     }
// }

// pub struct LocalDirtyNodeCache {
//     dirties: HashMap<NodeId, DirtyNode>,
//     cache: LruCache<NodeId, DirtyNode>,
// }

// /// New, Dirty, Del in dirties and Readonly in cache,
// /// There is no intersection between the dirties and cache
// impl DirtyNodeCache for LocalDirtyNodeCache {
//     // Readonly can only be inserted in cache if node isn't dirty
//     fn insert(&mut self, node_id: NodeId, node: DirtyNode) -> Option<DirtyNode> {
//         if node.is_readonly() {
//             assert!(!self.dirties.contains_key(&node_id));
//             self.cache.insert(node_id, node)
//         } else {
//             let old_node = self.remove(&node_id);
//             self.dirties.insert(node_id, node);
//             old_node
//         }
//     }
//     fn remove(&mut self, node_id: &NodeId) -> Option<DirtyNode> {
//         let old_node = self.cache.remove(&node_id);
//         if old_node.is_some() {
//             return old_node;
//         }
//         self.dirties.remove(&node_id)
//     }
//     fn contain(&mut self, node_id: &NodeId) -> bool {
//         self.dirties.contains_key(&node_id) || self.cache.contains_key(&node_id)
//     }
//     fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut DirtyNode> {
//         let node_mut = self.dirties.get_mut(&node_id);
//         if node_mut.is_some() {
//             return node_mut;
//         }
//         self.cache.get_mut(&node_id)
//     }
//     fn get_mut_dirty(&mut self, node_id: &NodeId) -> Option<&mut DirtyNode> {
//         if let Some(mut node) = self.cache.remove(node_id) {
//             node.to_dirty();
//             self.dirties.insert(*node_id, node);
//         }
//         self.get_mut(node_id)
//     }
//     fn drain(&mut self) -> Box<dyn Iterator<Item = (NodeId, DirtyNode)>> {
//         self.cache.clear();
//         Box::new(mem::replace(&mut self.dirties, HashMap::with_capacity(0)).into_iter())
//     }
// }
