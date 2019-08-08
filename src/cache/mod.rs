use crate::nodetable::NodeId;
use crate::transaction::TimeStamp;
use crate::tree::Node;
use crate::utils::ArcCow;
use crossbeam::{
    channel::{unbounded, Sender, TryRecvError},
    utils::Backoff,
};
use lru_cache::LruCache;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::thread;
thread_local!(pub static LOCAL_CACHE: RefCell<Option<LruCache<NodeId,Arc<Node>>>> = RefCell::new(None));
const MAX_LRUCACHE_SIZE: usize = 1 << 16;
const MAX_LOCAL_CACHE_SIZE: usize = 512;

pub trait ReadonlyNodeCache: Send {
    fn insert(&self, node_id: NodeId, ts: TimeStamp, arc_node: Arc<Node>);
    fn get(&self, node_id: NodeId, ts: TimeStamp) -> Option<Arc<Node>>;
    fn remove(&self, node_id: NodeId, ts: TimeStamp);
    fn clear(&self);
    fn close(&self);
}

enum NodeCacheOp {
    Insert(NodeId, TimeStamp, Arc<Node>),
    Remove(NodeId, TimeStamp),
    Clear,
    Close,
}

pub struct BackgroundNodeCache {
    sender: Sender<NodeCacheOp>,
}

impl BackgroundNodeCache {
    pub fn new(cap: usize) -> Self {
        let (sender, receiver) = unbounded();
        let mut cache = LruCache::new(cap);
        thread::spawn(move || loop {
            let backoff = Backoff::new();
            match receiver.try_recv() {
                Ok(op) => {
                    match op {
                        NodeCacheOp::Insert(node_id, ts, arc_node) => {
                            cache.insert((node_id, ts), arc_node);
                        }
                        NodeCacheOp::Remove(node_id, ts) => {
                            cache.remove(&(node_id, ts));
                        }
                        NodeCacheOp::Clear => {
                            cache.clear();
                        }
                        NodeCacheOp::Close => {
                            break;
                        }
                    }
                    backoff.reset();
                }
                Err(err) => match err {
                    TryRecvError::Empty => {
                        backoff.spin();
                    }
                    TryRecvError::Disconnected => {
                        cache.clear();
                        break;
                    }
                },
            }
        });

        BackgroundNodeCache { sender }
    }
}

impl ReadonlyNodeCache for BackgroundNodeCache {
    fn insert(&self, node_id: NodeId, ts: TimeStamp, arc_node: Arc<Node>) {
        self.sender
            .try_send(NodeCacheOp::Insert(node_id, ts, arc_node))
            .expect("send error");
    }
    fn get(&self, node_id: NodeId, ts: TimeStamp) -> Option<Arc<Node>> {
        None
    }
    fn remove(&self, node_id: NodeId, ts: TimeStamp) {
        self.sender
            .try_send(NodeCacheOp::Remove(node_id, ts))
            .expect("send error");
    }
    fn clear(&self) {
        self.sender
            .try_send(NodeCacheOp::Clear)
            .expect("send error");
    }
    fn close(&self) {
        self.sender
            .try_send(NodeCacheOp::Close)
            .expect("send error");
    }
}

pub struct LocalNodeCache {}

impl Drop for LocalNodeCache {
    fn drop(&mut self) {
        self.close();
    }
}

impl LocalNodeCache {
    pub fn new(cap: usize) -> Self {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            *cache_mut = Some(LruCache::new(cap));
        });
        Self {}
    }
}

impl ReadonlyNodeCache for LocalNodeCache {
    fn insert(&self, node_id: NodeId, _: TimeStamp, arc_node: Arc<Node>) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut.as_mut().unwrap().insert(node_id, arc_node);
        });
    }
    fn get(&self, node_id: NodeId, _: TimeStamp) -> Option<Arc<Node>> {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut
                .as_mut()
                .unwrap()
                .get_mut(&node_id)
                .map(|node_mut| node_mut.clone())
        })
    }
    fn remove(&self, node_id: NodeId, _: TimeStamp) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut.as_mut().unwrap().remove(&node_id);
        });
    }
    fn clear(&self) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut.as_mut().unwrap().clear();
        });
    }
    fn close(&self) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            *cache_mut = None;
        });
    }
}

pub trait DirtyNodeCache {
    fn insert(&mut self, node_id: NodeId, node: DirtyNode) -> Option<DirtyNode>;
    fn remove(&mut self, node_id: &NodeId) -> Option<DirtyNode>;
    fn contain(&mut self, node_id: &NodeId) -> bool;
    fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut DirtyNode>;
    fn get_mut_dirty(&mut self, node_id: &NodeId) -> Option<&mut DirtyNode>;
    fn drain(&mut self) -> Box<dyn Iterator<Item = (NodeId, DirtyNode)>>;
}

pub enum DirtyNode {
    Readonly(Arc<Node>),
    Dirty(Node),
    New(Node),
    Del,
}

impl From<Arc<Node>> for DirtyNode {
    fn from(arc_node: Arc<Node>) -> Self {
        DirtyNode::Readonly(arc_node)
    }
}

impl From<Node> for DirtyNode {
    fn from(node: Node) -> Self {
        DirtyNode::New(node)
    }
}

impl Default for DirtyNode {
    fn default() -> Self {
        Self::Del
    }
}

impl Clone for DirtyNode {
    fn clone(&self) -> Self {
        use DirtyNode::*;
        match self {
            Readonly(node) => Readonly(node.clone()),
            Dirty(node) => Dirty(node.clone()),
            New(node) => New(node.clone()),
            Del => Del,
        }
    }
}

impl DirtyNode {
    pub fn drain(self) -> Node {
        match self {
            DirtyNode::Dirty(node) => node,
            _ => unreachable!(),
        }
    }
    pub fn get_ref(&self) -> ArcCow<Node> {
        match self {
            DirtyNode::Dirty(node) => ArcCow::from(node),
            DirtyNode::New(node) => ArcCow::from(node),
            DirtyNode::Readonly(node) => ArcCow::from(node.clone()),
            _ => unreachable!(),
        }
    }
    pub fn get_mut(&mut self) -> &mut Node {
        match self {
            DirtyNode::Dirty(node) => node,
            DirtyNode::New(node) => node,
            _ => unreachable!(),
        }
    }
    pub fn to_dirty(&mut self) {
        assert!(self.is_readonly());
        let node = self.get_ref().into_owned();
        *self = Self::Dirty(node);
    }
    pub fn is_dirty(&self) -> bool {
        match self {
            DirtyNode::Dirty(_) => true,
            _ => false,
        }
    }
    pub fn is_new(&self) -> bool {
        match self {
            DirtyNode::New(_) => true,
            _ => false,
        }
    }
    pub fn is_del(&self) -> bool {
        match self {
            DirtyNode::Del => true,
            _ => false,
        }
    }
    pub fn is_readonly(&self) -> bool {
        match self {
            DirtyNode::Readonly(_) => true,
            _ => false,
        }
    }
}

pub struct LocalDirtyNodeCache {
    dirties: HashMap<NodeId, DirtyNode>,
    cache: LruCache<NodeId, DirtyNode>,
}

/// New, Dirty, Del in dirties and Readonly in cache,
/// There is no intersection between the dirties and cache
impl DirtyNodeCache for LocalDirtyNodeCache {
    // Readonly can only be inserted in cache if node isn't dirty
    fn insert(&mut self, node_id: NodeId, node: DirtyNode) -> Option<DirtyNode> {
        if node.is_readonly() {
            assert!(!self.dirties.contains_key(&node_id));
            self.cache.insert(node_id, node)
        } else {
            let old_node = self.remove(&node_id);
            self.dirties.insert(node_id, node);
            old_node
        }
    }
    fn remove(&mut self, node_id: &NodeId) -> Option<DirtyNode> {
        let old_node = self.cache.remove(&node_id);
        if old_node.is_some() {
            return old_node;
        }
        self.dirties.remove(&node_id)
    }
    fn contain(&mut self, node_id: &NodeId) -> bool {
        self.dirties.contains_key(&node_id) || self.cache.contains_key(&node_id)
    }
    fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut DirtyNode> {
        let node_mut = self.dirties.get_mut(&node_id);
        if node_mut.is_some() {
            return node_mut;
        }
        self.cache.get_mut(&node_id)
    }
    fn get_mut_dirty(&mut self, node_id: &NodeId) -> Option<&mut DirtyNode> {
        if let Some(mut node) = self.cache.remove(node_id) {
            node.to_dirty();
            self.dirties.insert(*node_id, node);
        }
        self.get_mut(node_id)
    }
    fn drain(&mut self) -> Box<dyn Iterator<Item = (NodeId, DirtyNode)>> {
        self.cache.clear();
        Box::new(mem::replace(&mut self.dirties, HashMap::with_capacity(0)).into_iter())
    }
}
 