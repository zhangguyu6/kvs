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
use std::collections::{hash_map::Drain, HashMap};
use std::sync::Arc;
use std::thread;
thread_local!(pub static LOCAL_CACHE: RefCell<Option<LruCache<NodeId,Arc<Node>>>> = RefCell::new(None));
const MAX_LRUCACHE_SIZE: usize = 1 << 16;
const MAX_LOCAL_CACHE_SIZE: usize = 512;

pub trait ReadonlyNodeCache: Sync + Send {
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
    fn insert(&mut self, node_id: NodeId, node: NodeState) -> Option<NodeState>;
    fn remove(&mut self, node_id: NodeId) -> Option<NodeState>;
    fn contain(&mut self, node_id: NodeId) -> bool;
    fn get_ref(&mut self, node_id: NodeId) -> (Option<ArcCow<Node>>, bool);
    // must insert before get_mut
    fn get_mut(&mut self, node_id: NodeId) -> &mut Node;
    fn drain(self) -> Box<dyn Iterator<Item = (NodeId, NodeState)>>;
}

pub enum NodeState {
    Dirty(Node),
    Del,
}

impl NodeState {
    pub fn get(self) -> Node {
        match self {
            NodeState::Dirty(node) => node,
            _ => unreachable!(),
        }
    }
    pub fn get_ref(&self) -> &Node {
        match self {
            NodeState::Dirty(node) => node,
            _ => unreachable!(),
        }
    }
    pub fn get_mut(&mut self) -> &mut Node {
        match self {
            NodeState::Dirty(node) => node,
            _ => unreachable!(),
        }
    }
    pub fn is_dirty(&self) -> bool {
        match self {
            NodeState::Dirty(_) => true,
            _ => false,
        }
    }
}

pub struct LocalDirtyNodeCache {
    dirties: HashMap<NodeId, NodeState>,
    cache: LruCache<NodeId, Arc<Node>>,
}

impl DirtyNodeCache for LocalDirtyNodeCache {
    fn insert(&mut self, node_id: NodeId, node: NodeState) -> Option<NodeState> {
        self.cache.remove(&node_id);
        self.dirties.insert(node_id, node)
    }
    fn remove(&mut self, node_id: NodeId) -> Option<NodeState> {
        self.cache.remove(&node_id);
        self.dirties.insert(node_id, NodeState::Del)
    }
    fn contain(&mut self, node_id: NodeId) -> bool {
        self.dirties.contains_key(&node_id) || self.cache.contains_key(&node_id)
    }
    fn get_ref(&mut self, node_id: NodeId) -> (Option<ArcCow<Node>>, bool) {
        if let Some(nodestate) = self.dirties.get(&node_id) {
            if nodestate.is_dirty() {
                (Some(ArcCow::from(nodestate.get_ref())), false)
            } else {
                (None, false)
            }
        } else {
            (
                self.cache
                    .get_mut(&node_id)
                    .map(|arc_node| ArcCow::from(arc_node.clone())),
                true,
            )
        }
    }
    fn get_mut(&mut self, node_id: NodeId) -> &mut Node {
        assert!(self.dirties.contains_key(&node_id));
        self.dirties.get_mut(&node_id).unwrap().get_mut()
    }
    fn drain(self) -> Box<dyn Iterator<Item = (NodeId, NodeState)>> {
        Box::new(self.dirties.into_iter())
    }
}
