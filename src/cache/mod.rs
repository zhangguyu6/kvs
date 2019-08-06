use crate::nodetable::NodeId;
use crate::transaction::{TimeStamp, LOCAL_TS};
use crate::tree::Node;
use crossbeam::{
    channel::{unbounded, Sender, TryRecvError},
    utils::Backoff,
};
use lru_cache::LruCache;
use std::cell::RefCell;
use std::sync::Arc;
use std::thread;

const MAX_LRUCACHE_SIZE: usize = 1 << 16;
const MAX_LOCAL_CACHE_SIZE: usize = 512;
thread_local!(pub static LOCAL_CACHE: RefCell<Option<LruCache<NodeId,Arc<Node>>>> = RefCell::new(None));

pub trait ReadonlyNodeCache: Sync + Send {
    fn insert(&self, node_id: NodeId, arc_node: Arc<Node>);
    fn remove(&self, node_id: NodeId);
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
    fn insert(&self, node_id: NodeId, arc_node: Arc<Node>) {
        let ts = LOCAL_TS.with(|ts| *ts.borrow());
        self.sender
            .try_send(NodeCacheOp::Insert(node_id, ts, arc_node))
            .expect("send error");
    }
    fn remove(&self, node_id: NodeId) {
        let ts = LOCAL_TS.with(|ts| *ts.borrow());
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

impl LocalNodeCache {
    pub fn new(cap: usize) -> Self {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            *cache_mut = Some(LruCache::new(cap));
        });
        Self {}
    }
}

impl Drop for LocalNodeCache {
    fn drop(&mut self) {
        self.close();
    }
}

impl ReadonlyNodeCache for LocalNodeCache {
    fn insert(&self, node_id: NodeId, arc_node: Arc<Node>) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut.as_mut().unwrap().insert(node_id, arc_node);
        });
    }
    fn remove(&self, node_id: NodeId) {
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
    fn insert(&mut self, node_id: NodeId, node: Node) -> Option<Node>;
    fn remove(&mut self, node_id: NodeId) -> Option<Node>;
    fn contain(&self, node_id: NodeId) -> bool;
    fn get_ref(&self, node_id: NodeId) -> Option<&Node>;
    fn get_mut(&mut self, node_id: NodeId) -> Option<&mut Node>;
    fn clear(&mut self);
}
