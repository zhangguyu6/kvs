use crate::nodetable::NodeId;
use crate::tree::Node;
use crossbeam::{
    channel::{unbounded, Receiver, Sender, TryRecvError},
    utils::Backoff,
};
use lazy_static::lazy_static;
use lru_cache::LruCache;
use std::sync::Arc;
use std::thread;
const MAX_LRUCACHE_SIZE: usize = 1 << 16;

lazy_static! {
    pub static ref GLOBAL_SENDER: Sender<(NodeId, Arc<Node>)> = {
        let (node_cache, sender) = NodeCache::new(MAX_LRUCACHE_SIZE);
        node_cache.background();
        sender
    };
}

pub struct NodeCache {
    cache: LruCache<NodeId, Arc<Node>>,
    receiver: Receiver<(NodeId, Arc<Node>)>,
}

impl NodeCache {
    pub fn new(cap: usize) -> (Self, Sender<(NodeId, Arc<Node>)>) {
        let (sender, receiver) = unbounded();
        (
            Self {
                cache: LruCache::new(cap),
                receiver: receiver,
            },
            sender,
        )
    }

    pub fn background(mut self) {
        thread::spawn(move || loop {
            let backoff = Backoff::new();
            match self.receiver.try_recv() {
                Ok((key, val)) => {
                    self.cache.insert(key, val);
                    backoff.reset()
                }
                Err(err) => match err {
                    TryRecvError::Empty => {
                        backoff.spin();
                    }
                    TryRecvError::Disconnected => {
                        self.cache.clear();
                        break;
                    }
                },
            }
        });
    }
}
