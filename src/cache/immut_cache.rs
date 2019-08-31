use crate::object::{Entry, Object};
use crate::storage::ObjectPos;
use crossbeam::{
    channel::{unbounded, Receiver, Sender, TryRecvError},
    utils::Backoff,
};
use lru_cache::LruCache;
use std::sync::Arc;
use std::thread;

const DEFAULT_CACHE_SIZE: usize = 4096;

enum ObjectOp {
    Insert(ObjectPos, Arc<Object>),
    Close,
}

/// Hander for send Arc<Object> to background cache thread
/// InnerTable has Weak<Object>,so background just need to own Arc<Object>
pub struct ImMutCache {
    sender: Sender<ObjectOp>,
}

impl Default for ImMutCache {
    fn default() -> Self {
        ImMutCacheInner::with_capacity(DEFAULT_CACHE_SIZE)
    }
}

impl Clone for ImMutCache {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl ImMutCache {
    /// Send Arc<Object> to cache
    pub fn insert(&self, obj_pos: ObjectPos, arc_obj: Arc<Object>) {
        if !arc_obj.is::<Entry>() {
            self.sender
                .try_send(ObjectOp::Insert(obj_pos, arc_obj))
                .expect("send error");
        }
    }
    /// Close background cache thread
    pub fn close(&self) {
        self.sender.try_send(ObjectOp::Close).expect("send error");
    }
}

pub struct ImMutCacheInner {
    lru_cache: LruCache<ObjectPos, Arc<Object>>,
    receiver: Receiver<ObjectOp>,
}

impl ImMutCacheInner {
    pub fn with_capacity(cap: usize) -> ImMutCache {
        let lru_cache = LruCache::new(cap);
        let (sender, receiver) = unbounded();
        let cache = ImMutCacheInner {
            lru_cache: lru_cache,
            receiver: receiver,
        };
        let handler = ImMutCache { sender };
        cache.work();
        handler
    }

    /// Background thread loop on received Arc<Object>
    fn work(mut self) {
        thread::spawn(move || loop {
            let backoff = Backoff::new();
            match self.receiver.try_recv() {
                Ok(op) => {
                    match op {
                        ObjectOp::Insert(obj_pos, arc_node) => {
                            self.lru_cache.insert(obj_pos, arc_node);
                        }
                        ObjectOp::Close => {
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
                        break;
                    }
                },
            }
        });
    }
}
