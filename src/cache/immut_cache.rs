use crate::object::{Object,Entry};
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
    Remove(ObjectPos),
    Clear,
    Close,
}

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

    fn work(mut self) {
        thread::spawn(move || loop {
            let backoff = Backoff::new();
            match self.receiver.try_recv() {
                Ok(op) => {
                    match op {
                        ObjectOp::Insert(obj_pos, arc_node) => {
                            self.lru_cache.insert(obj_pos, arc_node);
                        }
                        ObjectOp::Remove(obj_pos) => {
                            self.lru_cache.remove(&obj_pos);
                        }
                        ObjectOp::Clear => {
                            self.lru_cache.clear();
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

impl ImMutCache {
    pub fn insert(&self, obj_pos: ObjectPos, arc_obj: Arc<Object>) {
        if !arc_obj.is::<Entry>() {
            self.sender
                .try_send(ObjectOp::Insert(obj_pos, arc_obj))
                .expect("send error");
        }
    }
    pub fn remove(&self, obj_pos: ObjectPos) {
        self.sender
            .try_send(ObjectOp::Remove(obj_pos))
            .expect("send error");
    }
    pub fn clear(&self) {
        self.sender.try_send(ObjectOp::Clear).expect("send error");
    }
    pub fn close(&self) {
        self.sender.try_send(ObjectOp::Close).expect("send error");
    }
}
