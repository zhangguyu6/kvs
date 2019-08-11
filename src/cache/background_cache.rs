use super::IndexCache;
use crate::object::{Object, ObjectId};
use crate::transaction::TimeStamp;

use crossbeam::{
    channel::{unbounded, Receiver, Sender, TryRecvError},
    utils::Backoff,
};
use lru_cache::LruCache;

use std::sync::Arc;
use std::thread;

enum ObjectOp {
    Insert(ObjectId, TimeStamp, Arc<Object>),
    Remove(ObjectId, TimeStamp),
    Clear,
    Close,
}

pub struct BackgroundIndexCache {
    sender: Sender<ObjectOp>,
}

pub struct BackgroundIndexCacheInner {
    lru_cache: LruCache<(ObjectId, TimeStamp), Arc<Object>>,
    receiver: Receiver<ObjectOp>,
}

impl BackgroundIndexCacheInner {
    pub fn new(cap: usize) -> BackgroundIndexCache {
        let lru_cache = LruCache::new(cap);
        let (sender, receiver) = unbounded();
        let cache = BackgroundIndexCacheInner {
            lru_cache: lru_cache,
            receiver: receiver,
        };
        let handler = BackgroundIndexCache { sender };
        cache.work();
        handler
    }
    fn work(mut self) {
        thread::spawn(move || loop {
            let backoff = Backoff::new();
            match self.receiver.try_recv() {
                Ok(op) => {
                    match op {
                        ObjectOp::Insert(node_id, ts, arc_node) => {
                            self.lru_cache.insert((node_id, ts), arc_node);
                        }
                        ObjectOp::Remove(node_id, ts) => {
                            self.lru_cache.remove(&(node_id, ts));
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

impl IndexCache for BackgroundIndexCache {
    fn insert(&self, oid: ObjectId, ts: TimeStamp, arc_node: Arc<Object>) {
        self.sender
            .try_send(ObjectOp::Insert(oid, ts, arc_node))
            .expect("send error");
    }
    fn get(&self, oid: ObjectId, ts: TimeStamp) -> Option<Arc<Object>> {
        None
    }
    fn remove(&self, oid: ObjectId, ts: TimeStamp) {
        self.sender
            .try_send(ObjectOp::Remove(oid, ts))
            .expect("send error");
    }
    fn clear(&self) {
        self.sender.try_send(ObjectOp::Clear).expect("send error");
    }
    fn close(&self) {
        self.sender.try_send(ObjectOp::Close).expect("send error");
    }
}
