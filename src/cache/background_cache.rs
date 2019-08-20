use crate::object::{Object, ObjectId};
use crate::transaction::TimeStamp;
use crate::tree::Entry;

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

pub struct BackgroundCache {
    sender: Sender<ObjectOp>,
}

pub struct BackgroundCacheInner {
    lru_cache: LruCache<(ObjectId, TimeStamp), Arc<Object>>,
    receiver: Receiver<ObjectOp>,
}

impl BackgroundCacheInner {
    pub fn new(cap: usize) -> BackgroundCache {
        let lru_cache = LruCache::new(cap);
        let (sender, receiver) = unbounded();
        let cache = BackgroundCacheInner {
            lru_cache: lru_cache,
            receiver: receiver,
        };
        let handler = BackgroundCache { sender };
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

impl  BackgroundCache {
    pub fn insert(&self, oid: ObjectId, ts: TimeStamp, arc_obj: Arc<Object>) {
        if !arc_obj.is::<Entry>() {
            self.sender
                .try_send(ObjectOp::Insert(oid, ts, arc_obj))
                .expect("send error");
        }
    }
    pub fn get(&self, oid: ObjectId, ts: TimeStamp) -> Option<Arc<Object>> {
        None
    }
    pub fn remove(&self, oid: ObjectId, ts: TimeStamp) {
        self.sender
            .try_send(ObjectOp::Remove(oid, ts))
            .expect("send error");
    }
    pub fn clear(&self) {
        self.sender.try_send(ObjectOp::Clear).expect("send error");
    }
    pub fn close(&self) {
        self.sender.try_send(ObjectOp::Close).expect("send error");
    }
}
