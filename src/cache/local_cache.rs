use super::IndexCache;
use crate::object::{Object, ObjectId};
use crate::transaction::TimeStamp;
use lru_cache::LruCache;
use std::cell::RefCell;
use std::sync::Arc;

pub struct LocalIndexCache {
    lru_cache: RefCell<LruCache<ObjectId, Arc<Object>>>,
}

impl Drop for LocalIndexCache {
    fn drop(&mut self) {
        self.close();
    }
}

impl LocalIndexCache {
    pub fn new(cap: usize) -> Self {
        let lru_cache = RefCell::new(LruCache::new(cap));
        Self { lru_cache }
    }
}

impl IndexCache for LocalIndexCache {
    fn insert(&self, oid: ObjectId, _: TimeStamp, arc_obj: Arc<Object>) {
        let mut cache_mut = self.lru_cache.borrow_mut();
        cache_mut.insert(oid, arc_obj);
    }
    fn get(&self, oid: ObjectId, _: TimeStamp) -> Option<Arc<Object>> {
        let mut cache_mut = self.lru_cache.borrow_mut();
        cache_mut.get_mut(&oid).map(|arc_obj| arc_obj.clone())
    }
    fn remove(&self, oid: ObjectId, _: TimeStamp) {
        let mut cache_mut = self.lru_cache.borrow_mut();
        cache_mut.remove(&oid);
    }
    fn clear(&self) {
        let mut cache_mut = self.lru_cache.borrow_mut();
        cache_mut.clear();
    }
    fn close(&self) {}
}
