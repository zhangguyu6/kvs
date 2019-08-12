use super::IndexCache;
use crate::object::{Object, ObjectId};
use crate::transaction::TimeStamp;
use lru_cache::LruCache;
use std::cell::RefCell;
use std::sync::Arc;

thread_local!(pub static LOCAL_CACHE: RefCell<Option<LruCache<ObjectId,Arc<Object>>>> = RefCell::new(None));

const MAX_CACHE_SIZE: usize = 512;

#[derive(Clone)]
pub struct LocalNodeCache {}

impl Drop for LocalNodeCache {
    fn drop(&mut self) {
        self.close();
    }
}

impl IndexCache for LocalNodeCache {
    fn init(&self) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            if let Some(cache_mut) = &mut *cache_mut {
                cache_mut.clear();
            } else {
                *cache_mut = Some(LruCache::new(MAX_CACHE_SIZE));
            }
        });
    }
    fn insert(&self, oid: ObjectId, _: TimeStamp, arc_obj: Arc<Object>) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();

            cache_mut.as_mut().unwrap().insert(oid, arc_obj);
        });
    }
    fn get(&self, oid: ObjectId, _: TimeStamp) -> Option<Arc<Object>> {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut
                .as_mut()
                .unwrap()
                .get_mut(&oid)
                .map(|node_mut| node_mut.clone())
        })
    }
    fn remove(&self, oid: ObjectId, _: TimeStamp) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            cache_mut.as_mut().unwrap().remove(&oid);
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
