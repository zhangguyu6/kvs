use super::IndexCache;
use crate::object::{Object, ObjectId};
use crate::transaction::TimeStamp;
use crate::tree::Entry;
use lru_cache::LruCache;
use std::cell::RefCell;
use std::sync::Arc;

thread_local!(pub static LOCAL_CACHE: RefCell<Option<LruCache<(ObjectId,TimeStamp),Arc<Object>>>> = RefCell::new(None));

const MAX_CACHE_SIZE: usize = 512;

#[derive(Clone)]
pub struct LocalCache {}

impl Drop for LocalCache {
    fn drop(&mut self) {
        self.close();
    }
}

impl IndexCache for LocalCache {
    fn insert(&self, oid: ObjectId, ts: TimeStamp, arc_obj: Arc<Object>) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
            if cache_mut.is_none() {
                *cache_mut = Some(LruCache::new(MAX_CACHE_SIZE));
            }
            if arc_obj.is::<Entry>() {
 
            cache_mut.as_mut().unwrap().insert((oid,ts), arc_obj);
            }
        });
    }
    fn get(&self, oid: ObjectId, ts: TimeStamp) -> Option<Arc<Object>> {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
                        if cache_mut.is_none() {
                *cache_mut = Some(LruCache::new(MAX_CACHE_SIZE));
            }
            cache_mut
                .as_mut()
                .unwrap()
                .get_mut(&(oid,ts))
                .map(|node_mut| node_mut.clone())
        })
    }
    fn remove(&self, oid: ObjectId, ts: TimeStamp) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
                        if cache_mut.is_none() {
                *cache_mut = Some(LruCache::new(MAX_CACHE_SIZE));
            }
            cache_mut.as_mut().unwrap().remove(&(oid,ts));
        });
    }
    fn clear(&self) {
        LOCAL_CACHE.with(|cache| {
            let mut cache_mut = cache.borrow_mut();
                        if cache_mut.is_none() {
                *cache_mut = Some(LruCache::new(MAX_CACHE_SIZE));
            }
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
