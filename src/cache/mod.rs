mod background_cache;
mod local_cache;
mod mut_cache;
pub use background_cache::{BackgroundCache,BackgroundCacheInner};
pub use local_cache::LocalCache;
pub use mut_cache::MutObjectCache;

use crate::object::{MutObject, Object, ObjectId};
use crate::transaction::TimeStamp;
use std::sync::Arc;

pub trait IndexCache: Send + Clone {
    fn insert(&self, oid: ObjectId, ts: TimeStamp, arc_node: Arc<Object>);
    fn get(&self, oid: ObjectId, ts: TimeStamp) -> Option<Arc<Object>>;
    fn remove(&self, oid: ObjectId, ts: TimeStamp);
    fn clear(&self);
    fn close(&self);
}

pub trait MutCache {
    fn contain(&mut self, oid: ObjectId) -> bool;
    fn remove(&mut self, oid: ObjectId) -> Option<MutObject>;
    fn insert(&mut self, oid: ObjectId, obj_mut: MutObject) -> Option<MutObject>;
    fn get_mut(&mut self, oid: ObjectId) -> Option<&mut MutObject>;
    fn get_mut_dirty(&mut self, oid: ObjectId) -> Option<&mut MutObject>;
    fn drain(&mut self) -> Box<dyn Iterator<Item = (ObjectId, MutObject)>>;
}
