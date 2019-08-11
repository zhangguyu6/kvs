use super::{Object, ObjectId, ObjectRef, ObjectTag, Versions};
use crate::storage::{BlockDev, RawBlockDev};
use crate::transaction::{TimeStamp, GLOBAL_MIN_TS, MAX_TS};
use crate::utils::RadixTree;
use parking_lot::RwLockUpgradableReadGuard;
use std::sync::{atomic::Ordering, Arc};

pub struct ObjectTable {
    tree: RadixTree<Versions>,
}

impl ObjectTable {
    pub fn with_capacity(cap: usize) -> Self {
        ObjectTable {
            tree: RadixTree::default(),
        }
    }
    pub fn get<Dev: RawBlockDev + Unpin>(
        &self,
        oid: ObjectId,
        ts: TimeStamp,
        dev: &BlockDev<Dev>,
    ) -> Option<Arc<Object>> {
        if let Some(read_versions) = self.tree.get_readlock(oid) {
            let obj_ref = read_versions.find_obj_ref(ts);
            if let Some(node_ref) = obj_ref {
                assert!(read_versions.obj_tag.is_some());
                if let Some(arc_node) = node_ref.obj_ref.upgrade() {
                    return Some(arc_node);
                } else {
                    let node = dev
                        .sync_read_node(&node_ref.obj_pos, read_versions.obj_tag.as_ref().unwrap())
                        .unwrap();
                    let mut write_versions = RwLockUpgradableReadGuard::upgrade(read_versions);
                    let arc_node = Arc::new(node);
                    let obj_mut = write_versions.find_obj_mut(ts).unwrap();
                    if obj_mut.obj_ref.strong_count() == 0 {
                        obj_mut.obj_ref = Arc::downgrade(&arc_node);
                    }
                    return Some(arc_node);
                }
            }
        }
        None
    }

    // Insert new allocated obj to table
    pub fn add_new(&self, oid: ObjectId, version: ObjectRef, obj_tag: ObjectTag) {
        let mut new_versions = self.tree.get_or_touchwritelock(oid as u32);
        assert!(new_versions.history.is_empty());
        assert!(new_versions.obj_tag.is_none());
        new_versions.add(version, obj_tag);
    }

    // Return Ok if no need to gc, Err(oid) for next gc
    pub fn append(
        &self,
        oid: ObjectId,
        version: ObjectRef,
        obj_tag: ObjectTag,
    ) -> Result<(), ObjectId> {
        let mut versions = self.tree.get_writelock(oid).unwrap();
        let min_ts = GLOBAL_MIN_TS.load(Ordering::SeqCst);
        versions.try_clear(min_ts);
        versions.add(version, obj_tag);
        if versions.history.len() == 1 {
            Ok(())
        } else {
            Err(oid)
        }
    }

    // Remove node from radixtree
    // if versions is empty ,free and return None,
    // else return None and try remove next time
    pub fn remove(&self, oid: ObjectId, ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.tree.get_writelock(oid).unwrap();
        versions.remove(ts);
        let min_ts = GLOBAL_MIN_TS.load(Ordering::SeqCst);
        versions.try_clear(min_ts);
        if versions.history.len() == 0 {
            versions.history.shrink_to_fit();
            versions.obj_tag = None;
            Ok(())
        } else {
            Err(oid)
        }
    }

    // Try to clean Object after insert and remove
    // Return Ok if Object is clean or update else Err(objectid)
    pub fn try_gc(&self, oid: ObjectId, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.tree.get_writelock(oid).unwrap();
        versions.try_clear(min_ts);
        if versions.history.len() == 0 {
            versions.history.shrink_to_fit();
            versions.obj_tag = None;
            Ok(())
        } else {
            if let Some(obj_ref) = versions.history.front_mut() {
                // obj has be update
                if obj_ref.end_ts == MAX_TS {
                    return Ok(());
                }
            }
            Err(oid)
        }
    }
}
