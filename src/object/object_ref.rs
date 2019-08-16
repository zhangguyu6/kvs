use super::{Object, ObjectId, ObjectRef, Versions};
use crate::error::TdbError;
use crate::storage::{BlockDev, RawBlockDev};
use crate::transaction::TimeStamp;
use crate::utils::RadixTree;
use std::sync::Arc;

pub struct ObjectTable {
    tree: RadixTree<Versions>,
}

impl ObjectTable {
    pub fn with_capacity(cap: usize) -> Self {
        ObjectTable {
            tree: RadixTree::with_capacity(cap).unwrap(),
        }
    }
    pub fn get<Dev: RawBlockDev + Unpin>(
        &self,
        oid: ObjectId,
        ts: TimeStamp,
        dev: &BlockDev<Dev>,
    ) -> Option<Arc<Object>> {
        if let Some(read_versions) = self.tree.get_readlock(oid) {
            if let Some(obj_ref) = read_versions.find_obj_ref(ts) {
                if let Some(arc_node) = obj_ref.obj_ref.upgrade() {
                    return Some(arc_node);
                } else {
                    let pos = obj_ref.obj_pos.clone();
                    let tag = obj_ref.obj_info.tag.clone();
                    // drop because read from disk may waste many time
                    drop(read_versions);
                    let node = dev.sync_read_node(&pos, &tag).unwrap();
                    let mut write_versions = self.tree.get_writelock(oid).unwrap();
                    let mut arc_node = Arc::new(node);
                    let obj_mut = write_versions.find_obj_mut(ts).unwrap();
                    if obj_mut.obj_ref.strong_count() == 0 {
                        obj_mut.obj_ref = Arc::downgrade(&arc_node);
                    } else {
                        arc_node = obj_mut.obj_ref.upgrade().unwrap();
                    }
                    return Some(arc_node);
                }
            }
        }
        None
    }

    // Return Ok if no need to gc, Err(oid) for next gc
    pub fn insert(
        &self,
        oid: ObjectId,
        version: ObjectRef,
        min_ts: TimeStamp,
    ) -> Result<(), ObjectId> {
        let mut versions = self.tree.get_writelock(oid).unwrap();
        if !versions.history.is_empty() {
            versions.try_clear(min_ts);
        }
        versions.add(version);
        if versions.history.len() == 1 {
            Ok(())
        } else {
            Err(oid)
        }
    }

    // Remove node from radixtree
    // if versions is empty ,free and return None,
    // else return None and try remove next time
    pub fn remove(&self, oid: ObjectId, ts: TimeStamp, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.tree.get_writelock(oid).unwrap();
        versions.obsolete_newest(min_ts);
        versions.try_clear(min_ts);
        if versions.history.len() == 0 {
            Ok(())
        } else {
            Err(oid)
        }
    }

    // Try to clean Object after insert and remove
    // Return Ok() if Object is clean  else Err(oid)
    pub fn try_gc(&self, oid: ObjectId, min_ts: TimeStamp) -> Result<(), ObjectId> {
        let mut versions = self.tree.get_writelock(oid).unwrap();
        versions.try_clear(min_ts);
        if versions.history.len() == 0 {
            Ok(())
        } else {
            Err(oid)
        }
    }

    pub fn extend(&self, extend: usize) -> Result<usize, TdbError> {
        self.tree.extend(extend)
    }
}
