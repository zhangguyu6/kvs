use super::{Object, ObjectTag};
use crate::storage::ObjectPos;
use crate::transaction::{TimeStamp, MAX_TS};
use std::collections::VecDeque;
use std::sync::{Arc, Weak};

pub struct ObjectRef {
    // don't own obj, just get ref from cache
    pub obj_ref: Weak<Object>,
    pub obj_pos: ObjectPos,
    // start_ts don't represent time write to disk, but time when read from dsik/new create
    pub start_ts: TimeStamp,
    pub end_ts: TimeStamp,
}

impl ObjectRef {
    pub fn new(arc_obj: &Arc<Object>, pos: ObjectPos, ts: TimeStamp) -> Self {
        Self {
            obj_ref: Arc::downgrade(arc_obj),
            obj_pos: pos,
            start_ts: ts,
            end_ts: MAX_TS,
        }
    }
}

pub struct Versions {
    pub history: VecDeque<ObjectRef>,
    pub obj_tag: Option<ObjectTag>,
}

impl Versions {
    pub fn find_obj_ref(&self, ts: TimeStamp) -> Option<&ObjectRef> {
        for obj_ref in self.history.iter() {
            if obj_ref.start_ts <= ts && obj_ref.end_ts > ts {
                return Some(obj_ref);
            }
        }
        None
    }

    pub fn find_obj_mut(&mut self, ts: TimeStamp) -> Option<&mut ObjectRef> {
        for obj_mut in self.history.iter_mut() {
            if obj_mut.start_ts <= ts && obj_mut.end_ts > ts {
                return Some(obj_mut);
            }
        }
        None
    }
    pub fn try_clear(&mut self, min_ts: TimeStamp) {
        loop {
            if let Some(version) = self.history.back() {
                if version.end_ts <= min_ts {
                    let version = self.history.pop_back().unwrap();
                    drop(version);
                    continue;
                }
            }
            break;
        }
    }
    pub fn add(&mut self, obj_ref: ObjectRef, obj_tag: ObjectTag) {
        if self.history.is_empty() {
            assert!(self.obj_tag.is_none());
            self.obj_tag = Some(obj_tag);
            self.history.push_back(obj_ref);
        } else {
            assert_eq!(self.obj_tag, Some(obj_tag));
            let last_obj_ref = self.history.front_mut().unwrap();
            last_obj_ref.end_ts = obj_ref.start_ts;
            self.history.push_front(obj_ref);
        }
    }
    pub fn remove(&mut self,ts:TimeStamp) {
        if let Some(_version) = self.history.front_mut() {
            if _version.end_ts == MAX_TS {
                _version.end_ts = ts;
            }
        }
    }
}

impl Default for Versions {
    fn default() -> Self {
        Self {
            history: VecDeque::with_capacity(0),
            obj_tag: None,
        }
    }
}
