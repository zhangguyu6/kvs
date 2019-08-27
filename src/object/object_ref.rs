use super::Object;
use crate::storage::ObjectPos;
use crate::transaction::{TimeStamp, MAX_TS};
use std::collections::VecDeque;
use std::sync::{Arc, Weak};

#[derive(Debug)]
pub struct ObjectRef {
    // don't own obj, just get ref from cache
    pub obj_ref: Weak<Object>,
    pub obj_pos: ObjectPos,
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
    pub fn on_disk(pos: ObjectPos, ts: TimeStamp) -> Self {
        Self {
            obj_ref: Weak::default(),
            obj_pos: pos,
            start_ts: ts,
            end_ts: MAX_TS,
        }
    }
}

#[derive(Debug)]
pub struct Versions {
    pub history: VecDeque<ObjectRef>,
}

impl Versions {
    pub fn new_only(obj_ref:ObjectRef) -> Self {
        let mut history = VecDeque::new();
        history.push_back(obj_ref);
        Self {
            history
        }
    }
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

    pub fn get_newest_objpos(&self) -> ObjectPos {
         if let Some(_version) = self.history.front() {
            if _version.end_ts == MAX_TS {
               return _version.obj_pos.clone();
            }
        }
        ObjectPos::default()
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
        self.history.shrink_to_fit();
    }
    pub fn add(&mut self, obj_ref: ObjectRef) {
        assert!(obj_ref.end_ts == MAX_TS, "insert obsoleted version");
        self.obsolete_newest(obj_ref.start_ts);
        self.history.push_front(obj_ref);
    }

    // Set newest version's end_ts to ts, make it obsolete
    pub fn obsolete_newest(&mut self, ts: TimeStamp) {
        if let Some(_version) = self.history.front_mut() {
            if _version.end_ts == MAX_TS {
                _version.end_ts = ts;
            }
        }
    }
    #[inline]
    pub fn is_clear(&self) -> bool {
        self.history.is_empty() || (self.history.len() == 1 && self.history.front().unwrap().end_ts == MAX_TS)
    }
}

impl Default for Versions {
    fn default() -> Self {
        Self {
            history: VecDeque::with_capacity(0),
        }
    }
}
