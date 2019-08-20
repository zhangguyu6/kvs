use super::TimeStamp;
use crate::cache::{BackgroundCache, MutObjectCache};
use crate::meta::{CheckPoint, ObjectAllocater, ObjectTable};
use crate::object::ObjectId;
use crate::storage::{DataLogFile, MetaLogFile, MetaTableFile};
use std::sync::Arc;


pub struct MutContext<'a> {
    pub ts: TimeStamp,
    pub root_oid: ObjectId,
    pub obj_table: Arc<ObjectTable>,
    pub obj_allocater: &'a mut ObjectAllocater,
    pub dirty_cache: &'a mut MutObjectCache,
    pub cp: &'a mut CheckPoint,
    pub data_file: DataLogFile,
    pub meta_file: MetaLogFile,
    pub meta_table_file: MetaTableFile,
}
