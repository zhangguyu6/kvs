use crate::cache::{BackgroundCache, MutObjectCache};
use crate::error::TdbError;
use crate::meta::{CheckPoint, ObjectAllocater, ObjectTable};
use crate::object::{MutObject, ObjectId};
use crate::storage::{DataLogFileReader, DataLogFilwWriter, Dev, MetaLogFileWriter, MetaTableFile};
use crate::transaction::{ImmutContext, MutContext, TimeStamp};

use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicU64, Arc, Weak};

pub struct Context {
    ts: TimeStamp,
    root_oid: ObjectId,
}

struct MutInner {
    pub obj_allocater: ObjectAllocater,
    pub dirty_cache: MutObjectCache,
    pub cp: CheckPoint,
    pub meta_changes: Vec<(ObjectId, MutObject)>,
    pub gc_ctx: Vec<(Weak<Context>, Vec<ObjectId>)>,
    pub data_file_reader: DataLogFileReader,
    pub data_file_writer: DataLogFilwWriter,
    pub meta_log_file: MetaLogFileWriter,
    pub meta_table_file: MetaLogFileWriter,
}

pub struct DataBase {
    // immutable
    obj_table: Arc<ObjectTable>,
    dev: Dev,
    cache: BackgroundCache,
    // mutable
    ctx: RwLock<Arc<Context>>,
    inner: Mutex<MutInner>,
}

impl DataBase {
    pub fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        unimplemented!()
    }

    pub fn get_read_ctx(&self) -> Result<ImmutContext, TdbError> {
        unimplemented!()
    }

    pub fn get_write_ctx(&self) -> Result<MutContext, TdbError> {
        unimplemented!()
    }
}
