use crate::cache::{BackgroundCache, MutObjectCache};
use crate::error::TdbError;
use crate::meta::{
    CheckPoint, ObjectAllocater, ObjectTable, ObjectTablePage, OBJECT_TABLE_ENTRY_PRE_PAGE,
};
use crate::object::{MutObject, ObjectId, ObjectRef};
use crate::storage::{
    DataLogFileReader, DataLogFilwWriter, Deserialize, Dev, MetaLogFileWriter, MetaTableFileWriter,
    ObjectPos,
};
use crate::transaction::{ImmutContext, MutContext, TimeStamp};

use parking_lot::{Mutex, RwLock};
use std::collections::BTreeSet;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

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
    pub dirty_table_pages: BTreeSet<u32>,
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
        let mut dev = Dev::open(dir_path)?;
        dev.meta_log_file.seek(SeekFrom::Start(0))?;
        let cps = CheckPoint::check(&mut dev.meta_log_file);
        let mut cp = CheckPoint::default();
        if let Some(last_cp) = cps.last() {
            cp = last_cp.clone();
        }
        let obj_table = ObjectTable::new(0);
        // about 1<<30
        let mut obj_allocater = ObjectAllocater::new(0, 0);
        let mut table_reader = BufReader::new(dev.meta_table_file.try_clone()?);
        table_reader.seek(SeekFrom::Start(0))?;
        for i in 0..cp.obj_tablepage_nums as usize {
            let page = ObjectTablePage::deserialize(&mut table_reader)?;
            assert_eq!(i as u32, page.get_page_id());
            for j in 0..page.2.len() {
                if page.2[j].is_some() {
                    obj_allocater
                        .bitmap
                        .set_bit(i * OBJECT_TABLE_ENTRY_PRE_PAGE as usize + j, true);
                }
            }
            obj_table.append_page(page);
        }
        let table_len = obj_table.len();
        obj_table.extend(
            OBJECT_TABLE_ENTRY_PRE_PAGE * cp.obj_tablepage_nums as usize - table_len,
        );
        if !cp.obj_changes.is_empty() {
            for (oid, mut obj_pos) in cp.obj_changes.drain(..) {
                let mut _obj_pos = ObjectPos::default();
                if obj_pos.is_some() {
                    _obj_pos = obj_pos.unwrap();
                }
                let version = ObjectRef::on_disk(_obj_pos, 0);
                obj_table.insert(oid, version, 0);
            }
        }
        unimplemented!()
    }

    pub fn get_read_ctx(&self) -> Result<ImmutContext, TdbError> {
        unimplemented!()
    }

    pub fn get_write_ctx(&self) -> Result<MutContext, TdbError> {
        unimplemented!()
    }
}
