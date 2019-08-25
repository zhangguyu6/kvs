use crate::cache::ImMutCache;
use crate::error::TdbError;
use crate::meta::{
    CheckPoint, ObjectAllocater, ObjectTable, ObjectTablePage, OBJECT_TABLE_ENTRY_PRE_PAGE,
};
use crate::object::{MutObject, ObjectId, ObjectRef, UNUSED_OID};
use crate::storage::{
    DataLogFileReader, DataLogFilwWriter, Deserialize, Dev, MetaLogFileWriter, MetaTableFileWriter,
    ObjectPos,
};
use crate::transaction::{ImMutContext, MutContext, TimeStamp};

use parking_lot::{Mutex, MutexGuard, RwLock};
use std::path::Path;
use std::sync::Arc;

pub struct Context {
    pub ts: TimeStamp,
    pub root_oid: ObjectId,
}
impl Default for Context {
    fn default() -> Self {
        Self {
            ts: 0,
            root_oid: UNUSED_OID,
        }
    }
}
pub struct DataBase {
    pub dev: Dev,
    pub immut_cache: ImMutCache,
    pub obj_table: Arc<ObjectTable>,
    pub global_ctx: RwLock<Arc<Context>>,
    pub mut_ctx: Mutex<MutContext>,
}

impl Drop for DataBase {
    fn drop(&mut self) {
        self.immut_cache.close();
    }
}

pub struct DataBaseReader(ImMutContext, Arc<Context>);

pub struct DataBaseWriter<'a>(MutexGuard<'a, MutContext>, &'a RwLock<Arc<Context>>);

impl DataBase {
    pub fn get_reader(&self) -> Result<DataBaseReader, TdbError> {
        let ctx = self.global_ctx.read().clone();
        let obj_table = self.obj_table.clone();
        let data_log_reader = self.dev.get_data_log_reader()?;
        let cache = self.immut_cache.clone();
        let immut_ctx = ImMutContext::new(ctx.root_oid, ctx.ts, obj_table, data_log_reader, cache);
        Ok(DataBaseReader(immut_ctx, ctx))
    }
    pub fn get_writer(&self) -> DataBaseWriter {
        let mut mut_ctx = self.mut_ctx.lock();
        mut_ctx.increase_ts();
        DataBaseWriter(mut_ctx, &self.global_ctx)
    }
    pub fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        let mut dev = Dev::open(dir_path)?;
        let mut meta_log_reader = dev.get_meta_log_reader()?;
        let mut checkpoints = meta_log_reader.read_cps()?;
        if checkpoints.is_empty() {
            let (mut_ctx, obj_table, immut_cache) = MutContext::new_empty(dev.clone())?;
            Ok(Self {
                dev,
                immut_cache,
                obj_table,
                global_ctx: RwLock::new(Arc::new(Context::default())),
                mut_ctx: Mutex::new(mut_ctx),
            })
        } else {
            let (changes, dirty_pages) = CheckPoint::merge(&checkpoints);
            let cp = checkpoints.pop().unwrap();
            let mut meta_table_reader = dev.get_meta_table_reader()?;
            let (obj_table, mut obj_allocater) = meta_table_reader.read_table(&cp)?;
            if let Some((oid, obj_pos)) = changes.last() {
                let last_pid = oid / OBJECT_TABLE_ENTRY_PRE_PAGE as u32;
                obj_table.extend(last_pid as usize * OBJECT_TABLE_ENTRY_PRE_PAGE);
                obj_allocater.extend(last_pid as usize * OBJECT_TABLE_ENTRY_PRE_PAGE);
            }
            for (oid, obj_pos) in changes.iter() {
                let obj_ref = ObjectRef::on_disk(*obj_pos, 0);
                obj_table.insert(*oid, obj_ref, 0);
                if obj_pos.is_empty() {
                    obj_allocater.set_bit(*oid as usize, false);
                } else {
                    obj_allocater.set_bit(*oid as usize, true);
                }
            }
            let (mut_ctx, obj_table, immut_cache) =
                MutContext::new(dev.clone(), &cp, obj_table, obj_allocater, dirty_pages)?;
            Ok(Self {
                dev,
                immut_cache,
                obj_table,
                global_ctx: RwLock::new(Arc::new(Context::default())),
                mut_ctx: Mutex::new(mut_ctx),
            })
        }
    }
}
//         dev.meta_log_file.seek(SeekFrom::Start(0))?;
//         let cps = CheckPoint::check(&mut dev.meta_log_file);
//         let mut cp = CheckPoint::default();
//         if let Some(last_cp) = cps.last() {
//             cp = last_cp.clone();
//         }
//         let obj_table = ObjectTable::new(0);
//         // about 1<<30
//         let mut obj_allocater = ObjectAllocater::new(0, 0);
//         let mut table_reader = BufReader::new(dev.meta_table_file.try_clone()?);
//         table_reader.seek(SeekFrom::Start(0))?;
//         for i in 0..cp.obj_tablepage_nums as usize {
//             let page = ObjectTablePage::deserialize(&mut table_reader)?;
//             assert_eq!(i as u32, page.get_page_id());
//             for j in 0..page.2.len() {
//                 if page.2[j].is_some() {
//                     obj_allocater
//                         .bitmap
//                         .set_bit(i * OBJECT_TABLE_ENTRY_PRE_PAGE as usize + j, true);
//                 }
//             }
//             obj_table.append_page(page);
//         }
//         let table_len = obj_table.len();
//         obj_table.extend(
//             OBJECT_TABLE_ENTRY_PRE_PAGE * cp.obj_tablepage_nums as usize - table_len,
//         );
//         if !cp.obj_changes.is_empty() {
//             for (oid, mut obj_pos) in cp.obj_changes.drain(..) {
//                 let mut _obj_pos = ObjectPos::default();
//                 if obj_pos.is_some() {
//                     _obj_pos = obj_pos.unwrap();
//                 }
//                 let version = ObjectRef::on_disk(_obj_pos, 0);
//                 obj_table.insert(oid, version, 0);
//             }
//         }
//         unimplemented!()
//     }

//     pub fn get_read_ctx(&self) -> Result<ImmutContext, TdbError> {
//         unimplemented!()
//     }

//     pub fn get_write_ctx(&self) -> Result<MutContext, TdbError> {
//         unimplemented!()
//     }
// }
