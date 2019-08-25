use crate::cache::ImMutCache;
use crate::error::TdbError;
use crate::meta::{CheckPoint, ObjectTable, OBJECT_TABLE_ENTRY_PRE_PAGE};
use crate::object::{ObjectId, ObjectRef, UNUSED_OID};
use crate::storage::Dev;
use crate::transaction::{ImMutContext, Iter, MutContext, TimeStamp};
use log::{debug, info};
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::borrow::Borrow;
use std::ops::Range;
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

impl DataBaseReader {
    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<Vec<u8>>, TdbError> {
        self.0.get(key)
    }

    pub fn range<'a, K: Borrow<[u8]>>(
        &'a mut self,
        range: Range<&'a K>,
    ) -> Result<Option<Iter<'a, K>>, TdbError> {
        self.0.range(range)
    }
}

pub struct DataBaseWriter<'a>(MutexGuard<'a, MutContext>, &'a RwLock<Arc<Context>>);

impl<'a> DataBaseWriter<'a> {
    pub fn insert<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<(), TdbError> {
        self.0.insert(key, val)
    }

    pub fn remove<K: Borrow<[u8]>>(
        &mut self,
        key: &K,
    ) -> Result<Option<(Vec<u8>, ObjectId)>, TdbError> {
        self.0.remove(key)
    }

    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<Vec<u8>>, TdbError> {
        Ok(self.0.get(key)?.map(|entry| entry.val.clone()))
    }

    pub fn commit(&mut self) -> Result<(), TdbError> {
        let arc_ctx = self.0.commit()?;
        *self.1.write() = arc_ctx;
        Ok(())
    }
}

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
        info!("open database at {:?}", dir_path.as_ref());
        let dev = Dev::open(dir_path)?;

        let mut meta_log_reader = dev.get_meta_log_reader()?;
        let mut checkpoints = meta_log_reader.read_cps()?;
        if checkpoints.is_empty() {
            debug!("checkpoint is empty, create empty database");
            let (mut_ctx, obj_table, immut_cache) = MutContext::new_empty(dev.clone())?;
            Ok(Self {
                dev,
                immut_cache,
                obj_table,
                global_ctx: RwLock::new(Arc::new(Context::default())),
                mut_ctx: Mutex::new(mut_ctx),
            })
        } else {
            debug!("open prev database");
            let (changes, dirty_pages) = CheckPoint::merge(&checkpoints);
            let cp = checkpoints.pop().unwrap();
            debug!("get meta table reader");
            let mut meta_table_reader = dev.get_meta_table_reader()?;
            let (obj_table, mut obj_allocater) = meta_table_reader.read_table(&cp)?;
            debug!("Get obj table and obj allocater");
            if let Some((oid, _)) = changes.last() {
                let last_pid = oid / OBJECT_TABLE_ENTRY_PRE_PAGE as u32;
                obj_table.extend((last_pid + 1) as usize * OBJECT_TABLE_ENTRY_PRE_PAGE);
                obj_allocater.extend((last_pid + 1) as usize * OBJECT_TABLE_ENTRY_PRE_PAGE);
            }
            debug!("obj_allocater is {:?}", obj_allocater);

            for (oid, obj_pos) in changes.iter() {
                let obj_ref = ObjectRef::on_disk(*obj_pos, 0);
                obj_table.insert(*oid, obj_ref, 0);
                if obj_pos.is_empty() {
                    obj_allocater.set_bit(*oid as usize, false);
                } else {
                    obj_allocater.set_bit(*oid as usize, true);
                }
            }
            debug!("changes {:?}", changes);
            debug!("obj_allocater is {:?}", obj_allocater);
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

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use std::env;
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    // #[test]
    // fn test_dev() {
    //     assert!(Dev::open(env::current_dir().unwrap()).is_ok());
    // }
    #[test]
    fn test_write() {
        init();
        let database = DataBase::open(env::current_dir().unwrap()).unwrap();
        let mut writer = database.get_writer();
        // assert!(writer.insert(vec![1, 2, 5], vec![1, 2, 5]).is_ok());
        // assert_eq!(writer.insert(vec![1, 2, 4], vec![1, 2, 4]),Ok(()));
        // assert!(writer.insert(vec![1, 2, 3], vec![1, 2, 3]).is_ok());
        // assert!(writer.insert(vec![1, 2, 6], vec![1, 2, 3]).is_ok());
        assert_eq!(writer.get(&vec![1, 2, 3]), Ok(Some(vec![1, 2, 3])));
        assert_eq!(writer.get(&vec![1, 2, 4]), Ok(Some(vec![1, 2, 4])));
        // assert_eq!(writer.get(&vec![1, 2, 5]), Ok(Some(vec![1, 2, 5])));
        // assert_eq!(writer.commit(), Ok(()));
    }

}
