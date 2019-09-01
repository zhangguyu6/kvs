use crate::cache::ImMutCache;
use crate::error::TdbError;
use crate::meta::{CheckPoint, InnerTable};
use crate::object::{Key, ObjectId, Val, UNUSED_OID};
use crate::storage::Dev;
use crate::transaction::{ImMutContext, Iter, MutContext, TimeStamp};
use log::{debug, info};
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::borrow::Borrow;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
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

pub struct KVReader(ImMutContext, Arc<Context>);

impl KVReader {
    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<Key>, TdbError> {
        self.0.get(key)
    }

    pub fn get_min(&mut self) -> Result<Option<(Key, Val)>, TdbError> {
        self.0.get_min()
    }

    pub fn get_max(&mut self) -> Result<Option<(Key, Val)>, TdbError> {
        self.0.get_max()
    }

    pub fn range<'a, K: Borrow<[u8]>>(
        &'a mut self,
        range: Range<&'a K>,
    ) -> Result<Option<Iter<'a, K>>, TdbError> {
        self.0.range(range)
    }
}

pub struct KVWriter<'a>(MutexGuard<'a, MutContext>, &'a RwLock<Arc<Context>>);

impl<'a> KVWriter<'a> {
    pub fn insert<K: Into<Key>, V: Into<Val>>(&mut self, key: K, val: V) -> Result<(), TdbError> {
        self.0.insert(key, val)
    }

    pub fn remove<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<(Key, Val)>, TdbError> {
        self.0.remove(key)
    }

    pub fn get<K: Borrow<[u8]>>(&mut self, key: &K) -> Result<Option<Val>, TdbError> {
        Ok(self.0.get_entry(key)?.map(|entry| entry.val.clone()))
    }

    pub fn commit(mut self) -> Result<(), TdbError> {
        let arc_ctx = self.0.commit()?;
        *self.1.write() = arc_ctx;
        Ok(())
    }
}

pub struct KVStore {
    dev: Dev,
    immut_cache: ImMutCache,
    table: Arc<InnerTable>,
    global_ctx: RwLock<Arc<Context>>,
    mut_ctx: Mutex<MutContext>,
}

impl Drop for KVStore {
    fn drop(&mut self) {
        self.immut_cache.close();
    }
}
impl KVStore {
    pub fn get_reader(&self) -> Result<KVReader, TdbError> {
        let ctx = self.global_ctx.read().clone();
        let table = self.table.clone();
        let data_log_reader = self.dev.get_data_reader()?;
        let cache = self.immut_cache.clone();
        let immut_ctx = ImMutContext::new(ctx.root_oid, ctx.ts, table, data_log_reader, cache);
        Ok(KVReader(immut_ctx, ctx))
    }
    pub fn get_writer(&self) -> KVWriter {
        let mut mut_ctx = self.mut_ctx.lock();
        mut_ctx.increase_ts();
        KVWriter(mut_ctx, &self.global_ctx)
    }
    pub fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        info!("open database at {:?}", dir_path.as_ref());
        let dev = Dev::open(dir_path)?;

        let mut meta_log_reader = dev.get_meta_reader()?;
        let checkpoints = meta_log_reader.read_cps()?;
        if checkpoints.is_empty() {
            debug!("checkpoint is empty, create empty database");
            let (mut_ctx, table, immut_cache) = MutContext::new_empty(dev.clone())?;
            Ok(Self {
                dev,
                immut_cache,
                table,
                global_ctx: RwLock::new(Arc::new(Context::default())),
                mut_ctx: Mutex::new(mut_ctx),
            })
        } else {
            debug!("find prev checkpoint, open prev database");
            let cp = CheckPoint::merge(checkpoints);
            let (mut_ctx, table, immut_cache) = MutContext::new(dev.clone(), cp)?;
            Ok(Self {
                dev,
                immut_cache,
                table,
                global_ctx: RwLock::new(Arc::new(Context::default())),
                mut_ctx: Mutex::new(mut_ctx),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    #[test]
    fn test_kv_base() {
        init();
        let dir = tempdir().unwrap();
        let kv = KVStore::open(dir.path()).unwrap();

        let mut writer = kv.get_writer();
        assert_eq!(writer.insert(vec![1, 2, 3], vec![3, 2, 1]), Ok(()));
        assert_eq!(writer.get(&vec![1, 2, 3]), Ok(Some(vec![3, 2, 1])));
        assert_eq!(writer.commit(), Ok(()));

        let mut reader = kv.get_reader().unwrap();
        assert_eq!(reader.get(&vec![1, 2, 3]), Ok(Some(vec![3, 2, 1])));
    }

    #[test]
    fn test_kv_many() {
        init();
        let dir = tempdir().unwrap();
        let kv = KVStore::open(dir.path()).unwrap();

        let mut writer = kv.get_writer();
        for i in 0..=255 {
            assert_eq!(writer.insert(vec![i, 1, 1], vec![i, 1, 1]), Ok(()));
        }
        for i in 0..=255 {
            assert_eq!(writer.insert(vec![i, 2, 2], vec![i, 2, 2]), Ok(()));
        }
        assert_eq!(writer.commit(), Ok(()));

        let mut reader = kv.get_reader().unwrap();
        for i in 0..=255 {
            assert_eq!(reader.get(&vec![i, 1, 1]), Ok(Some(vec![i, 1, 1])));
        }
        for i in 0..=255 {
            assert_eq!(reader.get(&vec![i, 2, 2]), Ok(Some(vec![i, 2, 2])));
        }
        assert_eq!(reader.get_min(), Ok(Some((vec![0, 1, 1], vec![0, 1, 1]))));
        assert_eq!(
            reader.get_max(),
            Ok(Some((vec![255, 2, 2], vec![255, 2, 2])))
        );

        let mut writer = kv.get_writer();
        assert_eq!(
            writer.remove(&vec![255, 2, 2]),
            Ok(Some((vec![255, 2, 2], vec![255, 2, 2])))
        );
        assert_eq!(writer.get(&vec![255, 2, 2]), Ok(None));
        assert_eq!(writer.commit(), Ok(()));
        let mut reader1 = kv.get_reader().unwrap();
        assert_eq!(reader.get(&vec![255, 2, 2]), Ok(Some(vec![255, 2, 2])));
        assert_eq!(reader1.get(&vec![255, 2, 2]), Ok(None));
        // close and re-open
        drop(kv);
        let kv = KVStore::open(dir.path()).unwrap();
        let mut reader0 = kv.get_reader().unwrap();
        assert_eq!(reader0.get(&vec![255, 2, 2]), Ok(None));
    }

}
