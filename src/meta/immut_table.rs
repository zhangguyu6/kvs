use crate::cache::ImMutCache;
use crate::error::TdbError;
use crate::meta::InnerTable;
use crate::object::{Object, ObjectId};
use crate::storage::DataFileReader;
use crate::transaction::TimeStamp;
use std::sync::Arc;

pub struct ImMutTable {
    table: Arc<InnerTable>,
    data_reader: DataFileReader,
    cache: ImMutCache,
}

impl ImMutTable {
    pub fn new(table: Arc<InnerTable>, data_reader: DataFileReader, cache: ImMutCache) -> Self {
        Self {
            table,
            data_reader,
            cache,
        }
    }
    pub fn get_obj(&mut self, oid: ObjectId, ts: TimeStamp) -> Result<Arc<Object>, TdbError> {
        let (pos, obj) = self.table.get(oid, ts, &mut self.data_reader)?;
        self.cache.insert(pos, obj.clone());
        Ok(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ImMutCache;
    use crate::object::{Entry, ObjectRef};
    use crate::storage::{Dev, ObjectPos};
    use std::env;
    use std::sync::Arc;
    #[test]
    fn test_immut_table() {
        let dev = Dev::open(env::current_dir().unwrap()).unwrap();
        let data_file = dev.get_data_reader().unwrap();
        let cache = ImMutCache::default();
        let table = InnerTable::with_capacity(1);
        let arc_obj0 = Arc::new(Object::E(Entry::default()));
        let arc_obj1 = Arc::new(Object::E(Entry::default()));
        let obj_ref0 = ObjectRef::new(&arc_obj0, ObjectPos::default(), 0);
        let obj_ref1 = ObjectRef::new(&arc_obj1, ObjectPos::default(), 2);
        assert_eq!(table.insert(0, obj_ref0, 0), Ok(()));
        assert_eq!(table.insert(0, obj_ref1, 1), Err(0));
        let mut immut_table = ImMutTable::new(Arc::new(table), data_file, cache);
        assert_eq!(immut_table.get_obj(0, 0), Ok(arc_obj0.clone()));
        assert_eq!(immut_table.get_obj(0, 1), Ok(arc_obj0.clone()));
        assert_eq!(immut_table.get_obj(0, 2), Ok(arc_obj1.clone()));
    }

}
