use super::ObjectPos;
use crate::{
    error::TdbError,
    object::{Entry, Object, ObjectId, ObjectState, DATA_ALIGN},
};
use byteorder::WriteBytesExt;
use log::debug;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};

const DEFAULT_BUF_SIZE: usize = 4096 * 2;

pub struct DataFileReader {
    reader: BufReader<File>,
}

impl DataFileReader {
    pub fn new(file: File) -> Self {
        DataFileReader {
            reader: BufReader::with_capacity(DEFAULT_BUF_SIZE, file),
        }
    }

    pub fn read_obj(&mut self, obj_pos: &ObjectPos) -> Result<Object, TdbError> {
        self.reader.seek(obj_pos.clone().into())?;
        let obj_tag = obj_pos.get_tag();
        Object::read(&mut self.reader, &obj_tag)
    }
}

pub struct DataFilwWriter {
    writer: BufWriter<File>,
    size: u64,
    removed_size: u64,
}

impl DataFilwWriter {
    pub fn new(mut file: File, size: u64, removed_size: u64) -> Self {
        file.seek(SeekFrom::Start(size)).unwrap();
        DataFilwWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
            size,
            removed_size,
        }
    }
    pub fn flush(&mut self) -> Result<(), TdbError> {
        self.writer.flush()?;
        Ok(())
    }
    pub fn write_objs(
        &mut self,
        dirty_cache: &mut HashMap<ObjectId, ObjectState>,
    ) -> Result<(u64, u64), TdbError> {
        // write branch and entry, align to 4k
        for (_, mut_obj) in dirty_cache.iter_mut() {
            match mut_obj {
                ObjectState::Dirty(obj, _) | ObjectState::New(obj) => {
                    if !obj.is::<Entry>() {
                        obj.get_pos_mut().set_pos(self.size as u64);
                        self.size += obj.write(&mut self.writer)? as u64;
                        assert!(self.size % DATA_ALIGN as u64 == 0);
                    }
                }
                _ => {}
            }
        }
        // write entry, not align
        for (_, mut_obj) in dirty_cache.iter_mut() {
            match mut_obj {
                ObjectState::Dirty(obj, _) | ObjectState::New(obj) => {
                    if obj.is::<Entry>() {
                        obj.get_pos_mut().set_pos(self.size as u64);
                        debug!(
                            "write obj{:?} at {:?} {:?}",
                            obj,
                            obj.get_pos(),
                            self.writer.seek(SeekFrom::Current(0))?
                        );
                        self.size += obj.write(&mut self.writer)? as u64;
                    }
                }
                _ => {}
            }
        }
        // make commit align to 4K
        if self.size % DATA_ALIGN as u64 != 0 {
            for _ in self.size % DATA_ALIGN as u64..DATA_ALIGN as u64 {
                self.writer.write_u8(0)?;
                self.size += 1;
            }
        }
        // static removed obj size
        for (_, mut_obj) in dirty_cache.iter_mut() {
            match mut_obj {
                ObjectState::Dirty(_, arc_obj) | ObjectState::Del(arc_obj) => {
                    self.removed_size += arc_obj.get_pos().get_len() as u64;
                }
                _ => {}
            }
        }
        debug!("current write file size is {:?}", self.size);
        Ok((self.size, self.removed_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Entry, Object, ObjectId, ObjectState};
    use crate::storage::Dev;
    use std::collections::HashMap;
    use std::env;
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    #[test]
    fn test_data_file() {
        init();
        let dev = Dev::open(env::current_dir().unwrap()).unwrap();
        let mut data_reader = dev.get_data_reader().unwrap();
        let mut data_writer = dev.get_data_writer(0, 0).unwrap();
        let obj0 = ObjectState::New(Object::E(Entry::new(vec![1, 1, 1], vec![1, 1, 1])));
        let obj1 = ObjectState::New(Object::E(Entry::new(vec![2, 2, 2], vec![2, 2, 2])));
        let obj2 = ObjectState::New(Object::E(Entry::new(vec![3, 3, 3], vec![3, 3, 3])));
        let mut objs: HashMap<ObjectId, ObjectState> = HashMap::default();
        objs.insert(0, obj0);
        objs.insert(1, obj1);
        objs.insert(2, obj2);
        data_writer.write_objs(&mut objs);
        data_writer.flush();
        for (_, objstate) in objs.iter() {
            let obj_ref = objstate.get_ref().unwrap();
            let pos = obj_ref.get_pos();
            println!("{:?}", obj_ref);
            assert_eq!(data_reader.read_obj(pos), Ok(obj_ref.clone()));
        }
        println!("{:?}", data_writer.size);

        let dev = Dev::open(env::current_dir().unwrap()).unwrap();
        let mut data_reader = dev.get_data_reader().unwrap();
        for (_, objstate) in objs.iter() {
            let obj_ref = objstate.get_ref().unwrap();
            let pos = obj_ref.get_pos();
            println!("{:?}", obj_ref);
            assert_eq!(data_reader.read_obj(pos), Ok(obj_ref.clone()));
        }
    }

}
