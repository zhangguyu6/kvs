use super::ObjectPos;
use crate::meta::{CheckPoint, ObjectTablePage};
use crate::storage::{Deserialize, Serialize};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectTag},
};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const META_LOG_FILE_MAX_SIZE: usize = 1 << 21;

pub struct Dev {
    meta_table_path: PathBuf,
    meta_table_file: File,
    meta_log_file_path: PathBuf,
    meta_log_file: File,
    data_log_file_path: PathBuf,
    data_log_file: File,
}

impl Dev {
    fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).read(true).write(true).append(true);
        let mut meta_table_path = PathBuf::from(dir_path.as_ref());
        meta_table_path.push("meta_table.db");
        let meta_table_file = options_mut.open(&meta_table_path)?;
        let mut meta_log_file_path = PathBuf::from(dir_path.as_ref());
        meta_log_file_path.push("meta_log_file.db");
        let mut meta_log_file = options_mut.open(&meta_log_file_path)?;
        let mut data_log_file_path = PathBuf::from(dir_path.as_ref());
        data_log_file_path.push("data_log_file.db");
        let mut data_log_file = options_mut.open(&data_log_file_path)?;
        Ok(Dev {
            meta_table_path,
            meta_table_file,
            meta_log_file_path,
            meta_log_file,
            data_log_file_path,
            data_log_file,
        })
    }
}

impl Dev {
    pub fn get_meta_table_file(&self) -> Result<MetaTableFile, TdbError> {
        let file = self.meta_table_file.try_clone()?;
        Ok(MetaTableFile { file: file })
    }
}

pub struct MetaTableFile {
    file: File,
}

impl MetaTableFile {
    pub fn read_page(&mut self, page_id: u32) -> Result<ObjectTablePage, TdbError> {
        self.file.seek(SeekFrom::Start())
        unimplemented!()
    }
    pub fn write_page(&mut self, page: ObjectTablePage) -> Result<(), TdbError> {
        unimplemented!()
    }
}

pub struct MetaLogFile {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    size: usize,
}

impl MetaLogFile {
    fn check(&mut self) -> Vec<CheckPoint> {
        let mut result = Vec::new();
        if self.size <= CheckPoint::min_len() {
            return result;
        } else if self.size > META_LOG_FILE_MAX_SIZE {
            panic!("meta log file bigger than expect");
        }
        self.reader.seek(SeekFrom::Start(0)).unwrap();
        loop {
            match CheckPoint::deserialize(&mut self.reader) {
                Ok(cp) => {
                    if cp.meta_log_total_len == 0 {
                        result.clear();
                    }
                    result.push(cp);
                }
                Err(_) => break,
            }
        }
        return result;
    }

    fn write_cp(&mut self, cp: &CheckPoint) -> Result<(), TdbError> {
        if self.size + cp.len() > META_LOG_FILE_MAX_SIZE {
            return Err(TdbError::NoSpace);
        } else {
            self.writer.seek(SeekFrom::Start(self.size as u64))?;
            cp.serialize(&mut self.writer)?;
            self.size += cp.len();
            self.writer.flush()?;
            Ok(())
        }
    }
}

pub struct DataLogFile {
    // file: File,
}

impl Clone for DataLogFile {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl Default for DataLogFile {
    fn default() -> Self {
        Self {}
    }
}

impl DataLogFile {
    pub fn read_obj(&mut self, obj_pos: &ObjectPos) -> Result<Object, TdbError> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    #[test]
    fn test_dev() {
        assert!(Dev::open(env::current_dir().unwrap()).is_ok());
    }
}
