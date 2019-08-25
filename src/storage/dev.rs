use crate::meta::{CheckPoint, ObjectTablePage, PageId, OBJECT_TABLE_PAGE_SIZE};
use crate::storage::{
    DataLogFileReader, DataLogFilwWriter, MetaLogFileReader, MetaLogFileWriter,MetaTableFileReader,
    MetaTableFileWriter, ObjectPos,
};
use crate::storage::{Deserialize, Serialize};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectLog, ObjectTag},
};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Dev {
    pub dir_path: PathBuf,
    pub meta_table_path: PathBuf,
    pub meta_log_file_path: PathBuf,
    pub data_log_file_path: PathBuf,
}

impl Dev {
    pub fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        let dir_path = PathBuf::from(dir_path.as_ref());
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).read(true).write(true).append(true);
        let mut meta_table_path = PathBuf::from(&dir_path);
        meta_table_path.push("meta_table.db");
        options_mut.open(&meta_table_path)?;
        let mut meta_log_file_path = PathBuf::from(&dir_path);
        meta_log_file_path.push("meta_log_file.db");
        options_mut.open(&meta_log_file_path)?;
        let mut data_log_file_path = PathBuf::from(&dir_path);
        data_log_file_path.push("data_log_file.db");
        options_mut.open(&data_log_file_path)?;
        Ok(Dev {
            dir_path,
            meta_table_path,
            meta_log_file_path,
            data_log_file_path,
        })
    }
}

impl Dev {
    pub fn get_data_log_reader(&self) -> Result<DataLogFileReader, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let file = options_mut.open(&self.data_log_file_path)?;
        Ok(DataLogFileReader::new(file))
    }
    pub fn get_data_log_writer(&self, size: usize) -> Result<DataLogFilwWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let mut file = options_mut.open(&self.data_log_file_path)?;
        file.seek(SeekFrom::Start(size as u64))?;
        Ok(DataLogFilwWriter::new(file))
    }
    pub fn get_meta_log_reader(&self) -> Result<MetaLogFileReader, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let mut file = options_mut.open(&self.meta_log_file_path)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(MetaLogFileReader::new(file))
    }
    pub fn get_meta_log_writer(&self, size: usize) -> Result<MetaLogFileWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.write(true).append(true);
        let mut file = options_mut.open(&self.meta_log_file_path)?;
        file.seek(SeekFrom::Start(size as u64))?;
        Ok(MetaLogFileWriter::new(file, size))
    }
    pub fn get_meta_table_reader(&self) -> Result<MetaTableFileReader,TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let mut file = options_mut.open(&self.meta_table_path)?;
        file.seek(SeekFrom::Start(0))?;
         Ok(MetaTableFileReader::new(file))
    }
    pub fn get_meta_table_writer(&self) -> Result<MetaTableFileWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.write(true).append(true);
        let file = options_mut.open(&self.meta_table_path)?;
        Ok(MetaTableFileWriter::new(file))
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
