use crate::error::TdbError;
use crate::storage::{
    DataFileReader, DataFilwWriter, MetaLogFileReader, MetaFileWriter,
    TableFileReader, TableFileWriter,
};
use std::fs::{self};
use std::io::{Seek, SeekFrom};
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
    pub fn get_data_reader(&self) -> Result<DataFileReader, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let file = options_mut.open(&self.data_log_file_path)?;
        Ok(DataFileReader::new(file))
    }
    pub fn get_data_writer(&self, size: usize) -> Result<DataFilwWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.write(true).append(true);
        let mut file = options_mut.open(&self.data_log_file_path)?;
        file.seek(SeekFrom::Start(size as u64))?;
        Ok(DataFilwWriter::new(file,size))
    }
    pub fn get_meta_reader(&self) -> Result<MetaLogFileReader, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let mut file = options_mut.open(&self.meta_log_file_path)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(MetaLogFileReader::new(file))
    }
    pub fn get_meta_writer(&self, size: usize) -> Result<MetaFileWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.write(true).append(true);
        let mut file = options_mut.open(&self.meta_log_file_path)?;
        file.seek(SeekFrom::Start(size as u64))?;
        Ok(MetaFileWriter::new(file, size))
    }
    pub fn get_table_reader(&self) -> Result<TableFileReader, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.read(true);
        let mut file = options_mut.open(&self.meta_table_path)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(TableFileReader::new(file))
    }
    pub fn get_table_writer(&self) -> Result<TableFileWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.write(true).append(true);
        let file = options_mut.open(&self.meta_table_path)?;
        Ok(TableFileWriter::new(file))
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
