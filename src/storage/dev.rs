use super::ObjectPos;
use crate::meta::{CheckPoint, ObjectTablePage, OBJECT_TABLE_PAGE_SIZE,PageId};
use crate::storage::{Deserialize, Serialize};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectLog, ObjectTag},
};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};


pub struct Dev {
    pub dir_path: PathBuf,
    pub meta_table_path: PathBuf,
    pub meta_table_file: File,
    pub meta_log_file_path: PathBuf,
    pub meta_log_file: File,
    pub data_log_file_path: PathBuf,
    pub data_log_file: File,
}

impl Dev {
    pub fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        let dir_path = PathBuf::from(dir_path.as_ref());
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).read(true).write(true).append(true);
        let mut meta_table_path = PathBuf::from(&dir_path);
        meta_table_path.push("meta_table.db");
        let meta_table_file = options_mut.open(&meta_table_path)?;
        let mut meta_log_file_path = PathBuf::from(&dir_path);
        meta_log_file_path.push("meta_log_file.db");
        let meta_log_file = options_mut.open(&meta_log_file_path)?;
        let mut data_log_file_path = PathBuf::from(&dir_path);
        data_log_file_path.push("data_log_file.db");
        let data_log_file = options_mut.open(&data_log_file_path)?;
        Ok(Dev {
            dir_path,
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
    // pub fn get_meta_table_file(&self) -> Result<MetaTableFile, TdbError> {
    //     let file = self.meta_table_file.try_clone()?;
    //     Ok(MetaTableFile { file: file })
    // }
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
