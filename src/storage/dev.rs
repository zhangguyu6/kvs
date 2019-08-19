use super::ObjectPos;
use crate::meta::ObjectTablePage;
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectTag},
};
use std::fs::{self, DirEntry, File as StdFile};
use std::path::{Path, PathBuf};
use tokio::fs::File;

pub struct Dev {
    meta_table_path: PathBuf,
    meta_table_file: StdFile,
    meta_log_file_path: PathBuf,
    meta_log_file: StdFile,
    data_log_file_path: PathBuf,
    data_log_file: StdFile,
}

impl Dev {
    fn open<P: AsRef<Path>>(dir_path: P) -> Result<Self, TdbError> {
        let mut options = fs::OpenOptions::new();
        let mut meta_table_path = PathBuf::from(dir_path.as_ref());
        meta_table_path.push("meta_table.db");
        let meta_table_file = options
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&meta_table_path)?;
        let mut meta_log_file_path = PathBuf::from(dir_path.as_ref());
        meta_log_file_path.push("meta_log_file.db");
        let mut meta_log_file = options
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&meta_log_file_path)?;
        let mut data_log_file_path = PathBuf::from(dir_path.as_ref());
        data_log_file_path.push("data_log_file.db");
        let mut data_log_file = options
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&data_log_file_path)?;
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
        Ok(MetaTableFile {
            file: File::from_std(file),
        })
    }
}

pub struct MetaTableFile {
    file: File,
}

impl MetaTableFile {
    pub fn sync_read_page(&mut self, obj_pos: &ObjectPos) -> Result<ObjectTablePage, TdbError> {
        unimplemented!()
    }
    pub fn sync_write_page(&mut self, page: ObjectTablePage) -> Result<(), TdbError> {
        unimplemented!()
    }
}

pub struct MetaLogFile {
    file: File,
}

pub struct DataLogFile {
    // file: File,
}

impl Default for DataLogFile {
    fn default() -> Self {
        Self {}
    }
}

impl DataLogFile {
    pub fn sync_read_obj(&mut self, obj_pos: &ObjectPos) -> Result<Object, TdbError> {
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
