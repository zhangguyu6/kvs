use super::ObjectPos;
use crate::meta::{CheckPoint, ObjectTablePage, OBJECT_TABLE_PAGE_SIZE};
use crate::storage::{Deserialize, Serialize};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectLog, ObjectTag},
};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const META_LOG_FILE_MAX_SIZE: usize = 1 << 21;

pub struct Dev {
    dir_path: PathBuf,
    meta_table_path: PathBuf,
    meta_log_file_path: PathBuf,
    data_log_file_path: PathBuf,
}

impl Dev {
    fn open<P: AsRef<Path>>(dir_path: P) -> Result<(Self, Vec<CheckPoint>), TdbError> {
        let dir_path = PathBuf::from(dir_path.as_ref());
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).read(true).write(true).append(true);
        let mut meta_table_path = PathBuf::from(&dir_path);
        meta_table_path.push("meta_table.db");
        let meta_table_file = options_mut.open(&meta_table_path)?;
        let mut meta_log_file_path = PathBuf::from(&dir_path);
        meta_log_file_path.push("meta_log_file.db");
        let mut meta_log_file = options_mut.open(&meta_log_file_path)?;
        let mut data_log_file_path = PathBuf::from(&dir_path);
        data_log_file_path.push("data_log_file.db");
        let mut data_log_file = options_mut.open(&data_log_file_path)?;
        meta_log_file.seek(SeekFrom::Start(0))?;
        let cps = CheckPoint::check(&mut BufReader::with_capacity(4096 * 4, meta_log_file));
        Ok((
            Dev {
                dir_path,
                meta_table_path,
                meta_log_file_path,
                data_log_file_path,
            },
            cps,
        ))
    }
}

impl Dev {
    // pub fn get_meta_table_file(&self) -> Result<MetaTableFile, TdbError> {
    //     let file = self.meta_table_file.try_clone()?;
    //     Ok(MetaTableFile { file: file })
    // }
}

pub struct MetaTableFile {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    size: usize,
}

impl MetaTableFile {
    pub fn read_page(&mut self, page_id: u32) -> Result<ObjectTablePage, TdbError> {
        self.reader.seek(SeekFrom::Start(
            page_id as u64 * OBJECT_TABLE_PAGE_SIZE as u64,
        ))?;
        ObjectTablePage::deserialize(&mut self.reader)
    }
    pub fn write_page(&mut self, page: ObjectTablePage) -> Result<(), TdbError> {
        let page_id = page.get_page_id();
        if page_id as usize * OBJECT_TABLE_PAGE_SIZE <= self.size {
            self.writer.seek(SeekFrom::Start(
                page_id as u64 * OBJECT_TABLE_PAGE_SIZE as u64,
            ))?;
        } else {
            assert_eq!(
                self.size + OBJECT_TABLE_PAGE_SIZE,
                page_id as usize * OBJECT_TABLE_PAGE_SIZE
            );
            self.writer.seek(SeekFrom::Start(self.size as u64))?;
        }
        self.size += OBJECT_TABLE_PAGE_SIZE;
        page.serialize(&mut self.writer)
    }
}

pub struct MetaLogFileWriter {
    writer: BufWriter<File>,
    size: usize,
}

impl MetaLogFileWriter {
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

    fn write_cp_rename<P: AsRef<Path>>(
        &mut self,
        cp: &CheckPoint,
        dir_path: P,
    ) -> Result<MetaLogFileWriter, TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).read(true).write(true).append(true);
        let mut path = PathBuf::from(dir_path.as_ref());
        let mut old_path = path.clone();
        old_path.push("meta_log_file.db");
        path.push("meta_log_file_temp.db");
        let file = options_mut.open(&path)?;
        let mut writer = BufWriter::with_capacity(4096 * 4, file);
        cp.serialize(&mut writer)?;
        writer.flush()?;
        fs::rename(&path, &old_path)?;
        let writer = BufWriter::with_capacity(4096 * 2, options_mut.open(&old_path)?);
        Ok(MetaLogFileWriter {
            writer: writer,
            size: cp.len(),
        })
    }
}

pub struct DataLogFileReader {
    reader: BufReader<File>,
}

impl DataLogFileReader {
    pub fn read_obj(&mut self, obj_pos: &ObjectPos) -> Result<Object, TdbError> {
        self.reader.seek(obj_pos.clone().into())?;
        let obj_tag = obj_pos.get_tag();
        Object::read(&mut self.reader, &obj_tag)
    }
}

pub struct DataLogFilwWriter {
    writer: BufWriter<File>,
}

impl DataLogFilwWriter {
    pub fn write_obj_log(&mut self, obj_log: &ObjectLog) -> Result<(), TdbError> {
        obj_log.serialize(&mut self.writer)?;
        self.writer.flush()?;
        Ok(())
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
