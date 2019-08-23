use super::ObjectPos;
use crate::meta::{CheckPoint, ObjectTablePage, OBJECT_TABLE_PAGE_SIZE,PageId};
use crate::storage::{Deserialize, Serialize,StaticSized};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectLog, ObjectTag},
};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// meta log file less than 2M 
pub const META_LOG_FILE_MAX_SIZE: usize = 1 << 21;

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
        meta_log_file_path: P,
    ) -> Result<(), TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).read(true).write(true).append(true);
        let mut temp_path = PathBuf::from(meta_log_file_path.as_ref());
        temp_path.pop();
        temp_path.push("meta_log_file_temp.db");
        let file = options_mut.open(&temp_path)?;
        self.writer = BufWriter::with_capacity(4096 * 2, file);
        self.size = 0;
        self.write_cp(cp)?;
        fs::rename(&temp_path, &meta_log_file_path)?;
        self.writer = BufWriter::with_capacity(4096 * 2, options_mut.open(&meta_log_file_path)?);
        Ok(())
    }
}


pub struct MetaLogFileReader {
    reader: BufReader<File>,
}

impl MetaLogFileReader {
    pub fn read_cps(&mut self) -> Result<Vec<CheckPoint>,TdbError> {
        let mut cps = Vec::default();
        loop {
            match CheckPoint::deserialize(&mut self.reader) {
                Ok(cp) => {
                    if cp.obj_changes.is_empty() {
                        cps.clear();
                    }
                    cps.push(cp);
                }
                Err(_) => break,
            }
        }
        Ok(cps)
    }
}
