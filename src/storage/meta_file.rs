use crate::error::TdbError;
use crate::meta::CheckPoint;
use crate::storage::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// meta log file less than 2M
pub const META_LOG_FILE_MAX_SIZE: usize = 1 << 21;

const DEFAULT_BUF_SIZE: usize = 4096;

pub struct MetaFileWriter {
    writer: BufWriter<File>,
    size: usize,
}

impl MetaFileWriter {
    pub fn new(mut file: File, size: usize) -> Self {
        file.seek(SeekFrom::Start(size as u64)).unwrap();
        MetaFileWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUF_SIZE, file),
            size: size,
        }
    }
    /// Write checkpoint to meta file
    /// Return true if should apply
    pub fn write_cp(&mut self, cp: &mut CheckPoint) -> Result<bool, TdbError> {
        self.size += cp.len();
        cp.meta_size = self.size as u32;
        cp.serialize(&mut self.writer)?;
        self.writer.flush()?;
        if self.size <= META_LOG_FILE_MAX_SIZE {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Write checkpoint to template meta file and rename template meta file to meta file
    /// # Notes
    /// checkpoint should be applied before write to template meta file
    pub fn write_cp_rename<P: AsRef<Path>>(
        &mut self,
        mut cp: CheckPoint,
        meta_log_file_path: P,
    ) -> Result<(), TdbError> {
        let mut options = fs::OpenOptions::new();
        let options_mut = options.create(true).write(true).append(true);
        let mut temp_path = PathBuf::from(meta_log_file_path.as_ref());
        temp_path.pop();
        temp_path.push("meta_log_file_temp.db");
        let file = options_mut.open(&temp_path)?;
        self.writer = BufWriter::with_capacity(DEFAULT_BUF_SIZE, file);
        self.size = cp.len();
        assert!(self.size < META_LOG_FILE_MAX_SIZE && cp.obj_changes.len() == 0);
        cp.meta_size = self.size as u32;
        cp.serialize(&mut self.writer)?;
        self.writer.flush()?;
        fs::rename(&temp_path, &meta_log_file_path)?;
        self.writer =
            BufWriter::with_capacity(DEFAULT_BUF_SIZE, options_mut.open(&meta_log_file_path)?);
        Ok(())
    }
}

pub struct MetaLogFileReader {
    reader: BufReader<File>,
}

impl MetaLogFileReader {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::with_capacity(DEFAULT_BUF_SIZE, file),
        }
    }
    pub fn read_cps(&mut self) -> Result<Vec<CheckPoint>, TdbError> {
        self.reader.seek(SeekFrom::Start(0))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::CheckPoint;
    use crate::storage::Dev;
    use crate::storage::ObjectPos;
    use tempfile::tempdir;
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    #[test]
    fn test_meta_file() {
        init();
        let dir = tempdir().unwrap();
        let dev = Dev::open(dir.path()).unwrap();
        let mut meta_reader = dev.get_meta_reader().unwrap();
        let mut meta_writer = dev.get_meta_writer(0).unwrap();
        let mut cp0 = CheckPoint::new(0, 0, 0, 0, 0, vec![]);
        assert!(meta_writer.write_cp(&mut cp0).is_ok());
        assert_eq!(meta_reader.read_cps(), Ok(vec![cp0.clone()]));
        let mut cp1 = CheckPoint::new(0, 0, 0, 0, 0, vec![(0, ObjectPos::default())]);
        let mut cp2 = CheckPoint::new(0, 0, 0, 0, 0, vec![(1, ObjectPos::default())]);
        assert!(meta_writer.write_cp(&mut cp1).is_ok());
        assert!(meta_writer.write_cp(&mut cp2).is_ok());
        assert_eq!(
            meta_reader.read_cps(),
            Ok(vec![cp0.clone(), cp1.clone(), cp2.clone()])
        );
    }

}
