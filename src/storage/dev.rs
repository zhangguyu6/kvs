use super::BlockId;
use super::{ObjectPos, BLOCK_SIZE};
use crate::{
    error::TdbError,
    object::{Object, ObjectId, ObjectTag},
};
use tokio::fs::File;
use std::fs::File as StdFile;
use std::path::PathBuf;


pub struct Dev {
    meta_path:PathBuf,
    meta_file:StdFile,
    meta_log_file_path:PathBuf,
    meta_log_file:StdFile,
    data_log_file_path:PathBuf,
    data_log_file:StdFile, 
}

pub struct MetaFile {
    file: File,
    total_size: u64,
    obj_table_pos:ObjectPos,
}

pub struct MetaLogFile {
    file: File,
    total_size: u64,
}

pub struct DataLogFile {
    file: File,
    total_size: u64,
    del_size: u64,
}


impl DataLogFile {
    pub fn sync_read_obj(&self,oid:ObjectId,tag:ObjectTag) -> Object {
        unimplemented!()
    }
}