use super::BlockId;
use super::{ObjectPos, BLOCK_SIZE};
use crate::{
    error::TdbError,
    object::{Object, ObjectTag},
};
use std::path::Path;
use tokio::fs::File;



pub struct Dev {
    meta_file: File,
    // segement_files: Vec<(PathBuf,File)>,
}
