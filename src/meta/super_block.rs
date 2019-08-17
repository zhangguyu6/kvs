use super::SegementId;
use crate::storage::BlockId;

pub struct SuperBlock {
    // block
    pub block_size: usize,
    pub block_num: usize,
    // segement
    pub segement_size: usize,
    pub segement_num: usize,
    pub segement_in_page: usize,
    pub segement_start: BlockId,
    // object table
    pub object_num: u32,
    pub object_in_page: usize,
    pub object_table_start: BlockId,
    // for segement and object
    pub meta_log_start: BlockId,
    pub meta_log_size: usize,
    pub superblock_start: BlockId,
}
