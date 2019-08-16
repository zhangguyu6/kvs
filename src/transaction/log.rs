use crate::object::MutObject;
use crate::object::{Object, ObjectId};
use crate::storage::{BlockDev, BlockId, ObjectPos, RawBlockDev, BLOCK_SIZE};
use std::sync::Arc;

// 4M DATA BUF
const OBJECT_LOG_BUF_SIZE: usize = BLOCK_SIZE * 1024;

// 4K META BUF
const META_LOG_BUF_SIZE: usize = BLOCK_SIZE;

pub struct ObjectLog(Vec<(Arc<Object>, ObjectPos)>);

pub struct MetaObjectLog(Vec<(ObjectId, ObjectPos)>);

pub struct LogCommitter<'a, D: RawBlockDev + Unpin> {
    obj_log_area: &'a mut [u8],
    metaobj_log_area: &'a mut [u8],
    start_block: BlockId,
    last_obj_pos: ObjectPos,
    current_block: BlockId,
    obj_offset_in_block: usize,
    obj_used_size: usize,
    meta_offset_in_block: usize,
    uncommited_meta_logs: Vec<MetaObjectLog>,
    dev: &'a BlockDev<D>,
}

impl<'a, D: RawBlockDev + Unpin> LogCommitter<'a, D> {
    pub fn commit(&mut self, mut obj_log: ObjectLog, meta_log: MetaObjectLog) {
        for (arc_obj, obj_pos) in obj_log.0.iter() {
            if obj_pos.block_start != self.current_block
                && obj_pos.block_start != self.current_block + 1
            {}
            // buf is full
            // if arc_obj.get_object_info_ref().size + self.next_obj_offset >= self.obj_log_area.len()
            // {
            // }
        }
    }

    pub fn apply(&mut self) {
        if self.obj_used_size > 0 {
            let buf_len = (self.current_block - self.start_block) as usize * BLOCK_SIZE;
            let buf = &self.obj_log_area[0..buf_len];
        }
    }
}
