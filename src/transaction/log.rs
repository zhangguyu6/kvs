use crate::error::TdbError;
use crate::object::{Object, ObjectId};
use crate::storage::{BlockDev, BlockId, ObjectPos, RawBlockDev, BLOCK_SIZE};
use crate::transaction::TimeStamp;
use std::collections::VecDeque;
use std::sync::Arc;

// 4M DATA BUF
const OBJECT_LOG_BUF_SIZE: usize = BLOCK_SIZE * 1024;

// 4K META BUF
const META_LOG_BUF_SIZE: usize = BLOCK_SIZE;

pub struct ObjectLog {
    ts: TimeStamp,
    // sort by objectpos
    obj_log: VecDeque<(Arc<Object>, ObjectPos)>,
}

pub struct MetaObjectLog {
    ts: TimeStamp,
    // sort by objectpos
    meta_log: VecDeque<(ObjectId, ObjectPos)>,
}

pub struct LogCommitter<'a, D: RawBlockDev + Unpin> {
    // obj
    obj_log_area: &'a mut [u8],
    block_start: BlockId,
    obj_log_used_size: usize,
    // meta
    meta_log_area: &'a mut [u8],
    first_meta_block_start: BlockId,
    meta_log_used_size: usize,
    uncommited_meta_logs: Vec<MetaObjectLog>,
    dev: &'a BlockDev<D>,
}

impl<'a, D: RawBlockDev + Unpin> LogCommitter<'a, D> {
    //
    pub fn commit(
        &mut self,
        mut obj_log: ObjectLog,
        meta_log: MetaObjectLog,
    ) -> Result<(), TdbError> {
        let current_ts = obj_log.ts;
        self.uncommited_meta_logs.push(meta_log);
        while let Some((arc_obj, obj_pos)) = obj_log.obj_log.pop_front() {
            if !self.is_sequential(&obj_pos) {
                self.apply();
            }
            if self.is_full(&arc_obj) {
                let mut buf = Vec::with_capacity(arc_obj.get_object_info().size);
                arc_obj.write(&mut buf);
                let 
                self.obj_log_area[self.obj_log_used_size..].copy_from_slice()
            }
            if self.block_start == 0 {
                self.block_start = obj_pos.block_start;
            }
            arc_obj.write(&mut self.obj_log_area[self.obj_log_used_size..])?;
            self.obj_log_used_size += arc_obj.get_object_info().size;
        }
        Ok(())
    }

    pub fn apply(&mut self) {
        // if self.obj_used_size > 0 {
        //     let buf_len = (self.current_block - self.start_block) as usize * BLOCK_SIZE;
        //     let buf = &self.obj_log_area[0..buf_len];
        // }
    }

    #[inline]
    fn is_full(&self, arc_obj: &Arc<Object>) -> bool {
        // for empty buf
        if self.obj_log_used_size == 0 {
            return false;
        }
        arc_obj.get_object_info().size + self.obj_log_used_size > self.obj_log_area.len()
    }

    #[inline]
    fn is_sequential(&self, obj_pos: &ObjectPos) -> bool {
        // for empty buf
        if self.block_start == 0 {
            return true;
        }
        let next_offset = self.block_start as usize * BLOCK_SIZE + self.obj_log_used_size;
        let obj_offset = obj_pos.block_start as usize * BLOCK_SIZE + obj_pos.offset as usize;
        next_offset == obj_offset
    }
}
