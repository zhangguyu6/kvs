// use super::MIN_TS;
// use crate::error::TdbError;
// use crate::object::{Object, ObjectId};
// use crate::storage::{BlockDev, BlockId, ObjectPos, RawBlockDev, BLOCK_SIZE, UNUSED_BLOCK_ID};
// use crate::transaction::TimeStamp;

// use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// use std::collections::{BTreeMap, VecDeque};
// use std::mem;
// use std::sync::Arc;
// // 4M DATA BUF
// const OBJECT_LOG_BUF_SIZE: usize = BLOCK_SIZE * 1024;

// // 4K META BUF
// const META_LOG_BUF_SIZE: usize = BLOCK_SIZE;

// pub struct ObjectLog {
//     ts: TimeStamp,
//     // sort by objectpos
//     obj_log: VecDeque<(Arc<Object>, ObjectPos)>,
// }

// pub struct ObjectLogCommitter<'a, D: RawBlockDev + Unpin> {
//     // temporary memory log area
//     obj_log_area: &'a mut [u8],
//     // block off of log area in disk
//     block_start: BlockId,
//     obj_log_used_size: usize,
//     max_uncommitted_ts: TimeStamp,
//     dev: &'a BlockDev<D>,
// }

// impl<'a, D: RawBlockDev + Unpin> ObjectLogCommitter<'a, D> {
//     // Add log to in-memory log area, if area is full, flush it to disk
//     // Return max timestamp for meta commit
//     pub fn commit(&mut self, mut obj_log: ObjectLog) -> Result<TimeStamp, TdbError> {
//         let mut max_committed_ts = MIN_TS;
//         while let Some((arc_obj, obj_pos)) = obj_log.obj_log.pop_front() {
//             // buf is not init
//             if self.block_start == 0 {
//                 assert!(self.obj_log_used_size == 0);
//                 self.block_start = obj_pos.get_bid();
//                 assert!(obj_pos.get_inner_offset() == 0);
//                 arc_obj.write(&mut self.obj_log_area[self.obj_log_used_size..])?;
//                 self.obj_log_used_size += arc_obj.get_object_info().size;
//             }
//             // log split into different segement
//             else if !self.is_sequential(&obj_pos) {
//                 max_committed_ts = self.flush()?;
//                 self.block_start = obj_pos.get_bid();
//                 assert!(obj_pos.get_inner_offset() == 0);
//                 arc_obj.write(&mut self.obj_log_area[self.obj_log_used_size..])?;
//                 self.obj_log_used_size += arc_obj.get_object_info().size;
//             }
//             // buf is full, flush before write
//             else if self.is_full(&arc_obj) {
//                 let mut buf = Vec::with_capacity(arc_obj.get_object_info().size);
//                 arc_obj.write(&mut buf)?;
//                 let left_half_obj_size = self.obj_log_area.len() - self.obj_log_used_size;
//                 let right_half_obj_size = arc_obj.get_object_info().size - left_half_obj_size;
//                 self.obj_log_area[self.obj_log_used_size..]
//                     .copy_from_slice(&buf[0..left_half_obj_size]);
//                 max_committed_ts = self.flush()?;
//                 self.block_start = ((obj_pos.get_pos() as usize + left_half_obj_size as usize)
//                     / BLOCK_SIZE) as u32;
//                 self.obj_log_area[0..right_half_obj_size]
//                     .copy_from_slice(&buf[left_half_obj_size..]);
//                 self.obj_log_used_size += right_half_obj_size;
//             }
//             // normal write
//             else {
//                 arc_obj.write(&mut self.obj_log_area[self.obj_log_used_size..])?;
//                 self.obj_log_used_size += arc_obj.get_object_info().size;
//             }
//         }
//         self.max_uncommitted_ts = obj_log.ts;
//         if self.obj_log_used_size > self.obj_log_area.len() / 2 {
//             max_committed_ts = self.flush()?;
//         }
//         // transaction must aligned to BLOCK_SIZE
//         self.align();
//         Ok(max_committed_ts)
//     }

//     #[inline]
//     fn is_full(&self, arc_obj: &Arc<Object>) -> bool {
//         // empty buf
//         if self.obj_log_used_size == 0 {
//             return false;
//         }
//         arc_obj.get_object_info().size + self.obj_log_used_size > self.obj_log_area.len()
//     }

//     #[inline]
//     fn is_sequential(&self, obj_pos: &ObjectPos) -> bool {
//         // for empty buf
//         if self.block_start == 0 {
//             return true;
//         }
//         let next_offset = self.block_start as usize * BLOCK_SIZE + self.obj_log_used_size;
//         let obj_offset = obj_pos.get_pos() as usize;
//         next_offset == obj_offset
//     }

//     fn flush(&mut self) -> Result<TimeStamp, TdbError> {
//         if self.obj_log_used_size == 0 {
//             Ok(MIN_TS)
//         } else {
//             self.align();
//             self.dev.sync_write(
//                 self.block_start,
//                 &self.obj_log_area[0..self.obj_log_used_size],
//             )?;
//             self.obj_log_used_size = 0;
//             self.block_start = UNUSED_BLOCK_ID;
//             let ts = self.max_uncommitted_ts;
//             self.max_uncommitted_ts = MIN_TS;
//             Ok(ts)
//         }
//     }
//     #[inline]
//     fn align(&mut self) {
//         if self.obj_log_used_size % BLOCK_SIZE != 0 {
//             let block_len = (self.obj_log_used_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
//             let end_size = block_len * BLOCK_SIZE;
//             assert!(self.obj_log_used_size < end_size);
//             // set zero for unused block room
//             for elem in self.obj_log_area[self.obj_log_used_size..end_size].iter_mut() {
//                 *elem = 0;
//             }
//             self.obj_log_used_size = end_size;
//         }
//     }
// }

// pub struct MetaObjectLog {
//     ts: TimeStamp,
//     // sort by objectpos
//     meta_log: VecDeque<(ObjectId, ObjectPos)>,
// }

// impl MetaObjectLog {
//     pub fn get_size(&self) -> usize {
//         mem::size_of::<TimeStamp>()
//             + mem::size_of::<u32>()
//             + self.meta_log.len() * (mem::size_of::<BlockId>() + ObjectPos::get_size())
//     }
//     pub fn write(&self, mut buf: &mut [u8]) -> Result<(), TdbError> {
//         assert!(buf.len() >= self.get_size());
//         buf.write_u64::<LittleEndian>(self.ts)?;
//         buf.write_u32::<LittleEndian>(self.meta_log.len() as u32)?;
//         for (oid, obj_pos) in self.meta_log.iter() {
//             buf.write_u32::<LittleEndian>(*oid)?;
//             buf.write_u64::<LittleEndian>(obj_pos.0)?;
//         }
//         Ok(())
//     }
// }

// pub struct MetaObjectLogComitter<'a, D: RawBlockDev + Unpin> {
//     // Persistent in memory meta log
//     meta_log_area: &'a mut [u8],
//     meta_map: BTreeMap<BlockId, Vec<(ObjectId, ObjectPos)>>,
//     meta_block_start: BlockId,
//     meta_log_used_size: usize,
//     last_flush_size: usize,
//     uncommited_meta_logs: VecDeque<MetaObjectLog>,
//     dev: &'a BlockDev<D>,
// }

// impl<'a, D: RawBlockDev + Unpin> MetaObjectLogComitter<'a, D> {
//     pub fn push(&mut self, meta_log: MetaObjectLog) {
//         self.uncommited_meta_logs.push_back(meta_log);
//     }
//     pub fn commit(&mut self, ts: TimeStamp) -> Result<(), TdbError> {
//         while let Some(meta_log) = self.uncommited_meta_logs.pop_front() {
//             if meta_log.ts > ts {
//                 self.uncommited_meta_logs.push_front(meta_log);
//                 break;
//             }
//             if self.meta_log_used_size + meta_log.get_size() > self.meta_log_area.len() {
//                 self.flush_metalog()?;
//                 self.flush_table()?;
//                 self.flush_superblock()?;
//             } else {
//                 // self.meta_log_area.
//             }
//         }
//         unimplemented!()
//     }
//     pub fn flush_table(&mut self) -> Result<(), TdbError> {
//         unimplemented!()
//     }

//     pub fn flush_metalog(&mut self) -> Result<(), TdbError> {
//         unimplemented!()
//     }

//     pub fn flush_superblock(&mut self) -> Result<(), TdbError> {
//         unimplemented!()
//     }
// }
