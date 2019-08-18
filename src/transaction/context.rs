// use super::TimeStamp;
// use crate::cache::{IndexCache, MutCache};
// use crate::meta::{ObjectAllocater, ObjectTable};
// // use crate::storage::{};
// use std::sync::Arc;

// pub struct Context<C: IndexCache> {
//     pub ts: TimeStamp,
//     pub obj_table: Arc<ObjectTable>,
//     pub cache: C,
//     // pub dev: Arc<BlockDev<D>>,
// }

// impl<C: IndexCache> Clone for Context<C> {
//     fn clone(&self) -> Self {
//         Self {
//             ts: self.ts,
//             obj_table: self.obj_table.clone(),
//             cache: self.cache.clone(),
//         }
//     }
// }

// pub struct MutContext<C: MutCache> {
//     // pub super_block: SuperBlock,
//     pub dirty_cache: C,
//     pub obj_allocater: ObjectAllocater,
//     // pub block_allocater: SegementInfoTable,
// }
