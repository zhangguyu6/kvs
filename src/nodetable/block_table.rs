use crate::{
    error::TdbError,
    storage::{
        block_io::{BlockDev, RawBlockDev, G_DEV},
        block_layout::{AsBlock, Block, BlockId, RawBlock},
        radixtree::RadixTree,
    },
    transaction::{TimeStamp, GLOBAL_TS, LOCAL_TS},
};
use lazy_static::lazy_static;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

lazy_static! {
    pub static ref GLOBAL_BLOCK_TABLE: BlockTable = BlockTable::new();
}

pub struct BlockTable {
    radixtree: RadixTree<BlockRef>,
}

impl BlockTable {
    fn new() -> Self {
        Self {
            radixtree: RadixTree::default(),
        }
    }

    fn get(&self, node_id: u64, ts: TimeStamp) -> Option<*mut BlockRef> {
        if let Some(mut block_ref_ptr) = self.radixtree.get(node_id) {
            if block_ref_ptr.is_null() {
                return None;
            }
            while !block_ref_ptr.is_null() {
                let block_ref = unsafe { block_ref_ptr.as_ref() }.unwrap();
                if block_ref.commit_ts <= ts {
                    return Some(block_ref_ptr);
                } else {
                    block_ref_ptr = block_ref.next_block.load(Ordering::SeqCst);
                }
            }
            None
        } else {
            None
        }
    }

    // 只有write thread能调用
    fn insert(&self, node_id: u64, block_ref: BlockRef) -> Option<*mut BlockRef> {
        let mut old_block_ptr = self.radixtree.get_or_touch(node_id);
        let new_block_ptr = Box::into_raw(Box::new(block_ref));
        unsafe { &(*new_block_ptr).next_block }.store(old_block_ptr, Ordering::SeqCst);
        let _old_block_ptr = self.radixtree.cas(node_id, old_block_ptr, new_block_ptr);
        assert_eq!(old_block_ptr, _old_block_ptr);
        if old_block_ptr.is_null() {
            None
        } else {
            Some(old_block_ptr)
        }
    }
    
    // 只有write thread能调用
    fn del(&self, node_id: u64) -> Option<*mut BlockRef> {
        let mut old_block_ptr = self.radixtree.get_or_touch(node_id);
        assert!(!old_block_ptr.is_null());
        let old_block_ref = unsafe { old_block_ptr.as_ref() }.unwrap();
        // only del if only del item in this node
        assert!(
            old_block_ref.block_id == 0
                && old_block_ref.next_block.load(Ordering::SeqCst).is_null()
        );
        let _old_block_ptr = self.radixtree.cas(node_id, old_block_ptr, ptr::null_mut());
        assert_eq!(old_block_ptr, _old_block_ptr);
        if old_block_ptr.is_null() {
            None
        } else {
            Some(old_block_ptr)
        }
    }
}

pub struct BlockRef {
    block_ptr: AtomicPtr<Block>,
    block_id: BlockId,
    logical_id: u64,
    commit_ts: TimeStamp,
    next_block: AtomicPtr<BlockRef>,
}

#[derive(Copy, Clone)]
pub struct LogicalId<B: AsBlock>(u64, PhantomData<B>);

impl<B: AsBlock> From<u64> for LogicalId<B> {
    fn from(id: u64) -> Self {
        Self(id, PhantomData)
    }
}

impl<B: AsBlock> Deref for LogicalId<B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        let local_ts = LOCAL_TS.with(|ts| *ts.borrow());
        if let Some(block_ref_ptr) = GLOBAL_BLOCK_TABLE.get(self.0, local_ts) {
            assert!(!block_ref_ptr.is_null());
            let block_ref = unsafe { block_ref_ptr.as_ref() }.unwrap();
            let block_id = block_ref.block_id;
            let mut block_ptr = block_ref.block_ptr.load(Ordering::SeqCst);
            if block_ptr.is_null() {
                let mut raw_block = RawBlock::default();
                let result = G_DEV.sync_read(block_id, &mut raw_block);
                assert!(result.is_ok());
                let block = B::deserialize(raw_block).unwrap();
                let new_block_ptr = Box::into_raw(Box::new(block.into()));
                let prev_block_ptr = block_ref.block_ptr.compare_and_swap(
                    block_ptr,
                    new_block_ptr,
                    Ordering::SeqCst,
                );
                if prev_block_ptr == block_ptr {
                    block_ptr = new_block_ptr;
                } else {
                    block_ptr = prev_block_ptr;
                }
            }
            return unsafe { block_ptr.as_ref() }.unwrap().as_ref();
        }

        panic!("deref null pointer")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_logical_id() {
        use crate::storage::block_layout::*;
        struct A {};
        impl BlockSerialize for A {
            fn serialize(&self) -> Result<RawBlock, TdbError> {
                unimplemented!()
            }
        }
        impl BlockDeserialize for A {
            fn deserialize(raw_block: RawBlock) -> Result<Self, TdbError> {
                unimplemented!()
            }
        }
        impl Into<Block> for A {
            fn into(self) -> Block {
                unimplemented!()
            }
        }
        impl AsBlock for A {
            fn get_ref(block: &Block) -> &Self {
                unimplemented!()
            }
        }
        struct B {};
        impl BlockSerialize for B {
            fn serialize(&self) -> Result<RawBlock, TdbError> {
                unimplemented!()
            }
        }
        impl BlockDeserialize for B {
            fn deserialize(raw_block: RawBlock) -> Result<Self, TdbError> {
                unimplemented!()
            }
        }
        impl Into<Block> for B {
            fn into(self) -> Block {
                unimplemented!()
            }
        }
        impl AsBlock for B {
            fn get_ref(block: &Block) -> &Self {
                unimplemented!()
            }
        }

        let id = 2;
        if id == 1 {
            let a: LogicalId<A> = LogicalId::from(id);
        } else {
            let a: LogicalId<B> = LogicalId::from(id);
        }
    }
}
