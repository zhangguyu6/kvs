use crate::{
    error::TxnError,
    storage::{
        block_io::{BlockDev, RawBlockDev, G_DEV},
        block_layout::{BlockId, RawBlock},
    },
    transaction::{TimeStamp, GLOBAL_TS, LOCAL_TS},
};
use lazy_static::lazy_static;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicPtr, Ordering};

lazy_static! {
    pub static ref GLOBAL_BLOCK_TABLE: BlockTable = BlockTable::new();
}

pub struct BlockTable {}

impl BlockTable {
    fn new() -> Self {
        unimplemented!()
    }
}

pub struct BlockRef<B: AsBlock> {
    block_ptr: AtomicPtr<B>,
    block_id: BlockId,
    logical_id: LogicalId<B>,
    commit_ts: TimeStamp,
    next_block: AtomicPtr<BlockRef<B>>,
}

#[derive(Copy, Clone)]
pub struct LogicalId<B: AsBlock>(u64, PhantomData<B>);

impl<B: AsBlock> Deref for LogicalId<B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        let local_ts = LOCAL_TS.with(|ts| *ts.borrow());
        if let Some(block_ref) = GLOBAL_BLOCK_TABLE.get(self, local_ts) {
            let block_id = block_ref.block_id;
            let mut block_ptr = block_ref.block_ptr.load(Ordering::SeqCst);
            if block_ptr.is_null() {
                let mut raw_block = RawBlock::default();
                let result = G_DEV.sync_read(block_id, &mut raw_block);
                assert!(result.is_ok());
                let block = B::deserialize(raw_block).unwrap();
                let new_block_ptr = Box::into_raw(Box::new(block));
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
            return unsafe { block_ptr.as_ref() }.unwrap();
        }

        panic!("deref null pointer")
    }
}

impl BlockTable {
    fn get<B: AsBlock>(&self, node_id: &LogicalId<B>, ts: TimeStamp) -> Option<BlockRef<B>> {
        unimplemented!()
    }

    fn insert<B: AsBlock>(&self, node_id: &LogicalId<B>, block_ref: BlockRef<B>) {
        unimplemented!()
    }

    fn del<B: AsBlock>(&self, node_id: &LogicalId<B>) -> Option<BlockRef<B>> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct Tuple {
    key: Vec<u8>,
    val: Vec<u8>,
}

pub trait BlockSerialize {
    fn serialize(&self) -> Result<RawBlock, TxnError>;
}

pub trait BlockDeserialize: Sized {
    fn deserialize(raw_block: RawBlock) -> Result<Self, TxnError>;
}

pub trait AsBlock: BlockSerialize + BlockDeserialize {
    fn get_block_kind(&self) -> BlockKind;
}

pub enum BlockKind {}
