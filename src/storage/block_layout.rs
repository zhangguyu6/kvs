use super::{BlockId, NodeId};
use crate::transaction::{TimeStamp, TxnContext};
use std::collections::VecDeque;

enum Block {
    SmallData,
    Data,
    DataIndex1,
    DataIndex2,
    DataIndex3,
    Leaf,
    Branch,
    Meta,
}

pub struct Tuple {
    block_id: BlockId,
    txn_context: TxnContext,
    key: Vec<u8>,
    val: Vec<u8>,
}

pub struct Record {
    node_id: NodeId,
    txn_context: TxnContext,
    versions: VecDeque<Tuple>,
}
