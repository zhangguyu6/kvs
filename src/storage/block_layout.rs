use crate::{
    storage::index::KeyId,
    transaction::{RecordTxnContext, TupleTxnContext},
};

type BlockId = u32;

struct Block {}

type TupleId = u32;

struct Tuple {
    block_id: BlockId,
    tuple_id: TupleId,
    txn_context: TupleTxnContext,
    key: KeyId,
    val: Vec<u8>,
}

struct Record {}
