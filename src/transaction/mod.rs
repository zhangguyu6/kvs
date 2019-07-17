use std::sync::atomic::AtomicU64;
pub(crate) struct TupleTxnContext {
    begin_ts: u64,
    end_ts: AtomicU64,
}

pub(crate) struct RecordTxnContext {
    commit_ts: u64,
    on_writing: bool,
}
