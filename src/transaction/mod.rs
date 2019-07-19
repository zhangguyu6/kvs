use std::sync::atomic::AtomicU64;
pub type TimeStamp = u64;

pub struct TxnContext {
    begin_ts: u64,
    end_ts: u64,
}

