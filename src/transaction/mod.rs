use lazy_static::lazy_static;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
pub type TimeStamp = u64;

thread_local!(pub static LOCAL_TS: RefCell<TimeStamp> = RefCell::new(0));

lazy_static! {
    pub static ref GLOBAL_MIN_TS: AtomicU64 = AtomicU64::new(0);
    pub static ref GLOBAL_MAX_TS: AtomicU64 = AtomicU64::new(0);
}
