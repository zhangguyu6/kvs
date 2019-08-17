mod context;
mod log;
pub use context::{Context, MutContext};

use lazy_static::lazy_static;
use std::cell::RefCell;
use std::sync::atomic::AtomicU64;
use std::u64;

pub type TimeStamp = u64;
pub const MAX_TS: u64 = u64::MAX;
pub const MIN_TS: u64 = 0;

thread_local!(pub static LOCAL_TS: RefCell<TimeStamp> = RefCell::new(0));

lazy_static! {
    pub static ref GLOBAL_MIN_TS: AtomicU64 = AtomicU64::new(0);
    pub static ref GLOBAL_MAX_TS: AtomicU64 = AtomicU64::new(0);
}
