use lazy_static::lazy_static;
use std::cell::RefCell;
use std::sync::RwLock;
pub type TimeStamp = u64;

thread_local!(pub static LOCAL_TS: RefCell<TimeStamp> = RefCell::new(0));

lazy_static! {
    pub static ref GLOBAL_TS: RwLock<u64> = RwLock::new(0);
}
