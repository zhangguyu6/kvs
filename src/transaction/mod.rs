mod immut_context;
mod mut_context;
pub use immut_context::ImmutContext;
pub use mut_context::MutContext;

use std::u64;

pub type TimeStamp = u64;
pub const MAX_TS: u64 = u64::MAX;
pub const MIN_TS: u64 = 0;
