#![feature(core_intrinsics)]
#![feature(arbitrary_self_types)]
mod storage;
mod transaction;
mod index;
mod error;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
