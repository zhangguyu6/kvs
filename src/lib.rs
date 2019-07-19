#![feature(core_intrinsics)]
mod storage;
mod transaction;
mod index;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
