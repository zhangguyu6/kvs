#![feature(core_intrinsics)]
#![feature(arbitrary_self_types)]
mod error;
mod index;
mod storage;
mod transaction;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        fn iter<F: FnMut(&u8)>(vec: &Vec<u8>, f: &mut F) {
            for i in vec.iter() {
                f(i)
            }
        }
        let mut vecs = vec![1, 2, 3, 4];
        let mut vecs1 = vec![];
        let mut f = |e: &u8| {
            vecs1.push(e.clone());
        };
        iter(&vecs, &mut f);
        assert_eq!(2 + 2, 4);
    }

}
