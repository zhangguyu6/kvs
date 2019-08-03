#[allow(dead_code)]
mod adaptive_radix_tree;

use std::ops::Range;

pub trait Index<T>: Sync + 'static {
    fn get(&self, key: &[u8]) -> Option<T>;
    fn insert(&self, key: Vec<u8>, val: T);
    fn del(&self, key: &[u8]) -> Option<T>;
    fn range<Iter: Iterator>(&self, range: Range<&[u8]>) -> Iter;
}

pub struct Leaf {}

pub struct Branch {}
