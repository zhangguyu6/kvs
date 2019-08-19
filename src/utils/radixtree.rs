use crate::error::TdbError;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use std::u32;

const MAX_CAP: u32 = u32::MAX;

pub const DEFAULT_LEVEL1_LEN: u32 = MAX_CAP / DEFAULT_LEVEL2_LEN;

pub const DEFAULT_LEVEL2_LEN: u32 = 511;

// Node1预分配，Node2运行时分配，但不回收 , block由lru background thread回收
pub struct RadixTree<T> {
    inner: Vec<AtomicPtr<Node<T>>>,
    len: AtomicU32,
    node_len: u32,
}

impl<T: Default> Default for RadixTree<T> {
    fn default() -> Self {
        Self::new(0, DEFAULT_LEVEL1_LEN, DEFAULT_LEVEL2_LEN)
    }
}

impl<T: Default> RadixTree<T> {
    pub fn new(len: u32, inner_len: u32, node_len: u32) -> Self{
        let mut inner = Vec::with_capacity(inner_len as usize);
        for _ in 0..inner.capacity() {
            inner.push(AtomicPtr::default());
        }
        let tree = Self {
            inner: inner,
            len: AtomicU32::new(0),
            node_len: node_len,
        };
        tree.extend(len);
        tree
    }

    pub fn get_node_ptr(&self, level1_index: usize) -> &AtomicPtr<Node<T>> {
        &self.inner[level1_index]
    }

    pub fn get_level1_index(&self, oid: u32) -> usize {
        (oid / self.node_len) as usize
    }

    fn get_level2_index(&self, oid: u32) -> usize {
        (oid % self.node_len) as usize
    }

    pub fn get_readlock(&self, oid: u32) -> Option<RwLockReadGuard<'_, T>> {
        let index1 = self.get_level1_index(oid);
        let node_ptr = self.inner[index1].load(Ordering::SeqCst);
        if node_ptr.is_null() {
            return None;
        }
        let index2 = self.get_level2_index(oid);
        let node_ref = unsafe { node_ptr.as_ref() }.unwrap();
        Some(node_ref.children[index2].read())
    }
    pub fn get_writelock(&self, oid: u32) -> Option<RwLockWriteGuard<'_, T>> {
        let index1 = self.get_level1_index(oid);
        let node_ptr = self.inner[index1].load(Ordering::SeqCst);
        if node_ptr.is_null() {
            return None;
        }
        let index2 = self.get_level2_index(oid);
        let node_ref = unsafe { node_ptr.as_ref() }.unwrap();
        Some(node_ref.children[index2].write())
    }
    // Extend tree'a cap to cap + extend
    // Return old cap
    pub fn extend(&self, extend: u32) -> u32 {
        assert!(extend % self.node_len == 0);
        let current_len = self.len.load(Ordering::Relaxed);
        let new_len = current_len + extend;
        if new_len > MAX_CAP {
            panic!("bigger than max cap");
        }
        for index in (current_len / self.node_len) as usize..(new_len / self.node_len) as usize {
            let node_ptr = self.inner[index].load(Ordering::SeqCst);
            if node_ptr.is_null() {
                let new_node_ptr = Box::into_raw(Box::from(Node::with_capacity(self.node_len)));
                self.inner[index].store(new_node_ptr, Ordering::SeqCst);
            }
        }
        self.len.store(new_len, Ordering::SeqCst);
        current_len
    }
    #[inline]
    pub fn get_len(&self) -> u32 {
        self.len.load(Ordering::SeqCst)
    }
    #[inline]
    pub fn add_len(&self, len: u32) -> u32 {
        self.len.fetch_add(len, Ordering::SeqCst)
    }
}

pub struct Node<T> {
    pub children: Vec<RwLock<T>>,
}

impl<T: Default> Node<T> {
    fn with_capacity(cap: u32) -> Self {
        let mut children = Vec::with_capacity(cap as usize);
        for _ in 0..children.capacity() {
            children.push(RwLock::default());
        }
        Self { children }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_radixtree() {
        let tree: RadixTree<u8> = RadixTree::default();
        assert!(tree.get_readlock(0).is_none());
        assert!(tree.get_writelock(0).is_none());
        assert_eq!(tree.extend(DEFAULT_LEVEL2_LEN), 0);
        *tree.get_writelock(1).unwrap() = 1;
        assert_eq!(*tree.get_readlock(1).unwrap(), 1);
        assert!(tree.get_readlock(DEFAULT_LEVEL2_LEN).is_none());
        assert!(tree.get_writelock(DEFAULT_LEVEL2_LEN).is_none());
        assert_eq!(tree.extend(DEFAULT_LEVEL2_LEN), DEFAULT_LEVEL2_LEN);
        *tree.get_writelock(DEFAULT_LEVEL2_LEN).unwrap() = 2;
        assert_eq!(*tree.get_readlock(DEFAULT_LEVEL2_LEN).unwrap(), 2);
        assert_eq!(tree.get_len(), DEFAULT_LEVEL2_LEN * 2);
    }
}
