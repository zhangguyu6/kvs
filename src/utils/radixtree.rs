use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
// Two level array
// First level array fills with atomic ptr to second level array
// Second level array fills with Rwlock<T>
// First level array initializes when radixtree is allocated
// Second level array initializes at running time
pub struct RadixTree<T> {
    first_level: Vec<AtomicPtr<Node<T>>>,
    first_level_size: u32,
    second_level_size: u32,
    len: AtomicU32,
}

impl<T> Drop for RadixTree<T> {
    fn drop(&mut self) {
        self.shrink_to(0);
    }
}

impl<T> RadixTree<T> {
    pub fn get_node_ptr(&self, level1_index: usize) -> &AtomicPtr<Node<T>> {
        &self.first_level[level1_index]
    }

    fn get_level1_index(&self, oid: u32) -> usize {
        (oid / self.second_level_size) as usize
    }

    fn get_level2_index(&self, oid: u32) -> usize {
        (oid % self.second_level_size) as usize
    }

    pub fn get_readlock(&self, oid: u32) -> Option<RwLockReadGuard<T>> {
        let index1 = self.get_level1_index(oid);
        let node_ptr = self.first_level[index1].load(Ordering::SeqCst);
        if node_ptr.is_null() {
            return None;
        }
        let index2 = self.get_level2_index(oid);
        let node_ref = unsafe { node_ptr.as_ref() }.unwrap();
        Some(node_ref.children[index2].read())
    }
    pub fn get_writelock(&self, oid: u32) -> Option<RwLockWriteGuard<T>> {
        let index1 = self.get_level1_index(oid);
        let node_ptr = self.first_level[index1].load(Ordering::SeqCst);
        if node_ptr.is_null() {
            return None;
        }
        let index2 = self.get_level2_index(oid);
        let node_ref = unsafe { node_ptr.as_ref() }.unwrap();
        Some(node_ref.children[index2].write())
    }

    // Shrink tree to len, free old node
    // Return old len
    pub fn shrink_to(&self, new_len: u32) -> u32 {
        let current_len = self.len.load(Ordering::Relaxed);
        assert!(new_len % self.second_level_size == 0 && new_len < current_len);
        for index in (new_len / self.second_level_size) as usize
            ..(current_len / self.second_level_size) as usize
        {
            let node_ptr = self.first_level[index].swap(null_mut(), Ordering::SeqCst);
            assert!(!node_ptr.is_null());
            let node = unsafe { Box::from_raw(node_ptr) };
            drop(node);
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

impl<T: Default> RadixTree<T> {
    pub fn new(len: u32, first_level_size: u32, second_level_size: u32) -> Self {
        let mut first_level = Vec::with_capacity(first_level_size as usize);
        // first level array init
        for _ in 0..first_level.capacity() {
            first_level.push(AtomicPtr::default());
        }
        let tree = Self {
            first_level,
            first_level_size,
            second_level_size,
            len: AtomicU32::new(len),
        };
        tree.extend_to(len);
        tree
    }
    // Extend tree to len + extend, allocate new node
    // Return old len
    pub fn extend_to(&self, new_len: u32) -> u32 {
        let current_len = self.len.load(Ordering::Relaxed);
        assert!(new_len % self.second_level_size == 0 && current_len <= new_len);
        for index in (current_len / self.second_level_size) as usize
            ..(new_len / self.second_level_size) as usize
        {
            let node_ptr = self.first_level[index].load(Ordering::SeqCst);
            assert!(node_ptr.is_null());
            let new_node_ptr =
                Box::into_raw(Box::from(Node::with_capacity(self.second_level_size)));
            self.first_level[index].store(new_node_ptr, Ordering::SeqCst);
        }
        self.len.store(new_len, Ordering::SeqCst);
        current_len
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
        let tree: RadixTree<u8> = RadixTree::new(0, 32, 512);
        assert!(tree.get_readlock(0).is_none());
        assert!(tree.get_writelock(0).is_none());
        assert_eq!(tree.extend_to(512), 0);
        *tree.get_writelock(1).unwrap() = 1;
        assert_eq!(*tree.get_readlock(1).unwrap(), 1);
        assert!(tree.get_readlock(512).is_none());
        assert!(tree.get_writelock(512).is_none());
        assert_eq!(tree.extend_to(1024), 512);
        *tree.get_writelock(512).unwrap() = 2;
        assert_eq!(*tree.get_readlock(512).unwrap(), 2);
        assert_eq!(tree.get_len(), 512 * 2);
        assert_eq!(tree.shrink_to(512), 1024);
        assert_eq!(tree.get_len(), 512);
        assert!(tree.get_readlock(512).is_none());
        assert!(tree.get_writelock(512).is_none());
    }
}
