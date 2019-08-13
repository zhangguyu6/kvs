use crate::error::TdbError;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const MAX_CAP: usize = 1 << 32;

const FAN_FACTOR: u32 = 16;

const FAN_OUT: u32 = 1 << FAN_FACTOR;

fn get_index1(node_id: u32) -> usize {
    (node_id / FAN_OUT) as usize
}

fn get_index2(node_id: u32) -> usize {
    (node_id % FAN_OUT) as usize
}

// Node1预分配，Node2运行时分配，但不回收 , block由lru background thread回收
pub struct RadixTree<T> {
    inner: Node1<T>,
    cap: AtomicUsize,
}

impl<T: Default> Default for RadixTree<T> {
    fn default() -> Self {
        Self {
            inner: Node1::default(),
            cap: AtomicUsize::new(0),
        }
    }
}

impl<T: Default> RadixTree<T> {
    pub fn with_capacity(cap: usize) -> Result<Self, TdbError> {
        let tree = RadixTree::default();
        tree.extend(cap)?;
        Ok(tree)
    }
    pub fn get_readlock(&self, node_id: u32) -> Option<RwLockReadGuard<'_, T>> {
        let index1 = get_index1(node_id);
        let node2_ptr = self.inner.children[index1].load(Ordering::SeqCst);
        if node2_ptr.is_null() {
            return None;
        }
        let index2 = get_index2(node_id);
        let node2_ref = unsafe { node2_ptr.as_ref() }.unwrap();
        Some(node2_ref.children[index2].read())
    }
    pub fn get_writelock(&self, node_id: u32) -> Option<RwLockWriteGuard<'_, T>> {
        let index1 = get_index1(node_id);
        let node2_ptr = self.inner.children[index1].load(Ordering::SeqCst);
        if node2_ptr.is_null() {
            return None;
        }
        let index2 = get_index2(node_id);
        let node2_ref = unsafe { node2_ptr.as_ref() }.unwrap();
        Some(node2_ref.children[index2].write())
    }
    pub fn extend(&self, extend: usize) -> Result<usize, TdbError> {
        assert!(extend % FAN_OUT as usize == 0);
        let current_cap = self.cap.load(Ordering::Relaxed);
        let new_cap = current_cap + extend;
        if new_cap > MAX_CAP {
            return Err(TdbError::ExceedMaxCap);
        }
        for index in current_cap / (FAN_OUT as usize)..new_cap / (FAN_OUT as usize) {
            let node2_ptr = self.inner.children[index].load(Ordering::SeqCst);
            if node2_ptr.is_null() {
                let new_node2_ptr = Box::into_raw(Box::from(Node2::default()));
                self.inner.children[index].store(new_node2_ptr, Ordering::SeqCst);
            }
        }
        self.cap.store(new_cap, Ordering::SeqCst);
        Ok(current_cap)
    }
    #[inline]
    pub fn get_cap(&self) -> usize {
        self.cap.load(Ordering::SeqCst)
    }
}

struct Node1<T> {
    children: Vec<AtomicPtr<Node2<T>>>,
}

impl<T: Default> Default for Node1<T> {
    fn default() -> Self {
        let mut children = Vec::with_capacity(FAN_OUT as usize);
        for _ in 0..children.capacity() {
            children.push(AtomicPtr::new(ptr::null_mut()));
        }
        Self { children }
    }
}

struct Node2<T> {
    children: Vec<RwLock<T>>,
}

impl<T: Default> Default for Node2<T> {
    fn default() -> Self {
        let mut children = Vec::with_capacity(FAN_OUT as usize);
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
        assert_eq!(tree.extend(FAN_OUT as usize).unwrap(), 0);
        *tree.get_writelock(1).unwrap() = 1;
        assert_eq!(*tree.get_readlock(1).unwrap(), 1);
        assert!(tree.get_readlock(FAN_OUT).is_none());
        assert!(tree.get_writelock(FAN_OUT).is_none());
        assert_eq!(tree.extend(FAN_OUT as usize).unwrap(), FAN_OUT as usize);
        *tree.get_writelock(FAN_OUT).unwrap() = 2;
        assert_eq!(*tree.get_readlock(FAN_OUT).unwrap(), 2);
        assert_eq!(tree.get_cap(), (FAN_OUT as usize) * 2);
    }
}
