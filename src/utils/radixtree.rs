use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const FAN_FACTOR: u32 = 16;

const FAN_OUT: u32 = 1 << FAN_FACTOR;

fn get_index1(node_id: u32) -> usize {
    (node_id % FAN_OUT) as usize
}

fn get_index2(node_id: u32) -> usize {
    (node_id >> FAN_FACTOR % FAN_OUT) as usize
}

// Node1预分配，Node2运行时分配，但不回收 , block由lru background thread回收
pub struct RadixTree<T> {
    inner: Node1<T>,
}

impl<T: Default> Default for RadixTree<T> {
    fn default() -> Self {
        Self {
            inner: Node1::default(),
        }
    }
}

impl<T: Default> RadixTree<T> {
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
    pub fn get_or_touchwritelock(&self, node_id: u32) -> RwLockWriteGuard<'_, T> {
        let index1 = get_index1(node_id);
        let mut node2_ptr = self.inner.children[index1].load(Ordering::SeqCst);
        if node2_ptr.is_null() {
            let new_node2_ptr = Box::into_raw(Box::from(Node2::default()));
            let old_node2_ptr = self.inner.children[index1].compare_and_swap(
                node2_ptr,
                new_node2_ptr,
                Ordering::SeqCst,
            );
            if old_node2_ptr != node2_ptr {
                node2_ptr = old_node2_ptr;
            } else {
                node2_ptr = new_node2_ptr;
            }
        }
        let index2 = get_index2(node_id);
        let node2_ref = unsafe { node2_ptr.as_ref() }.unwrap();
        node2_ref.children[index2].write()
    }
}

struct Node1<T> {
    children: [AtomicPtr<Node2<T>>; FAN_OUT as usize],
}

impl<T: Default> Default for Node1<T> {
    fn default() -> Self {
        let mut children: [AtomicPtr<Node2<T>>; FAN_OUT as usize] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for index in 0..children.len() {
            children[index] = AtomicPtr::new(ptr::null_mut());
        }
        Self { children }
    }
}

struct Node2<T> {
    children: [RwLock<T>; FAN_OUT as usize],
}

impl<T: Default> Default for Node2<T> {
    fn default() -> Self {
        let mut children: [RwLock<T>; FAN_OUT as usize] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for index in 0..children.len() {
            children[index] = RwLock::default()
        }
        Self { children }
    }
}
