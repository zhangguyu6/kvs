use std::any::Any;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
const FAN_FACTOR: u64 = 18;
const FAN_OUT: u64 = 1 << FAN_FACTOR;

fn get_index1(node_id: u64) -> usize {
    (node_id % FAN_OUT) as usize
}

fn get_index2(node_id: u64) -> usize {
    (node_id >> FAN_FACTOR % FAN_OUT) as usize
}

// Node1预分配，Node2运行时分配，但不回收 , block由lru background thread回收
pub struct RadixTree<T> {
    inner: Node1<T>,
}

impl<T> Default for RadixTree<T> {
    fn default() -> Self {
        Self {
            inner: Node1::default(),
        }
    }
}

impl<T: Send + 'static> RadixTree<T> {
    
    pub fn get(&self, node_id: u64) -> Option<*mut T> {
        let index1 = get_index1(node_id);
        let node2_ptr = self.inner.children[index1].load(Ordering::SeqCst);
        if node2_ptr.is_null() {
            return None;
        }
        let index2 = get_index2(node_id);
        let block_ptr = unsafe { &(*node2_ptr).children[index2] }.load(Ordering::SeqCst);
        if block_ptr.is_null() {
            return None;
        }
        Some(block_ptr)
    }

    pub fn get_or_touch(&self, node_id: u64) -> *mut T {
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
        unsafe { &(*node2_ptr).children[index2] }.load(Ordering::SeqCst)
    }

    pub fn cas(&self, node_id: u64, old: *mut T, new: *mut T) -> *mut T {
        let index1 = get_index1(node_id);
        let node2_ptr = self.inner.children[index1].load(Ordering::SeqCst);
        assert!(!node2_ptr.is_null());
        let index2 = get_index2(node_id);
        unsafe { &(*node2_ptr).children[index2] }.compare_and_swap(old, new, Ordering::SeqCst)
    }
}

struct Node1<T> {
    children: [AtomicPtr<Node2<T>>; FAN_OUT as usize],
}

impl<T> Default for Node1<T> {
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
    children: [AtomicPtr<T>; FAN_OUT as usize],
}

impl<T> Default for Node2<T> {
    fn default() -> Self {
        let mut children: [AtomicPtr<T>; FAN_OUT as usize] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for index in 0..children.len() {
            children[index] = AtomicPtr::new(ptr::null_mut());
        }
        Self { children }
    }
}
