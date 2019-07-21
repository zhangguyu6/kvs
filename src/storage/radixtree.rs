use crate::storage::block_table::{AsBlock, BlockRef};
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

struct RadixTree {
    inner: Node1,
}

impl RadixTree {
    pub fn get(&self, node_id: u64) -> Option<*mut Box<dyn Any>> {
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
    pub fn store(&self, node_id: u64, val: *mut Box<dyn Any>) -> *mut Box<dyn Any> {
        let index1 = get_index1(node_id);
        let mut node2_ptr = self.inner.children[index1].load(Ordering::SeqCst);
        if node2_ptr.is_null() {
            node2_ptr = Box::into_raw(Box::from(Node2::default()));
            self.inner.children[index1].store(node2_ptr, Ordering::SeqCst);
        }
        let index2 = get_index2(node_id);
        let block_ptr = unsafe { &(*node2_ptr).children[index2] }.load(Ordering::SeqCst);
        unsafe { &(*node2_ptr).children[index2] }.store(val, Ordering::SeqCst);
        block_ptr
    }
    pub fn del(&self, node_id: u64) -> Option<*mut Box<dyn Any>> {
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
        unsafe { &(*node2_ptr).children[index2] }.store(ptr::null_mut(),Ordering::SeqCst);
        self.inner.children[index1].store(ptr::null_mut(),Ordering::SeqCst);
        Box::from(node2_ptr);
        Some(block_ptr)
    }
}

struct Node1 {
    children: [AtomicPtr<Node2>; FAN_OUT as usize],
}

impl Default for Node1 {
    fn default() -> Self {
        let mut children: [AtomicPtr<Node2>; FAN_OUT as usize] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for index in 0..children.len() {
            children[index] = AtomicPtr::new(ptr::null_mut());
        }
        Self { children }
    }
}

struct Node2 {
    children: [AtomicPtr<Box<dyn Any>>; FAN_OUT as usize],
}

impl Default for Node2 {
    fn default() -> Self {
        let mut children: [AtomicPtr<Box<dyn Any>>; FAN_OUT as usize] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for index in 0..children.len() {
            children[index] = AtomicPtr::new(ptr::null_mut());
        }
        Self { children }
    }
}
