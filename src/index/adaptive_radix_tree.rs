use crate::error::TdbError::{self, *};
use crate::transaction::{TimeStamp, LOCAL_TS};
use std::mem::{self, MaybeUninit};
use std::ops::Range;
use std::result;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::thread::yield_now;

enum Node<K, V> {
    N4(Node4<K, V>),
    N16(Node16<K, V>),
    N48(Node48<K, V>),
    N256(Node256<K, V>),
    Leaf(K, V),
}

struct NodeBase {
    children_num: u8,
    ts: TimeStamp,
    prefix: Vec<u8>,
}

impl Default for NodeBase {
    fn default() -> Self {
        Self {
            children_num: 0,
            ts: LOCAL_TS.with(|ts| *ts.borrow()),
            prefix: Vec::new(),
        }
    }
}

impl NodeBase {
    // return len of string eq
    #[inline]
    fn prefix_match_len<K: AsRef<[u8]>>(&self, key: K, depth: usize) -> usize {
        for i in 0..self.prefix.len() {
            if key.as_ref()[i + depth] != self.prefix[i] {
                return i;
            }
        }
        self.prefix.len()
    }
}

struct Node4<K, V> {
    base: NodeBase,
    keys: [u8; 4],
    children: [AtomicPtr<Node<K, V>>; 4],
}

impl<K, V> Default for Node4<K, V> {
    fn default() -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 4] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            keys: [0; 4],
            children: children,
        }
    }
}

impl<K, V> Drop for Node4<K, V> {
    fn drop(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                unsafe { Box::from_raw(node_ptr) };
            }
        }
    }
}

struct Node16<K, V> {
    base: NodeBase,
    keys: [u8; 16],
    children: [AtomicPtr<Node<K, V>>; 16],
}

impl<K, V> Default for Node16<K, V> {
    fn default() -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 16] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            keys: [0; 16],
            children: children,
        }
    }
}

impl<K, V> Drop for Node16<K, V> {
    fn drop(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                unsafe { Box::from_raw(node_ptr) };
            }
        }
    }
}

struct Node48<K, V> {
    base: NodeBase,
    keys: [u8; 48],
    children: [AtomicPtr<Node<K, V>>; 48],
}

impl<K, V> Default for Node48<K, V> {
    fn default() -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 48] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            keys: [0; 48],
            children: children,
        }
    }
}

impl<K, V> Drop for Node48<K, V> {
    fn drop(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                unsafe { Box::from_raw(node_ptr) };
            }
        }
    }
}

struct Node256<K, V> {
    base: NodeBase,
    children: [AtomicPtr<Node<K, V>>; 256],
}

impl<K, V> Default for Node256<K, V> {
    fn default() -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 256] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            children: children,
        }
    }
}

impl<K, V> Drop for Node256<K, V> {
    fn drop(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                unsafe { Box::from_raw(node_ptr) };
            }
        }
    }
}

pub struct Art<K, V> {
    root: AtomicPtr<Node<K, V>>,
}

impl<K, V> Default for Art<K, V> {
    fn default() -> Self {
        Self {
            root: AtomicPtr::default(),
        }
    }
}

impl<K, V> Drop for Art<K, V> {
    fn drop(&mut self) {
        let node_ptr = self.root.load(Ordering::Relaxed);
        if !node_ptr.is_null() {
            unsafe { Box::from_raw(node_ptr) };
        }
    }
}

impl<K: AsRef<[u8]>, V: Clone> Art<K, V> {
    pub fn get(&self, key: K) -> Option<V> {
        unimplemented!()
    }

    // copy path
    pub fn insert(&self, key: K, val: V) -> Option<V> {
        unimplemented!()
    }

    // copy path
    pub fn remove(&self, key: K) -> Option<V> {
        unimplemented!()
    }

    pub fn range<Iter: Iterator>(&self, range: Range<K>) -> Iter {
        unimplemented!()
    }
}

impl<K: AsRef<u8>, V: Clone> Node<K, V> {}
