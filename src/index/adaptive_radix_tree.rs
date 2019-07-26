use crate::error::TdbError::{self, *};
use crate::transaction::{TimeStamp, LOCAL_TS};
use std::mem::{self, MaybeUninit};
use std::ops::Range;
use std::ptr::NonNull;
use std::result;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::thread::yield_now;

const DEL: u8 = std::u8::MAX;

enum Node<K, V> {
    B(Branch<K, V>),
    L(K, V),
}

impl<K, V> Destory for Node<K, V> {
    fn destory(&mut self) {
        match self {
            Node::B(branch) => branch.destory(),
            _ => {}
        }
    }
}

enum Branch<K, V> {
    N4(Node4<K, V>),
    N16(Node16<K, V>),
    N48(Node48<K, V>),
    N256(Node256<K, V>),
}

impl<K, V> Destory for Branch<K, V> {
    fn destory(&mut self) {
        use Branch::*;
        match self {
            N4(node) => node.destory(),
            N16(node) => node.destory(),
            N48(node) => node.destory(),
            N256(node) => node.destory(),
        }
    }
}

trait Destory {
    fn destory(&mut self);
}

#[derive(Clone)]
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
    children_nums: usize,
}

impl<K, V> Clone for Node4<K, V> {
    fn clone(&self) -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 4] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
            children[i].store(self.children[i].load(Ordering::SeqCst), Ordering::SeqCst);
        }
        Self {
            base: self.base.clone(),
            keys: self.keys.clone(),
            children: children,
            children_nums: self.children_nums,
        }
    }
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
            keys: [DEL; 4],
            children: children,
            children_nums: 0,
        }
    }
}

impl<K, V> Destory for Node4<K, V> {
    fn destory(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                let mut node = unsafe { Box::from_raw(node_ptr) };
                node.destory()
            }
        }
    }
}

struct Node16<K, V> {
    base: NodeBase,
    keys: [u8; 16],
    children: [AtomicPtr<Node<K, V>>; 16],
    children_nums: usize,
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
            keys: [DEL; 16],
            children: children,
            children_nums: 0,
        }
    }
}

impl<K, V> Destory for Node16<K, V> {
    fn destory(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                let mut node = unsafe { Box::from_raw(node_ptr) };
                node.destory();
            }
        }
    }
}

impl<K, V> Clone for Node16<K, V> {
    fn clone(&self) -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 16] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
            children[i].store(self.children[i].load(Ordering::SeqCst), Ordering::SeqCst);
        }
        Self {
            base: self.base.clone(),
            keys: self.keys.clone(),
            children: children,
            children_nums: self.children_nums,
        }
    }
}

struct Node48<K, V> {
    base: NodeBase,
    keys: [u8; 256],
    children: [AtomicPtr<Node<K, V>>; 48],
    children_nums: usize,
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
            keys: [DEL; 256],
            children: children,
            children_nums: 0,
        }
    }
}

impl<K, V> Destory for Node48<K, V> {
    fn destory(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                unsafe { Box::from_raw(node_ptr) };
            }
        }
    }
}

impl<K, V> Clone for Node48<K, V> {
    fn clone(&self) -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 48] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
            children[i].store(self.children[i].load(Ordering::SeqCst), Ordering::SeqCst);
        }
        Self {
            base: self.base.clone(),
            keys: self.keys.clone(),
            children: children,
            children_nums: self.children_nums,
        }
    }
}

struct Node256<K, V> {
    base: NodeBase,
    children: [AtomicPtr<Node<K, V>>; 256],
    children_nums: usize,
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
            children_nums: 0,
        }
    }
}

impl<K, V> Destory for Node256<K, V> {
    fn destory(&mut self) {
        for aptr in self.children.iter() {
            let node_ptr = aptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                let mut node = unsafe { Box::from_raw(node_ptr) };
                node.destory();
            }
        }
    }
}

impl<K, V> Clone for Node256<K, V> {
    fn clone(&self) -> Self {
        let mut children: [AtomicPtr<Node<K, V>>; 256] =
            unsafe { MaybeUninit::uninit().assume_init() };
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
            children[i].store(self.children[i].load(Ordering::SeqCst), Ordering::SeqCst);
        }
        Self {
            base: self.base.clone(),
            children: children,
            children_nums: self.children_nums,
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

impl<K, V> Destory for Art<K, V> {
    fn destory(&mut self) {
        let node_ptr = self.root.load(Ordering::Relaxed);
        if !node_ptr.is_null() {
            let node = unsafe { Box::from_raw(node_ptr) };
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

impl<K, V> Branch<K, V> {
    fn find_child(&self, byte: u8) -> Option<&Node<K, V>> {
        use Branch::*;
        match self {
            N4(node) => {
                for i in 0..node.children_nums {
                    if node.keys[i] == byte {
                        let ptr = node.children[i].load(Ordering::SeqCst);
                        assert!(!ptr.is_null());
                        let ptr_ref = unsafe { &*ptr };
                        return Some(ptr_ref);
                    }
                }
            }
            N16(node) => {
                for i in 0..node.children_nums {
                    if node.keys[i] == byte {
                        let ptr = node.children[i].load(Ordering::SeqCst);
                        assert!(!ptr.is_null());
                        let ptr_ref = unsafe { &*ptr };
                        return Some(ptr_ref);
                    }
                }
            }
            N48(node) => {
                let index = node.keys[byte as usize];
                if index != DEL {
                    let ptr = node.children[index as usize].load(Ordering::SeqCst);
                    assert!(!ptr.is_null());
                    let ptr_ref = unsafe { &*ptr };
                    return Some(ptr_ref);
                }
            }
            N256(node) => {
                let ptr = node.children[byte as usize].load(Ordering::SeqCst);
                if !ptr.is_null() {
                    let ptr_ref = unsafe { &*ptr };
                    return Some(ptr_ref);
                }
            }
        }
        None
    }
    // old ptr (if old ptr is clean), current ref ,
    fn find_child_ptr(&mut self, byte: u8) -> Option<&AtomicPtr<Node<K, V>>> {
        use Branch::*;
        match self {
            N4(node) => {
                for i in 0..node.children_nums {
                    if node.keys[i] == byte {
                        return Some(&node.children[i]);
                    }
                }
            }
            N16(node) => {
                for i in 0..node.children_nums {
                    if node.keys[i] == byte {
                        if node.keys[i] == byte {
                            return Some(&node.children[i]);
                        }
                    }
                }
            }
            N48(node) => {
                let index = node.keys[byte as usize];
                if index != DEL {
                    return Some(&node.children[index as usize]);
                }
            }
            N256(node) => return Some(&node.children[byte as usize]),
        }
        None
    }
    //     fn find_children_mut(&mut self, byte: u8) -> Option<(*mut Node<K,V>,&mut Node<K, V>)> {
    //         let current_ts =LOCAL_TS.with(|ts| *ts.borrow());
    //         for i in 0..self.children_nums {
    //             if self.keys[i] == byte {
    //                 let ptr = self.children[i].load(Ordering::SeqCst);
    //                 assert!(!ptr.is_null());
    //                 let ptr_ref = unsafe { ptr.as_ref() }.unwrap();
    //                 if ptr_ref.base.ts == current_ts {
    //                     return Some(unsafe { &mut *ptr });
    //                 }
    //             }
    //         }
    //         None
    //     }
    // }
}

impl<K, V> Node<K, V> {
    fn is_leaf(&self) -> bool {
        match self {
            Node::B(_) => false,
            _ => true,
        }
    }
    fn get_branch(&self) -> &Branch<K, V> {
        match self {
            Node::B(branch) => branch,
            _ => panic!("only has leaf"),
        }
    }
}
