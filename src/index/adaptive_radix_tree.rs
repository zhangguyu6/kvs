use crate::transaction::{TimeStamp, LOCAL_TS};
use arrayvec::ArrayVec;
use std::cmp;
use std::collections::HashSet;
use std::mem::{self, MaybeUninit};
use std::ops::Range;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

const DEL: u8 = std::u8::MAX;

pub enum Node<K, V> {
    B(Branch<K, V>),
    L(Leaf<K, V>),
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

impl<K, V> Clone for Branch<K, V> {
    fn clone(&self) -> Self {
        use Branch::*;
        match self {
            N4(node) => N4(node.clone()),
            N16(node) => N16(node.clone()),
            N48(node) => N48(node.clone()),
            N256(node) => N256(node.clone()),
        }
    }
}

fn longest_common_prefix<K: AsRef<[u8]>>(key0: &K, key1: &K, depth: usize) -> usize {
    let max_cmp = cmp::min(key0.as_ref().len(), key1.as_ref().len()) - depth;
    for i in 0..max_cmp {
        if key0.as_ref()[depth + i] != key1.as_ref()[depth + i] {
            return i;
        }
    }
    panic!("leaf1 and leaf2 should not eq or contain");
}

struct Leaf<K, V>(K, V);

trait Destory {
    fn destory(&mut self);
}

#[derive(Clone)]
struct NodeBase {
    children_num: u8,
    ts: TimeStamp,
    prefix: Vec<u8>,
}

impl NodeBase {
    fn new(prefix: Vec<u8>) -> Self {
        NodeBase {
            children_num: 0,
            ts: LOCAL_TS.with(|ts| *ts.borrow()),
            prefix: prefix,
        }
    }
}

impl Default for NodeBase {
    fn default() -> Self {
        Self {
            children_num: 0,
            ts: LOCAL_TS.with(|ts| *ts.borrow()),
            prefix: Vec::with_capacity(0),
        }
    }
}

impl NodeBase {
    // return len of string
    #[inline]
    fn prefix_match_len<K: AsRef<[u8]>>(&self, key: &K, depth: usize) -> usize {
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
    entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 4]>,
}

impl<K, V> Clone for Node4<K, V> {
    fn clone(&self) -> Self {
        let mut entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 4]> = ArrayVec::default();
        for i in 0..entrys.len() {
            entrys[i].0 = self.entrys[i].0;
            entrys[i].1 = AtomicPtr::default();
            entrys[i]
                .1
                .store(self.entrys[i].1.load(Ordering::SeqCst), Ordering::SeqCst);
        }
        let mut base = self.base.clone();
        base.ts = LOCAL_TS.with(|ts| *ts.borrow());
        Self {
            base: base,
            entrys: entrys,
        }
    }
}
impl<K, V> Default for Node4<K, V> {
    fn default() -> Self {
        let mut entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 4]> = ArrayVec::default();
        for i in 0..entrys.len() {
            entrys[i].0 = DEL;
            entrys[i].1 = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            entrys: ArrayVec::default(),
        }
    }
}

impl<K, V> Destory for Node4<K, V> {
    fn destory(&mut self) {
        for (_, atomic_ptr) in self.entrys.iter() {
            let node_ptr = atomic_ptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                let mut node = unsafe { Box::from_raw(node_ptr) };
                node.destory()
            }
        }
    }
}

struct Node16<K, V> {
    base: NodeBase,
    entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 16]>,
}

impl<K, V> Default for Node16<K, V> {
    fn default() -> Self {
        let mut entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 16]> = ArrayVec::default();
        for i in 0..entrys.len() {
            entrys[i].0 = DEL;
            entrys[i].1 = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            entrys: entrys,
        }
    }
}

impl<K, V> Destory for Node16<K, V> {
    fn destory(&mut self) {
        for (_, atomic_ptr) in self.entrys.iter() {
            let node_ptr = atomic_ptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                let mut node = unsafe { Box::from_raw(node_ptr) };
                node.destory();
            }
        }
    }
}

impl<K, V> Clone for Node16<K, V> {
    fn clone(&self) -> Self {
        let mut entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 16]> = ArrayVec::default();
        for i in 0..entrys.len() {
            entrys[i].0 = self.entrys[i].0;
            entrys[i].1 = AtomicPtr::default();
            entrys[i]
                .1
                .store(self.entrys[i].1.load(Ordering::SeqCst), Ordering::SeqCst);
        }
        let mut base = self.base.clone();
        base.ts = LOCAL_TS.with(|ts| *ts.borrow());
        Self {
            base: base,
            entrys: entrys,
        }
    }
}

struct Node48<K, V> {
    base: NodeBase,
    keys: ArrayVec<[u8; 256]>,
    children: ArrayVec<[AtomicPtr<Node<K, V>>; 48]>,
}

impl<K, V> Default for Node48<K, V> {
    fn default() -> Self {
        let mut children: ArrayVec<[AtomicPtr<Node<K, V>>; 48]> = ArrayVec::default();
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            keys: ArrayVec::from([DEL; 256]),
            children: children,
        }
    }
}

impl<K, V> Destory for Node48<K, V> {
    fn destory(&mut self) {
        for atomic_ptr in self.children.iter() {
            let node_ptr = atomic_ptr.load(Ordering::Relaxed);
            if !node_ptr.is_null() {
                unsafe { Box::from_raw(node_ptr) };
            }
        }
    }
}

impl<K, V> Clone for Node48<K, V> {
    fn clone(&self) -> Self {
        let mut children: ArrayVec<[AtomicPtr<Node<K, V>>; 48]> = ArrayVec::default();
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
            children[i].store(self.children[i].load(Ordering::SeqCst), Ordering::SeqCst);
        }
        let mut base = self.base.clone();
        base.ts = LOCAL_TS.with(|ts| *ts.borrow());
        Self {
            base: base,
            keys: self.keys.clone(),
            children: children,
        }
    }
}

struct Node256<K, V> {
    base: NodeBase,
    children: ArrayVec<[AtomicPtr<Node<K, V>>; 256]>,
}

impl<K, V> Default for Node256<K, V> {
    fn default() -> Self {
        let mut children: ArrayVec<[AtomicPtr<Node<K, V>>; 256]> = ArrayVec::default();
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
        }
        Self {
            base: NodeBase::default(),
            children: children,
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
        let mut children: ArrayVec<[AtomicPtr<Node<K, V>>; 256]> = ArrayVec::default();
        for i in 0..children.len() {
            children[i] = AtomicPtr::default();
            children[i].store(self.children[i].load(Ordering::SeqCst), Ordering::SeqCst);
        }
        let mut base = self.base.clone();
        base.ts = LOCAL_TS.with(|ts| *ts.borrow());
        Self {
            base: base,
            children: children,
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
            let mut node = unsafe { Box::from_raw(node_ptr) };
            node.destory();
        }
    }
}

impl<K: AsRef<[u8]>, V: Clone> Art<K, V> {
    pub fn get(&self, key: &K) -> Option<V> {
        let mut depth = 0;
        let node_ptr = self.root.load(Ordering::SeqCst);
        if node_ptr.is_null() {
            return None;
        }
        let mut node_ref = unsafe { &*node_ptr };
        loop {
            match node_ref {
                Node::L(Leaf(_key, _val)) => {
                    if _key.as_ref() == key.as_ref() {
                        return Some(_val.clone());
                    } else {
                        break;
                    }
                }
                Node::B(branch) => {
                    let base = branch.get_base();
                    let prefix_len = base.prefix_match_len(key, depth);
                    if prefix_len != base.prefix.len() {
                        return None;
                    } else {
                        depth += prefix_len;
                    }
                    assert!(depth < key.as_ref().len(), "don't support prefix eq key");
                    if let Some(_node_ref) = branch.find_child(key.as_ref()[depth]) {
                        node_ref = _node_ref;
                        depth += 1;
                    } else {
                        break;
                    }
                }
            }
        }
        None
    }

    // copy path
    pub fn insert(&mut self, key: K, val: V, old_ptrs: &mut HashSet<*mut Node<K, V>>) {
        let mut depth = 0;
        let mut node_ptr = self.root.load(Ordering::SeqCst);
        if node_ptr.is_null() {
            let leaf = Node::L(Leaf(key, val));
            self.root
                .store(Box::into_raw(Box::new(leaf)), Ordering::SeqCst);
            return;
        }
        let mut node_ref = unsafe { &*node_ptr };
        let mut atomic_ptr = &self.root;
        loop {
            match node_ref {
                Node::L(Leaf(_key, _val)) => {
                    let longest_common_prefix = longest_common_prefix(&key, _key, depth);
                    let prefix = _key.as_ref()[depth..depth + longest_common_prefix].to_vec();
                    depth += longest_common_prefix;
                    let nodebase = NodeBase::new(prefix);
                    let mut entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 4]> =
                        ArrayVec::default();
                    for i in 0..entrys.len() {
                        entrys[i].1 = AtomicPtr::default();
                    }
                    entrys[0].0 = _key.as_ref()[depth];
                    entrys[0].1.store(node_ptr, Ordering::SeqCst);
                    entrys[1].0 = key.as_ref()[depth];
                    let leaf = Node::L(Leaf(key, val));
                    entrys[1]
                        .1
                        .store(Box::into_raw(Box::new(leaf)), Ordering::SeqCst);
                    entrys.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
                    let node4 = Node4 {
                        base: nodebase,
                        entrys: entrys,
                    };
                    atomic_ptr.store(
                        Box::into_raw(Box::new(Node::B(Branch::N4(node4)))),
                        Ordering::SeqCst,
                    );
                    return;
                }
                Node::B(branch) => {
                    let mut branch_ref = branch;
                    if !branch_ref.is_dirty() {
                        let new_branch = branch.clone();
                        old_ptrs.insert(node_ptr);
                        node_ptr = Box::into_raw(Box::new(Node::B(new_branch)));
                        atomic_ptr.store(node_ptr, Ordering::SeqCst);
                        node_ref = unsafe { &*node_ptr };
                        branch_ref = node_ref.get_branch();
                    }
                    let base = branch_ref.get_base();
                    let prefix_len = base.prefix_match_len(&key, depth);
                    if prefix_len == base.prefix.len() {
                        depth += prefix_len;
                        let byte = key.as_ref()[depth];
                        if let Some(_atomic_ptr) = branch_ref.find_child_ptr(byte) {
                            // recursion find next
                            atomic_ptr = _atomic_ptr;
                            node_ptr = atomic_ptr.load(Ordering::SeqCst);
                            node_ref = unsafe { &*node_ptr };
                        } else {
                            let leaf_ptr = Box::into_raw(Box::new(Node::L(Leaf(key, val))));
                            node_ptr.add_children(byte, leaf_ptr, atomic_ptr);
                            return;
                        }
                    } else {
                        let prefix = key.as_ref()[depth..depth + prefix_len].to_vec();
                        depth += prefix_len;
                        let nodebase = NodeBase::new(prefix);
                        let mut entrys: ArrayVec<[(u8, AtomicPtr<Node<K, V>>); 4]> =
                            ArrayVec::default();
                        for i in 0..entrys.len() {
                            entrys[i].1 = AtomicPtr::default();
                        }
                        let mut keys = [0; 4];
                        keys[0] = base.prefix[depth];
                        keys[1] = key.as_ref()[depth];
                        entrys[0].0 = base.prefix[depth];
                        entrys[0].1.store(node_ptr, Ordering::SeqCst);
                        entrys[1].0 = key.as_ref()[depth];
                        let leaf = Node::L(Leaf(key, val));
                        entrys[1]
                            .1
                            .store(Box::into_raw(Box::new(leaf)), Ordering::SeqCst);
                        entrys.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
                        let node4 = Node4 {
                            base: nodebase,
                            entrys: entrys,
                        };
                        atomic_ptr.store(
                            Box::into_raw(Box::new(Node::B(Branch::N4(node4)))),
                            Ordering::SeqCst,
                        );
                        return;
                    }
                }
            }
        }
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
    fn get_base(&self) -> &NodeBase {
        use Branch::*;
        match self {
            N4(node) => &node.base,
            N16(node) => &node.base,
            N48(node) => &node.base,
            N256(node) => &node.base,
        }
    }
    fn get_base_mut(&mut self) -> &mut NodeBase {
        use Branch::*;
        match self {
            N4(node) => &mut node.base,
            N16(node) => &mut node.base,
            N48(node) => &mut node.base,
            N256(node) => &mut node.base,
        }
    }
    fn find_child(&self, byte: u8) -> Option<&Node<K, V>> {
        if let Some(atomic_ptr) = self.find_child_ptr(byte) {
            let ptr = atomic_ptr.load(Ordering::SeqCst);
            if !ptr.is_null() {
                return Some(unsafe { &*ptr });
            }
        }
        None
    }
    fn find_child_ptr(&self, byte: u8) -> Option<&AtomicPtr<Node<K, V>>> {
        use Branch::*;
        match self {
            N4(node) => {
                for i in 0..node.base.children_num as usize {
                    if node.entrys[i].0 == byte {
                        return Some(&node.entrys[i].1);
                    }
                }
            }
            N16(node) => {
                match node.entrys[0..node.base.children_num as usize]
                    .binary_search_by(|(key, _)| key.cmp(&byte))
                {
                    Ok(index) => return Some(&node.entrys[index].1),
                    Err(_) => {}
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
    fn is_dirty(&self) -> bool {
        let current_ts = LOCAL_TS.with(|ts| *ts.borrow());
        current_ts == self.get_base().ts
    }
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
    fn get_branch_mut(&mut self) -> &mut Branch<K, V> {
        match self {
            Node::B(branch) => branch,
            _ => panic!("only has leaf"),
        }
    }
    fn add_children(
        self: *mut Node<K, V>,
        byte: u8,
        node_ptr: *mut Node<K, V>,
        atomic_ptr: &AtomicPtr<Node<K, V>>,
    ) {
        use Branch::*;
        let node_mut = unsafe { &mut *self };
        assert!(!node_mut.is_leaf() && node_mut.get_branch().is_dirty());
        match node_mut {
            Node::L(_) => unreachable!(),
            Node::B(branch) => match branch {
                N4(node) => {
                    if node.base.children_num < 4 {
                        let index = node.base.children_num as usize;
                        node.entrys[index].0 = byte;
                        node.entrys[index].1.store(node_ptr, Ordering::SeqCst);
                        node.entrys.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
                        node.base.children_num += 1;
                    } else {
                        let mut node_16 = Node16::default();
                        for index in 0..4 {
                            node_16.entrys[index].0 = node.entrys[index].0;
                            let node_ptr = node.entrys[index].1.load(Ordering::SeqCst);
                            assert!(!node_ptr.is_null());
                            node_16.entrys[index].1.store(node_ptr, Ordering::SeqCst);
                        }
                        mem::swap(&mut node_16.base, &mut node.base);
                        node_16.entrys[4].0 = byte;
                        node_16.entrys[4].1.store(node_ptr, Ordering::SeqCst);
                        node_16.base.children_num += 1;
                        node_16
                            .entrys
                            .sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
                        atomic_ptr.store(
                            Box::into_raw(Box::new(Node::B(N16(node_16)))),
                            Ordering::SeqCst,
                        );
                        unsafe { Box::from_raw(self) };
                    }
                }
                N16(node) => {
                    if node.base.children_num < 16 {
                        let index = node.base.children_num as usize;
                        node.entrys[index].0 = byte;
                        node.entrys[index].1.store(node_ptr, Ordering::SeqCst);
                        node.entrys.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
                        node.base.children_num += 1;
                    } else {
                        let mut node_48 = Node48::default();
                        for index in 0..16 {
                            let key = node.entrys[index].0;
                            let node_ptr = node.entrys[index].1.load(Ordering::SeqCst);
                            node_48.keys[key as usize] = index as u8;
                            node_48.children[index].store(node_ptr, Ordering::SeqCst);
                        }
                        mem::swap(&mut node_48.base, &mut node.base);
                        node_48.keys[byte as usize] = 48;
                        node_48.children[48].store(node_ptr, Ordering::SeqCst);
                        node_48.base.children_num += 1;
                        atomic_ptr.store(
                            Box::into_raw(Box::new(Node::B(N48(node_48)))),
                            Ordering::SeqCst,
                        );
                        unsafe { Box::from_raw(self) };
                    }
                }
                N48(node) => {
                    if node.base.children_num < 48 {
                        let mut index = 48;
                        for _index in 0..48 {
                            let node_ptr = node.children[index].load(Ordering::SeqCst);
                            if node_ptr.is_null() {
                                index = _index;
                                break;
                            }
                        }
                        assert!(index != 48);
                        node.keys[byte as usize] = index as u8;
                        node.children[index].store(node_ptr, Ordering::SeqCst);
                    } else {
                        let mut node_256 = Node256::default();
                        for key in 0..node.keys.len() {
                            let pos = node.keys[key];
                            if pos != DEL {
                                let node_ptr = node.children[pos as usize].load(Ordering::SeqCst);
                                node_256.children[key as usize].store(node_ptr, Ordering::SeqCst);
                            }
                        }
                        mem::swap(&mut node_256.base, &mut node.base);
                        node_256.children[byte as usize].store(node_ptr, Ordering::SeqCst);
                        node_256.base.children_num += 1;
                        atomic_ptr.store(
                            Box::into_raw(Box::new(Node::B(N256(node_256)))),
                            Ordering::SeqCst,
                        );
                        unsafe { Box::from_raw(self) };
                    }
                }
                N256(node) => {
                    node.children[byte as usize].store(node_ptr, Ordering::SeqCst);
                }
            },
        }
    }

    fn remove_children(
        self: *mut Node<K, V>,
        byte: u8,
        atomic_ptr: &AtomicPtr<Node<K, V>>,
        old_ptrs: &mut HashSet<*mut Node<K, V>>
    ) -> Option<*mut Node<K, V>> {
        use Branch::*;
        let node_mut = unsafe { &mut *self };
        assert!(!node_mut.is_leaf() && node_mut.get_branch().is_dirty());
        match node_mut {
            Node::L(_) => unreachable!(),
            Node::B(branch) => match branch {
                N4(node) => {
                    let mut index = 4;
                    for index in 0..node.base.children_num as usize {
                        if node.entrys[index].0 == byte {
                            break;
                        }
                    }
                    if index == 4 {
                        return None;
                    }
                    node.entrys[index].0 = DEL;
                    let old_ptr = node.entrys[index].1.load(Ordering::SeqCst);
                    node.entrys[index]
                        .1
                        .store(ptr::null_mut(), Ordering::SeqCst);
                    // swap to last
                    node.entrys.swap(index, 3);
                    node.base.children_num -= 1;
                    node.entrys[0..node.base.children_num as usize]
                        .sort_by(|(key1, _), (key2, _)| key1.cmp(key2));
                    if node.base.children_num == 1 {
                        let node_ptr = node.entrys[0].1.load(Ordering::SeqCst);
                        let node_ref = unsafe { &*node_ptr };
                        if node_ref.is_leaf() {
                            atomic_ptr.store(node_ptr, Ordering::SeqCst);
                        } else {
                            let node_mut = unsafe { &mut *node_ptr };
                            let mut prefix = node.base.prefix
                        }
                    }
                    return Some(old_ptr);
                }
                _ => unimplemented!(),
            },
        }
    }
}
