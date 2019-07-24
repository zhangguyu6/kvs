use crate::error::TdbError::{self, *};
use std::mem::{self, MaybeUninit};
use std::ops::Range;
use std::result;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::thread::yield_now;

const SPLIN_COUNT: usize = 60;
const MAX_PREFIXLEN: usize = 8;

struct OptimisticLock(AtomicU64);

type Result<T> = result::Result<T, TdbError>;

impl OptimisticLock {
    #[inline]
    fn read_lock(&self) -> Result<u64> {
        let version = self.wait_unlock();
        if version & 1 == 1 {
            Err(Restart)
        } else {
            Ok(version)
        }
    }

    #[inline]
    fn check_lock(&self, version: u64) -> Result<()> {
        self.read_unlock(version)
    }

    #[inline]
    fn read_unlock(&self, version: u64) -> Result<()> {
        if version == self.0.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Restart)
        }
    }

    #[inline]
    fn read_unlock_other(&self, version: u64, other: &Self) -> Result<()> {
        if version != self.0.load(Ordering::SeqCst) {
            self.write_unlock();
            Err(Restart)
        } else {
            Ok(())
        }
    }

    #[inline]
    fn update_to_write(&self, version: u64) -> Result<()> {
        if self
            .0
            .compare_and_swap(version, version + 2, Ordering::SeqCst)
            == version
        {
            Ok(())
        } else {
            Err(Restart)
        }
    }

    #[inline]
    fn update_to_write_other(&self, version: u64, other: &Self) -> Result<()> {
        if self
            .0
            .compare_and_swap(version, version + 2, Ordering::SeqCst)
            == version
        {
            Ok(())
        } else {
            self.write_unlock();
            Err(Restart)
        }
    }

    #[inline]
    fn write_lock(&self) -> Result<()> {
        loop {
            let version = self.read_lock()?;
            if self.update_to_write(version).is_ok() {
                break;
            }
        }
        Ok(())
    }

    #[inline]
    fn write_unlock(&self) {
        self.0.fetch_add(2, Ordering::SeqCst);
    }

    #[inline]
    fn write_unlock_obsolete(&self) {
        self.0.fetch_add(3, Ordering::SeqCst);
    }

    #[inline]
    fn wait_unlock(&self) -> u64 {
        let mut version = self.0.load(Ordering::SeqCst);
        let mut count = 0;
        while version & 2 == 2 {
            if count >= SPLIN_COUNT {
                yield_now();
                count = 0;
            }
            count += 1;
            version = self.0.load(Ordering::SeqCst);
        }
        version
    }
}

// enum Node<K,V> {}

enum Node<K, V> {
    N4(Node4<K, V>),
    N16(Node16<K, V>),
    N48(Node48<K, V>),
    N256(Node256<K, V>),
    Leaf(K, V),
}

struct NodeBase {
    children_num: u8,
    lock: OptimisticLock,
    prefix_len: u8,
    prefix: [u8; MAX_PREFIXLEN],
}

impl Default for NodeBase {
    fn default() -> Self {
        Self {
            children_num: 0,
            lock: OptimisticLock(AtomicU64::new(0)),
            prefix_len: 0,
            prefix: [0; MAX_PREFIXLEN],
        }
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

impl<K: AsRef<[u8]>, V: Clone> Art<K, V> {
    pub fn get(&self, key: K) -> Option<V> {
        unimplemented!()
    }
    pub fn insert(&self, key: K, val: V) -> Option<V> {
        unimplemented!()
    }
    pub fn remove(&self, key: K) -> Option<V> {
        unimplemented!()
    }
    pub fn range<Iter: Iterator>(&self, range: Range<K>) -> Iter {
        unimplemented!()
    }
}
