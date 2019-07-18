use std::{
    mem,
    ops::Index,
    sync::atomic::{AtomicUsize, Ordering},
};
struct ConcurrentBitMap {
    inner: Vec<AtomicUsize>,
    last_used_index: AtomicUsize,
    free_count: AtomicUsize,
}

impl ConcurrentBitMap {
    pub(crate) fn is_empty(&self) -> bool {
        self.free_count.load(Ordering::SeqCst) == self.free_count.load(Ordering::SeqCst)
    }
    pub(crate) fn check_bit(&self, index: usize) -> (usize, bool) {
        let first_index = index / mem::size_of::<usize>();
        let second_index = index - first_index * mem::size_of::<usize>();
        let vals = self[first_index].load(Ordering::SeqCst);
        (vals, (vals & 0x1 << second_index) == 0x1)
    }
    pub(crate) fn set_bit(&self, index: usize, old_vals: usize, val: bool) -> (usize, bool) {
        let first_index = index / mem::size_of::<usize>();
        let second_index = index - first_index * mem::size_of::<usize>();
        let vals = if val {
            old_vals & 0x1 << second_index
        } else {
            old_vals & 0x0 << second_index
        };
        let pre_vals = self[first_index].compare_and_swap(old_vals, vals, Ordering::SeqCst);
        (pre_vals, old_vals == pre_vals)
    }
}

impl Index<usize> for ConcurrentBitMap {
    type Output = AtomicUsize;
    fn index(&self, index: usize) -> &Self::Output {
        self.inner.get(index).unwrap()
    }
}
