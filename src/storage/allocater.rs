use std::intrinsics::{ctpop, cttz};
use std::mem;

struct LocalBitMap {
    inner: Vec<u64>,
    free_num: usize,
    next_zero: usize,
}

pub trait BitMap {
    fn is_full(&self) -> bool;
    fn first_one(&self) -> Option<usize>;
    fn first_one_start(&self, start: usize) -> Option<usize>;
    fn first_zero(&self) -> Option<usize>;
    fn first_zero_start(&self, start: usize) -> Option<usize>;
    fn get_bit(&self, index: usize) -> bool;
    fn set_bit(&mut self, index: usize, bit: bool);
}

impl BitMap for LocalBitMap {
    #[inline]
    fn is_full(&self) -> bool {
        for bits in self.inner.iter() {
            if ctpop(*bits) as usize != mem::size_of::<u64>() {
                return false;
            }
        }
        true
    }

    #[inline]
    fn first_one(&self) -> Option<usize> {
        self.first_one_start(0)
    }

    #[inline]
    fn first_one_start(&self, start: usize) -> Option<usize> {
        let start_index = start / mem::size_of::<u64>();
        for index in start_index..self.inner.len() {
            let bits = self.inner[index];
            let used_bits = cttz(!bits);
            if used_bits as usize != mem::size_of::<u64>() {
                return Some(index * mem::size_of::<u64>() + used_bits as usize);
            }
        }
        None
    }

    #[inline]
    fn first_zero(&self) -> Option<usize> {
        self.first_zero_start(0)
    }

    #[inline]
    fn first_zero_start(&self, start: usize) -> Option<usize> {
        let start_index = start / mem::size_of::<u64>();
        for index in start_index..self.inner.len() {
            let bits = self.inner[index];
            let used_bits = cttz(bits);
            if used_bits as usize != mem::size_of::<u64>() {
                return Some(index * mem::size_of::<u64>() + used_bits as usize);
            }
        }
        None
    }

    #[inline]
    fn get_bit(&self, index: usize) -> bool {
        let _index = index / mem::size_of::<u64>();
        let bit_index = index % mem::size_of::<u64>();
        if _index >= self.inner.len() {
            panic!("range overflow");
        }
        let bit_flag = if bit_index == 0 {
            0x1
        } else {
            0x1 << (bit_index - 1)
        };
        if self.inner[_index] & bit_flag == bit_flag {
            true
        } else {
            false
        }
    }

    #[inline]
    fn set_bit(&mut self, index: usize, set: bool) {
        let _index = index / mem::size_of::<u64>();
        let bit_index = index % mem::size_of::<u64>();
        if _index >= self.inner.len() {
            panic!("range overflow");
        }
        let mut bit_flag = if bit_index == 0 {
            0x1
        } else {
            0x1 << (bit_index - 1)
        };
        if !set {
            bit_flag = !bit_flag;
        }
        if set {
            self.inner[_index] |= bit_flag;
        } else {
            self.inner[_index] &= bit_flag;
        }
    }
}
