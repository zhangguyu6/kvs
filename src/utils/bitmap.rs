use std::u32;
pub struct LocalBitMap {
    inner: Vec<u64>,
    free_bits: usize,
}

pub trait AsBitBlock: Copy {
    fn bits() -> usize;
    fn all_zero() -> Self;
    fn all_one() -> Self;
    fn get_bit(&self, pos: usize) -> bool;
    fn set_bit(&mut self, pos: usize, bit: bool);
    fn get_first(&self, start: usize, bit: bool) -> Option<usize>;
    fn ones(&self) -> usize;
    fn zeros(&self) -> usize;
}

impl AsBitBlock for u32 {
    #[inline]
    fn bits() -> usize {
        32
    }

    #[inline]
    fn all_zero() -> Self {
        0
    }

    #[inline]
    fn all_one() -> Self {
        u32::MAX
    }

    #[inline]
    fn get_bit(&self, pos: usize) -> bool {
        self & (1 << pos) == 1 << pos
    }

    #[inline]
    fn set_bit(&mut self, pos: usize, bit: bool) {
        if bit {
            *self |= 1 << pos
        } else {
            *self &= !(1 << pos)
        }
    }

    #[inline]
    fn get_first(&self, start: usize, bit: bool) -> Option<usize> {
        if bit {
            let target = self >> start;
            let tail_zeros = target.trailing_zeros();
            let one_index = tail_zeros as usize + start;
            if one_index == Self::bits() {
                None
            } else {
                Some(one_index)
            }
        } else {
            let target = self >> start;
            let tail_ones = (!target).trailing_zeros();
            let zero_index = tail_ones as usize + start;
            if zero_index == Self::bits() {
                None
            } else {
                Some(zero_index)
            }
        }
    }
    fn ones(&self) -> usize {
        self.count_ones() as usize
    }
    fn zeros(&self) -> usize {
        self.count_zeros() as usize
    }
}

struct BitVec<B> {
    bit_blocks: Vec<B>,
    zero_bits: usize,
    all_bits: usize,
}

impl<B: AsBitBlock> BitVec<B> {
    fn with_capacity(cap: usize) -> Self {
        assert!(cap % B::bits() == 0);
        let mut bit_blocks = Vec::with_capacity(cap / B::bits());
        for _ in 0..bit_blocks.len() {
            bit_blocks.push(B::all_zero())
        }
        Self {
            bit_blocks: bit_blocks,
            zero_bits: cap,
            all_bits: cap,
        }
    }
    #[inline]
    fn get_bit(&self, index: usize) -> bool {
        if index >= self.all_bits {
            panic!("overflow max bit bound")
        }
        let big_index = index / B::bits();
        let small_index = index % B::bits();
        let bit_block = self.bit_blocks[big_index];
        bit_block.get_bit(small_index)
    }

    #[inline]
    fn set_bit(&self, index: usize, bit: bool) {
        if index >= self.all_bits {
            panic!("overflow max bit bound")
        }
        let big_index = index / B::bits();
        let small_index = index % B::bits();
        let mut bit_block = self.bit_blocks[big_index];
        bit_block.set_bit(small_index, bit);
    }

    #[inline]
    fn count_ones(&self) -> usize {
        self.all_bits - self.zero_bits
    }

    #[inline]
    fn count_zeros(&self) -> usize {
        self.zero_bits
    }

    #[inline]
    fn first_zero_with_hint(&self, hint: usize) -> Option<usize> {
        if hint >= self.all_bits {
            panic!("overflow max bit bound")
        }
        if self.count_zeros() == 0 {
            return None;
        }
        let start_index = hint / B::bits();
        let small_index = hint % B::bits();
        let bit_block = self.bit_blocks[start_index];
        if let Some(index) = bit_block.get_first(small_index, false) {
            return Some(index + start_index * B::bits());
        }

        for index in start_index..self.bit_blocks.len() {
            let bit_block = self.bit_blocks[index];
            if bit_block.zeros() != 0 {
                if let Some(_index) = bit_block.get_first(0, false) {
                    return Some(_index + index * B::bits());
                }
            }
        }
        for index in 0..start_index {
            let bit_block = self.bit_blocks[index];
            if bit_block.zeros() != 0 {
                if let Some(_index) = bit_block.get_first(small_index, false) {
                    return Some(_index + index * B::bits());
                }
            }
        }
        None
    }
    #[inline]
    fn first_zero(&self) -> Option<usize> {
        self.first_one_with_hint(0)
    }
    #[inline]
    fn first_one_with_hint(&self, hint: usize) -> Option<usize> {
        if hint >= self.all_bits {
            panic!("overflow max bit bound")
        }
        if self.count_ones() == 0 {
            return None;
        }
        let start_index = hint / B::bits();
        let small_index = hint % B::bits();
        let bit_block = self.bit_blocks[start_index];
        if let Some(index) = bit_block.get_first(small_index, true) {
            return Some(index + start_index * B::bits());
        }

        for index in start_index..self.bit_blocks.len() {
            let bit_block = self.bit_blocks[index];
            if bit_block.ones() != 0 {
                if let Some(_index) = bit_block.get_first(0, true) {
                    return Some(_index + index * B::bits());
                }
            }
        }
        for index in 0..start_index {
            let bit_block = self.bit_blocks[index];
            if bit_block.ones() != 0 {
                if let Some(_index) = bit_block.get_first(small_index, true) {
                    return Some(_index + index * B::bits());
                }
            }
        }
        None
    }
    #[inline]
    fn first_one(&self) -> Option<usize> {
        self.first_one_with_hint(0)
    }
}

impl LocalBitMap {
    pub fn new(size: usize) -> Self {
        assert!(size % 64 == 0);
        let mut inner = Vec::with_capacity(size / 64);
        for _ in 0..size / 64 {
            inner.push(0);
        }
        Self {
            inner: inner,
            free_bits: size,
        }
    }
    #[inline]
    fn find_bit(bits: u64, set: bool) -> Option<usize> {
        if set {
            let tailing_zero_bits = bits.trailing_zeros();
            if tailing_zero_bits != 64 {
                Some(tailing_zero_bits as usize)
            } else {
                None
            }
        } else {
            let tailing_one_bits = (!bits).trailing_zeros();
            if tailing_one_bits != 64 {
                Some(tailing_one_bits as usize)
            } else {
                None
            }
        }
    }
}

pub trait BitMap {
    fn is_full(&self) -> bool;
    fn first_one(&self) -> Option<usize>;
    fn first_one_with_hint(&self, hint: usize) -> Option<usize>;
    fn first_zero(&self) -> Option<usize>;
    fn first_zero_with_hint(&self, hint: usize) -> Option<usize>;
    fn get_bit(&self, index: usize) -> bool;
    fn set_bit(&mut self, index: usize, bit: bool);
}

impl BitMap for LocalBitMap {
    #[inline]
    fn is_full(&self) -> bool {
        self.free_bits == 0
    }

    #[inline]
    fn first_one(&self) -> Option<usize> {
        self.first_one_with_hint(0)
    }

    #[inline]
    fn first_one_with_hint(&self, hint: usize) -> Option<usize> {
        let start_index = hint / 64;
        for index in start_index..self.inner.len() {
            match LocalBitMap::find_bit(self.inner[index], true) {
                Some(offset) => return Some(index * 64 + offset),
                _ => {}
            }
        }
        for index in 0..start_index {
            match LocalBitMap::find_bit(self.inner[index], true) {
                Some(offset) => return Some(index * 64 + offset),
                _ => {}
            }
        }
        None
    }

    #[inline]
    fn first_zero(&self) -> Option<usize> {
        self.first_zero_with_hint(0)
    }

    #[inline]
    fn first_zero_with_hint(&self, hint: usize) -> Option<usize> {
        let start_index = hint / 64;
        for index in start_index..self.inner.len() {
            match LocalBitMap::find_bit(self.inner[index], false) {
                Some(offset) => return Some(index * 64 + offset),
                _ => {}
            }
        }
        for index in 0..start_index {
            match LocalBitMap::find_bit(self.inner[index], false) {
                Some(offset) => return Some(index * 64 + offset),
                _ => {}
            }
        }
        None
    }

    #[inline]
    fn get_bit(&self, index: usize) -> bool {
        let inner_index = index / 64;
        let bit_index = index % 64;
        if inner_index >= self.inner.len() {
            panic!("range overflow");
        }
        let bits = self.inner[inner_index] >> bit_index;
        bits & 0x1 == 0x1
    }

    #[inline]
    fn set_bit(&mut self, index: usize, set: bool) {
        let _index = index / 64;
        let bit_index = index % 64;
        if _index >= self.inner.len() {
            panic!("range overflow");
        }
        let mut bit_flag = 0x1 << bit_index;
        if !set {
            bit_flag = !bit_flag;
        }
        if set {
            self.inner[_index] |= bit_flag;
            self.free_bits -= 1;
        } else {
            self.inner[_index] &= bit_flag;
            self.free_bits += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_asbitblock_u32() {
        let a = 0b1000;
        assert_eq!(a.get_bit(3), true);
        let mut a = 1 << 31;
        assert_eq!(a.get_bit(31), true);
        assert_eq!(a.get_bit(30), false);
        a.set_bit(30, true);
        assert_eq!(a.get_bit(30), true);
        assert_eq!(a.get_first(0, true), Some(30));
        assert_eq!(a.get_first(0, false), Some(0));
        assert_eq!(a.get_first(30, true), Some(30));
        assert_eq!(a.get_first(31, true), Some(31));
        assert_eq!(a.get_first(31, false), None);
    }
    #[test]
    fn test_bitmap_first_one() {
        let mut bitmap = LocalBitMap::new(512);
        assert!(!bitmap.is_full());
        assert_eq!(bitmap.first_one(), None);
        bitmap.inner[0] = 0b1;
        assert_eq!(bitmap.first_one(), Some(0));
        bitmap.inner[0] = 0b10;
        assert_eq!(bitmap.first_one(), Some(1));
        assert_eq!(bitmap.first_one_with_hint(1), Some(1));
        bitmap.inner[0] = 0x1 << 62;
        assert_eq!(bitmap.first_one_with_hint(1), Some(62));
        bitmap.inner[0] = 0;
        bitmap.inner[1] = 0b1;
        assert_eq!(bitmap.first_one_with_hint(2), Some(64));
        bitmap.inner[1] = 0b1;
    }

    #[test]
    fn test_bitmap_first_zero() {
        let mut bitmap = LocalBitMap::new(512);
        assert_eq!(bitmap.first_zero(), Some(0));
        bitmap.inner[0] = 0b1;
        assert_eq!(bitmap.first_zero(), Some(1));
        assert_eq!(bitmap.first_zero_with_hint(64), Some(64));
        bitmap.inner[0] = !0;
        assert_eq!(bitmap.first_zero(), Some(64));
        for i in 0..7 {
            bitmap.inner[i] = !0;
        }
        bitmap.inner[7] = 0b011;
        assert_eq!(bitmap.first_zero(), Some(64 * 7 + 2));
    }

    #[test]
    fn test_bitmap_get() {
        let mut bitmap = LocalBitMap::new(512);
        bitmap.inner[0] = 0b1;
        assert_eq!(bitmap.get_bit(0), true);
        assert_eq!(bitmap.get_bit(1), false);
        bitmap.inner[0] = 0b1000;
        assert_eq!(bitmap.get_bit(3), true);
        assert_eq!(bitmap.get_bit(2), false);
        bitmap.inner[1] = 0b1;
        assert_eq!(bitmap.get_bit(64), true);
        assert_eq!(bitmap.get_bit(65), false);
    }

    #[test]
    fn test_bitmap_set() {
        let mut bitmap = LocalBitMap::new(512);
        bitmap.set_bit(511, true);
        assert_eq!(bitmap.inner[7], 1 << 63);
        assert_eq!(bitmap.get_bit(511), true);
    }

}
