use std::u32;
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
            if one_index >= Self::bits() {
                None
            } else {
                Some(one_index)
            }
        } else {
            let target = self >> start;
            let tail_ones = (!target).trailing_zeros();
            let zero_index = tail_ones as usize + start;
            if zero_index >= Self::bits() {
                None
            } else {
                Some(zero_index)
            }
        }
    }
    // TODO ADD get last
    #[inline]
    fn ones(&self) -> usize {
        self.count_ones() as usize
    }
    #[inline]
    fn zeros(&self) -> usize {
        self.count_zeros() as usize
    }
}

#[derive(Debug)]
pub struct BitMap<B = u32> {
    bit_blocks: Vec<B>,
    all_bits: usize,
}

impl<B: AsBitBlock> BitMap<B> {
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap % B::bits() == 0);
        let mut bit_blocks = Vec::with_capacity(cap / B::bits());
        for _ in 0..bit_blocks.capacity() {
            bit_blocks.push(B::all_zero())
        }
        BitMap {
            bit_blocks: bit_blocks,
            all_bits: cap,
        }
    }
    pub fn extend_to(&mut self, new_len: usize) -> usize {
        assert!(new_len >= self.all_bits && new_len % B::bits() == 0);
        self.bit_blocks.resize(new_len, B::all_zero());
        self.all_bits += new_len;
        self.all_bits
    }

    #[inline]
    pub fn get_bit(&self, index: usize) -> bool {
        if index >= self.all_bits {
            panic!("overflow max bit bound")
        }
        let big_index = index / B::bits();
        let small_index = index % B::bits();
        let bit_block = self.bit_blocks[big_index];
        bit_block.get_bit(small_index)
    }

    #[inline]
    pub fn set_bit(&mut self, index: usize, bit: bool) {
        if index >= self.all_bits {
            panic!("overflow max bit bound")
        }
        let big_index = index / B::bits();
        let small_index = index % B::bits();
        let bit_block = &mut self.bit_blocks[big_index];
        bit_block.set_bit(small_index, bit);
    }

    #[inline]
    fn count_ones(&self) -> usize {
        let mut ones = 0;
        for i in self.bit_blocks.iter() {
            ones += i.ones()
        }
        ones
    }

    #[inline]
    fn count_zeros(&self) -> usize {
        let mut zeros = 0;
        for i in self.bit_blocks.iter() {
            zeros += i.zeros()
        }
        zeros
    }

    #[inline]
    pub fn first_zero_with_hint(&self, hint: usize) -> Option<usize> {
        if self.all_bits == 0 {
            return None;
        }
        if hint >= self.all_bits {
            panic!("overflow max bit bound")
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
    pub fn first_zero_with_hint_set(&mut self, hint: usize) -> Option<usize> {
        if let Some(index) = self.first_zero_with_hint(hint) {
            self.set_bit(index, true);
            Some(index)
        } else {
            None
        }
    }
    #[inline]
    pub fn first_zero(&self) -> Option<usize> {
        self.first_zero_with_hint(0)
    }
    #[inline]
    pub fn last_zero(&self) -> Option<usize> {
        for i in (0..self.bit_blocks.len()).rev() {
            let bits = self.bit_blocks[i];
            if bits.zeros() != 0 {
                for ii in (0..B::bits()).rev() {
                    if !bits.get_bit(ii) {
                        return Some(i * B::bits() + ii);
                    }
                }
            }
        }
        None
    }
    #[inline]
    pub fn first_one_with_hint(&self, hint: usize) -> Option<usize> {
        if self.all_bits == 0 {
            return None;
        }
        if hint >= self.all_bits {
            panic!("overflow max bit bound")
        }
        let start_index = hint / B::bits();
        let small_index = hint % B::bits();
        let bit_block = self.bit_blocks[start_index];
        if let Some(index) = bit_block.get_first(small_index, true) {
            return Some(index + start_index * B::bits());
        }

        for index in start_index + 1..self.bit_blocks.len() {
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
    pub fn first_one_with_hint_set(&mut self, hint: usize) -> Option<usize> {
        if let Some(index) = self.first_one_with_hint(hint) {
            self.set_bit(index, false);
            Some(index)
        } else {
            None
        }
    }
    #[inline]
    pub fn first_one(&self) -> Option<usize> {
        self.first_one_with_hint(0)
    }
    #[inline]
    pub fn last_one(&self) -> Option<usize> {
        for i in (0..self.bit_blocks.len()).rev() {
            let bits = self.bit_blocks[i];
            if bits.ones() != 0 {
                for ii in (0..B::bits()).rev() {
                    if bits.get_bit(ii) {
                        return Some(i * B::bits() + ii);
                    }
                }
            }
        }
        None
    }
    #[inline]
    pub fn get_cap(&self) -> usize {
        self.all_bits
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
        let mut bitmap: BitMap<u32> = BitMap::with_capacity(512);
        assert_eq!(bitmap.first_one(), None);
        bitmap.bit_blocks[0] = 0b1;
        assert_eq!(bitmap.first_one(), Some(0));
        bitmap.bit_blocks[0] = 0b10;
        assert_eq!(bitmap.first_one(), Some(1));
        bitmap.bit_blocks[0] = 0b11;
        assert_eq!(bitmap.first_one_with_hint(1), Some(1));
        bitmap.bit_blocks[0] = 0x1 << 31;
        assert_eq!(bitmap.first_one_with_hint(1), Some(31));
        bitmap.bit_blocks[0] = 0;
        bitmap.bit_blocks[1] = 0b1;
        assert_eq!(bitmap.first_one_with_hint(2), Some(32));
        bitmap.bit_blocks[0] = 0b1;
        bitmap.bit_blocks[1] = 0b1;
        assert_eq!(bitmap.first_one_with_hint(2), Some(32));
        assert_eq!(bitmap.first_one_with_hint(32), Some(32));
        assert_eq!(bitmap.first_one_with_hint(31), Some(32));
    }

    #[test]
    fn test_bitmap_first_zero() {
        let mut bitmap: BitMap<u32> = BitMap::with_capacity(512);
        assert_eq!(bitmap.first_zero(), Some(0));
        bitmap.bit_blocks[0] = 0b1;
        assert_eq!(bitmap.first_zero(), Some(1));
        assert_eq!(bitmap.first_zero_with_hint(32), Some(32));
        bitmap.bit_blocks[0] = !0;
        assert_eq!(bitmap.first_zero(), Some(32));
        for i in 0..7 {
            bitmap.bit_blocks[i] = !0;
        }
        bitmap.bit_blocks[7] = 0b011;
        assert_eq!(bitmap.first_zero(), Some(32 * 7 + 2));
    }

    #[test]
    fn test_bitmap_get_set() {
        let mut bitmap: BitMap<u32> = BitMap::with_capacity(512);
        bitmap.set_bit(0, true);
        assert_eq!(bitmap.count_ones(), 1);
        assert_eq!(bitmap.count_zeros(), 511);
        assert_eq!(bitmap.first_zero_with_hint(0), Some(1));
        assert_eq!(bitmap.first_one_with_hint(0), Some(0));
        assert_eq!(bitmap.get_bit(0), true);
        assert_eq!(bitmap.get_bit(1), false);
        bitmap.set_bit(3, true);
        assert_eq!(bitmap.count_ones(), 2);
        assert_eq!(bitmap.count_zeros(), 510);
        assert_eq!(bitmap.first_zero_with_hint(0), Some(1));
        assert_eq!(bitmap.first_one_with_hint(0), Some(0));
        assert_eq!(bitmap.first_zero_with_hint(3), Some(4));
        assert_eq!(bitmap.first_one_with_hint(3), Some(3));
        assert_eq!(bitmap.get_bit(3), true);
        assert_eq!(bitmap.get_bit(2), false);
        bitmap.set_bit(64, true);
        assert_eq!(bitmap.get_bit(64), true);
        assert_eq!(bitmap.get_bit(65), false);
    }

    #[test]
    fn test_bitmap_extend() {
        let mut bitmap: BitMap<u32> = BitMap::with_capacity(32);
        for i in 0..32 {
            assert_eq!(bitmap.first_zero_with_hint_set(0), Some(i));
        }
        assert_eq!(bitmap.count_zeros(), 0);
        bitmap.extend_to(32);
        assert_eq!(bitmap.first_zero_with_hint_set(31), Some(32));
    }

    #[test]
    fn test_bitmap_get_last() {
        let mut bitmap: BitMap<u32> = BitMap::with_capacity(1024);
        assert_eq!(bitmap.last_one(), None);
        assert_eq!(bitmap.last_zero(), Some(1023));
        bitmap.set_bit(1023, true);
        assert_eq!(bitmap.last_one(), Some(1023));
        assert_eq!(bitmap.last_zero(), Some(1022));
    }
}
