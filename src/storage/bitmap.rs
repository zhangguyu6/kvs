struct LocalBitMap {
    inner: Vec<u64>,
    free_num: usize,
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
            free_num: size,
        }
    }
    #[inline]
    fn find_bit(&self, bits: u64, set: bool) -> Option<usize> {
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
        self.free_num == 0
    }

    #[inline]
    fn first_one(&self) -> Option<usize> {
        self.first_one_with_hint(0)
    }

    #[inline]
    fn first_one_with_hint(&self, hint: usize) -> Option<usize> {
        let start_index = hint / 64;
        for index in start_index..self.inner.len() {
            match self.find_bit(self.inner[index], true) {
                Some(offset) => return Some(index * 64 + offset),
                _ => {}
            }
        }
        for index in 0..start_index {
            match self.find_bit(self.inner[index], true) {
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
            match self.find_bit(self.inner[index], false) {
                Some(offset) => return Some(index * 64 + offset),
                _ => {}
            }
        }
        for index in 0..start_index {
            match self.find_bit(self.inner[index], false) {
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
            self.free_num -= 1;
        } else {
            self.inner[_index] &= bit_flag;
            self.free_num += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
