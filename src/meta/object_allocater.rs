use crate::object::ObjectId;
use crate::utils::BitMap;

pub struct ObjectAllocater {
    bitmap: BitMap<u32>,
    last_used: usize,
}

impl ObjectAllocater {
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap % 32 == 0);
        Self {
            bitmap: BitMap::with_capacity(cap),
            last_used: 0,
        }
    }
    pub fn allocate(&mut self) -> Option<ObjectId> {
        if let Some(oid) = self.bitmap.first_zero_with_hint_set(self.last_used) {
            self.last_used = oid;
            Some(oid as u32)
        } else {
            None
        }  
    }
    pub fn free(&mut self, oid: ObjectId) {
        self.bitmap.set_bit(oid as usize, false);
        self.last_used = oid as usize;
    }
    pub fn extend(&mut self, extend: usize) {
        self.bitmap.extend(extend);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_object_allocater() {
        let mut allocater = ObjectAllocater::with_capacity(32);
        for i in 0..32 {
            assert_eq!(allocater.allocate(), Some(i));
        }
        for i in 0..32 {
            allocater.free(i);
        }
        allocater.last_used = 0;
        for i in 0..32 {
            assert_eq!(allocater.allocate(), Some(i));
        }
        allocater.extend(32);
        for i in 32..64 {
            assert_eq!(allocater.allocate(), Some(i));
        }
    }

}
