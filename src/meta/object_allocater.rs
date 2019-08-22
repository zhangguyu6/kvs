use crate::meta::{OBJECT_NUM, OBJECT_TABLE_ENTRY_PRE_PAGE};
use crate::object::ObjectId;
use crate::utils::BitMap;

pub struct ObjectAllocater {
    pub bitmap: BitMap<u32>,
    last_used: usize,
    data_log_remove_len: u64,
    data_log_len: u64,
}

impl ObjectAllocater {
    pub fn new(obj_tablepage_nums:u32, data_log_remove_len: u64, data_log_len: u64) -> Self {
        Self {
            bitmap: BitMap::with_capacity(OBJECT_NUM),
            last_used: 0,
            data_log_remove_len,
            data_log_len,
        }
    }
    pub fn allocate_oid(&mut self) -> Option<ObjectId> {
        if let Some(oid) = self.bitmap.first_zero_with_hint_set(self.last_used) {
            self.last_used = oid;
            Some(oid as u32)
        } else {
            None
        }
    }
    pub fn free_oid(&mut self, oid: ObjectId) {
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
        let mut allocater = ObjectAllocater::new(0, 0);
        for i in 0..32 {
            assert_eq!(allocater.allocate_oid(), Some(i));
        }
        for i in 0..32 {
            allocater.free_oid(i);
        }
        allocater.last_used = 0;
        for i in 0..32 {
            assert_eq!(allocater.allocate_oid(), Some(i));
        }
        allocater.extend(32);
        for i in 32..64 {
            assert_eq!(allocater.allocate_oid(), Some(i));
        }
    }

}
