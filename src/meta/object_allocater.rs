use crate::meta::{OBJECT_NUM, OBJECT_TABLE_ENTRY_PRE_PAGE};
use crate::object::{ObjectId, ObjectTag,META_DATA_ALIGN};
use crate::storage::ObjectPos;
use crate::utils::BitMap;

pub struct ObjectAllocater {
    pub bitmap: BitMap<u32>,
    last_used: usize,
    data_log_remove_len: u64,
    data_log_len: u64,
}

impl ObjectAllocater {
    pub fn new(cap: usize, data_log_remove_len: u64, data_log_len: u64) -> Self {
        assert!(cap % 32 == 0);
        Self {
            bitmap: BitMap::with_capacity(cap),
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
    pub fn allocate_obj_pos(&mut self, len: u16, tag: ObjectTag) -> ObjectPos {
        match tag {
            ObjectTag::Branch | ObjectTag::Leaf => { 
                if len as usize % META_DATA_ALIGN != 0 
                { self.data_log_len +=  (META_DATA_ALIGN -  (len as usize % META_DATA_ALIGN)) as u64;
                } 
            
            },
            _ => {}
        }
        let obj_pos = ObjectPos::new(self.data_log_len, len, tag);
        self.data_log_len += len as u64;
        obj_pos
    }

    pub fn free_oid(&mut self, oid: ObjectId) {
        self.bitmap.set_bit(oid as usize, false);
        self.last_used = oid as usize;
    }

    pub fn free_obj_pos(&mut self, obj_pos: ObjectPos) {
        let len = obj_pos.get_len() as u64;
        self.data_log_remove_len += len;
    }

    pub fn set_bit(&mut self, index: usize, used: bool) {
        self.bitmap.set_bit(index, used);
    }

    pub fn extend(&mut self, extend: usize) {
        if self.bitmap.get_cap() >= OBJECT_NUM {
            panic!("obj num more than MAX_OBJECT_NUM ");
        }
        self.bitmap.extend(extend);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_object_allocater() {
        let mut allocater = ObjectAllocater::new(32, 0, 0);
        for i in 0..32 {
            assert_eq!(allocater.allocate_oid(), Some(i));
        }
        assert_eq!(allocater.allocate_oid(), None);
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
        assert_eq!(allocater.allocate_oid(), None);
        for i in 0..32 {
            allocater.free_oid(i);
        }
        for _ in (0..32).rev() {
            assert!(allocater.allocate_oid().is_some());
        }
        let mut allocater = ObjectAllocater::new(0, 0, 0);
        assert_eq!(allocater.allocate_oid(), None);
    }
}
