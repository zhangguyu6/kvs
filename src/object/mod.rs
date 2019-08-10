use crate::error::TdbError;
use crate::tree::{Branch, Leaf
// Entry, Leaf
};
use std::mem;
use std::u32;

pub const OBJECT_MAX_SIZE: usize = (1 << 24 - 1) as usize;
pub const UNUSED_OID:u32 = u32::MAX;

#[derive(PartialEq, Eq,  Clone)]
pub enum Object {
    L(Leaf),
    B(Branch),
    // E(Entry),
}

impl Object {
    #[inline]
    fn get_ref<T: AsObject>(&self) -> &T {
        T::get_ref(self)
    }

    #[inline]
    fn get_mut<T: AsObject>(&mut self) -> &mut T {
        T::get_mut(self)
    }

    #[inline]
    fn is<T: AsObject>(&self) -> bool {
        T::is(self)
    }
}

pub type ObjectId = u32;

#[repr(u8)]
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ObejctTag {
    Leaf = 0,
    Branch,
    Entry,
}

impl From<u8> for ObejctTag {
    fn from(val: u8) -> Self {
        if val == 0 {
            ObejctTag::Leaf
        } else if val == 1 {
            ObejctTag::Branch
        } else if val == 2 {
            ObejctTag::Entry
        } else {
            unreachable!()
        }
    }
}

impl Into<u8> for ObejctTag {
    fn into(self) -> u8 {
        self as u8
    }
}

// Embed in ob-disk object struct
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ObjectInfo {
    pub oid: ObjectId,
    pub tag: ObejctTag,
    pub size: usize,
}

impl ObjectInfo {
    pub fn static_size() -> usize {
        mem::size_of::<u64>()
    }
}

impl From<u64> for ObjectInfo {
    fn from(val: u64) -> Self {
        // low [0~32) bit
        let oid = (val & 0xFFFFFFFF) as u32;
        // [32,40) bit
        let tag = ObejctTag::from(((val & 0xFF00000000) >> 32) as u8);
        // [40,64) bit
        let size = ((val & 0xFFFFFF0000000000) >> 40) as usize;
        Self { oid, tag, size }
    }
}

impl Into<u64> for ObjectInfo {
    fn into(self) -> u64 {
        assert!(self.size <= OBJECT_MAX_SIZE);
        self.oid as u64 + ((self.tag as u8 as u64) << 32) + ((self.size as u64) << 40)
    }
}




pub trait ObjectSerialize {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError>;
}

pub trait ObjectDeserialize: Sized {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError>;
}

pub trait AsObject: ObjectDeserialize + ObjectSerialize {
    fn get_tag(&self) -> ObejctTag;
    fn get_ref(obejct_ref: &Object) -> &Self;
    fn get_mut(object_mut: &mut Object) -> &mut Self;
    fn is(obejct_ref: &Object) -> bool;
    fn get_object_info(&self) -> &ObjectInfo;
    fn get_header_size() -> usize;
    fn get_size(&self) -> usize;
    fn get_maxsize() -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_obejct_tag() {
        assert_eq!(ObejctTag::Leaf as u8, 0);
        assert_eq!(ObejctTag::Branch as u8, 1);
        assert_eq!(ObejctTag::Entry as u8, 2);
        assert_eq!(ObejctTag::Leaf, ObejctTag::from(0));
        assert_eq!(ObejctTag::Branch, ObejctTag::from(1));
        assert_eq!(ObejctTag::Entry, ObejctTag::from(2));
    }

    #[test]
    fn test_obejct_info() {
        let obj_info = ObjectInfo {
            oid: 1,
            tag: ObejctTag::Leaf,
            size: 1,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 2,
            tag: ObejctTag::Branch,
            size: 4096,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 3,
            tag: ObejctTag::Leaf,
            size: 4096,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 4,
            tag: ObejctTag::Entry,
            size: 40960,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 4,
            tag: ObejctTag::Entry,
            size: OBJECT_MAX_SIZE,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));
    }

}
