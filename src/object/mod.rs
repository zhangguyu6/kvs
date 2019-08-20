mod object_ref;
mod object_log;
 
pub use object_ref::{ObjectRef, Versions};

use crate::error::TdbError;
use crate::tree::{Branch, Entry, Leaf};
use crate::storage::{Serialize,Deserialize};
use std::mem;
use std::sync::Arc;
use std::u32;

// Entry less than 2M
pub const OBJECT_MAX_SIZE: usize = (1 << 21) as usize;
pub const UNUSED_OID: u32 = u32::MAX;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Object {
    L(Leaf),
    B(Branch),
    E(Entry),
}

impl Object {
    #[inline]
    pub fn get_ref<T: AsObject>(&self) -> &T {
        T::get_ref(self)
    }

    #[inline]
    pub fn get_mut<T: AsObject>(&mut self) -> &mut T {
        T::get_mut(self)
    }
    #[inline]
    pub fn get_key(&self) -> &[u8] {
        match self {
            Object::L(leaf) => leaf.get_key(),
            Object::B(branch) => branch.get_key(),
            Object::E(entry) => entry.get_key(),
        }
    }
    #[inline]
    pub fn unwrap<T: AsObject>(self) -> T {
        T::unwrap(self)
    }
    #[inline]
    pub fn is<T: AsObject>(&self) -> bool {
        T::is(self)
    }
    #[inline]
    pub fn get_object_info(&self) -> &ObjectInfo {
        match self {
            Object::L(leaf) => leaf.get_object_info(),
            Object::B(branch) => branch.get_object_info(),
            Object::E(entry) => entry.get_object_info(),
        }
    }
    #[inline]
    pub fn get_object_info_mut(&mut self) -> &mut ObjectInfo {
        match self {
            Object::L(leaf) => leaf.get_object_info_mut(),
            Object::B(branch) => branch.get_object_info_mut(),
            Object::E(entry) => entry.get_object_info_mut(),
        }
    }
    #[inline]
    pub fn read(buf: &[u8], obj_tag: &ObjectTag) -> Result<Self, TdbError> {
        match obj_tag {
            ObjectTag::Leaf => Ok(Object::L(Leaf::deserialize(buf)?)),
            ObjectTag::Branch => Ok(Object::B(Branch::deserialize(buf)?)),
            ObjectTag::Entry => Ok(Object::E(Entry::deserialize(buf)?)),
        }
    }
    #[inline]
    pub fn write(&self, buf: &mut [u8]) -> Result<(), TdbError> {
        match self {
            Object::L(leaf) => leaf.serialize(buf),
            Object::B(branch) => branch.serialize(buf),
            Object::E(entry) => entry.serialize(buf),
        }
    }
}

pub enum MutObject {
    Readonly(Arc<Object>),
    Dirty(Arc<Object>),
    New(Arc<Object>),
    Del,
}

impl MutObject {
    #[inline]
    pub fn get_ref(&self) -> Option<&Object> {
        match self {
            MutObject::Readonly(obj) => Some(&*obj),
            MutObject::Dirty(obj) => Some(&*obj),
            MutObject::New(obj) => Some(&*obj),
            _ => None,
        }
    }
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut Object> {
        match self {
            MutObject::Dirty(obj) => Some(Arc::get_mut(obj).unwrap()),
            MutObject::New(obj) => Some(Arc::get_mut(obj).unwrap()),
            _ => None,
        }
    }
    #[inline]
    pub fn into_arc(self) -> Option<Arc<Object>> {
        match self {
            MutObject::Readonly(obj) => Some(obj.clone()),
            MutObject::Dirty(obj) => Some(obj.clone()),
            MutObject::New(obj) => Some(obj.clone()),
            _ => None,
        }
    }
    #[inline]
    pub fn to_dirty(self) -> Self {
        match self {
            MutObject::Readonly(obj) => MutObject::Dirty(Arc::new((*obj).clone())),
            _ => panic!("object is not readonly"),
        }
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        match self {
            MutObject::Dirty(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_new(&self) -> bool {
        match self {
            MutObject::New(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_del(&self) -> bool {
        match self {
            MutObject::Del => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_readonly(&self) -> bool {
        match self {
            MutObject::Readonly(_) => true,
            _ => false,
        }
    }
}

pub type ObjectId = u32;

#[repr(u8)]
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ObjectTag {
    Leaf = 0,
    Branch,
    Entry,
}

impl From<u8> for ObjectTag {
    fn from(val: u8) -> Self {
        if val == 0 {
            ObjectTag::Leaf
        } else if val == 1 {
            ObjectTag::Branch
        } else if val == 2 {
            ObjectTag::Entry
        } else {
            unreachable!()
        }
    }
}

impl Into<u8> for ObjectTag {
    fn into(self) -> u8 {
        self as u8
    }
}

// Embed in ob-disk object struct
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ObjectInfo {
    pub oid: ObjectId,
    pub tag: ObjectTag,
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
        let tag = ObjectTag::from(((val & 0xFF00000000) >> 32) as u8);
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


pub trait AsObject: Deserialize + Serialize {
    fn get_tag(&self) -> ObjectTag;
    fn get_key(&self) -> &[u8];
    fn get_ref(obejct_ref: &Object) -> &Self;
    fn get_mut(object_mut: &mut Object) -> &mut Self;
    fn is(obejct_ref: &Object) -> bool;
    fn get_object_info(&self) -> &ObjectInfo;
    fn get_object_info_mut(&mut self) -> &mut ObjectInfo;
    fn get_header_size() -> usize;
    fn get_size(&self) -> usize;
    fn get_maxsize() -> usize;
    fn unwrap(obj: Object) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_obejct_tag() {
        assert_eq!(ObjectTag::Leaf as u8, 0);
        assert_eq!(ObjectTag::Branch as u8, 1);
        assert_eq!(ObjectTag::Entry as u8, 2);
        assert_eq!(ObjectTag::Leaf, ObjectTag::from(0));
        assert_eq!(ObjectTag::Branch, ObjectTag::from(1));
        assert_eq!(ObjectTag::Entry, ObjectTag::from(2));
    }

    #[test]
    fn test_obejct_info() {
        let obj_info = ObjectInfo {
            oid: 1,
            tag: ObjectTag::Leaf,
            size: 1,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 2,
            tag: ObjectTag::Branch,
            size: 4096,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 3,
            tag: ObjectTag::Leaf,
            size: 4096,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 4,
            tag: ObjectTag::Entry,
            size: 40960,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));

        let obj_info = ObjectInfo {
            oid: 4,
            tag: ObjectTag::Entry,
            size: OBJECT_MAX_SIZE,
        };
        let val: u64 = obj_info.clone().into();
        assert_eq!(obj_info, ObjectInfo::from(val));
    }
}
