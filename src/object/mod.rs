mod branch;
mod entry;
mod leaf;
mod object_ref;
mod object_state;

use crate::error::TdbError;
use crate::storage::{Deserialize, ObjectPos, Serialize};
pub use branch::Branch;
pub use entry::Entry;
pub use leaf::Leaf;
pub use object_ref::{ObjectRef, Versions};
pub use object_state::ObjectState;
use std::io::{Read, Write};
use std::u16;
use std::u32;
use std::u8;

// 255 byte
pub const MAX_KEY_SIZE: u16 = u8::MAX as u16;
pub const MAX_OBJ_SIZE: u16 = u16::MAX;
pub const UNUSED_OID: u32 = u32::MAX;
pub const DATA_ALIGN: usize = 4096;

pub type Key = Vec<u8>;

pub type Val = Vec<u8>;

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
    pub fn get_pos(&self) -> &ObjectPos {
        match self {
            Object::L(leaf) => leaf.get_pos(),
            Object::B(branch) => branch.get_pos(),
            Object::E(entry) => entry.get_pos(),
        }
    }
    #[inline]
    pub fn get_pos_mut(&mut self) -> &mut ObjectPos {
        match self {
            Object::L(leaf) => leaf.get_pos_mut(),
            Object::B(branch) => branch.get_pos_mut(),
            Object::E(entry) => entry.get_pos_mut(),
        }
    }
    #[inline]
    pub fn read<R: Read>(buf: &mut R, obj_tag: &ObjectTag) -> Result<Self, TdbError> {
        match obj_tag {
            ObjectTag::Leaf => Ok(Object::L(Leaf::deserialize(buf)?)),
            ObjectTag::Branch => Ok(Object::B(Branch::deserialize(buf)?)),
            ObjectTag::Entry => Ok(Object::E(Entry::deserialize(buf)?)),
        }
    }
    #[inline]
    pub fn write<W: Write>(&self, buf: &mut W) -> Result<usize, TdbError> {
        match self {
            Object::L(leaf) => leaf.serialize(buf),
            Object::B(branch) => branch.serialize(buf),
            Object::E(entry) => entry.serialize(buf),
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

pub trait AsObject: Deserialize + Serialize {
    fn get_tag(&self) -> ObjectTag;
    fn get_key(&self) -> &[u8];
    fn get_ref(obejct_ref: &Object) -> &Self;
    fn get_mut(object_state: &mut Object) -> &mut Self;
    fn is(obejct_ref: &Object) -> bool;
    fn get_pos(&self) -> &ObjectPos;
    fn get_pos_mut(&mut self) -> &mut ObjectPos;
    fn get_header_size() -> usize;
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
}
