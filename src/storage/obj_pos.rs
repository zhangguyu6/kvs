use crate::object::{ObjectInfo, ObjectTag};
use std::fmt;
use std::io::SeekFrom;
use std::u64;
// [20~63)
pub const MAX_DATABASE_SIZE: u64 = (1 << 44) - 1;
// [4~20)
pub const MAX_OBJECT_SIZE: u64 = (1 << 16) - 1;
// [1~4)
pub const MAX_OBJECT_TAG_SIZE: u64 = (1 << 4) - 1;

#[derive(Eq, PartialEq, Clone, Copy, Hash)]
pub struct ObjectPos(pub u64);

impl fmt::Debug for ObjectPos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObjectPos {{ offset: {:?}, len: {:?} , tag :{:?} }}",
            self.get_pos(),
            self.get_len(),
            self.get_tag()
        )
    }
}

impl Into<SeekFrom> for ObjectPos {
    fn into(self) -> SeekFrom {
        SeekFrom::Start(self.get_pos())
    }
}

impl ObjectPos {
    pub fn new(pos: u64, len: u16, tag: ObjectTag) -> Self {
        let tag: u8 = tag.into();
        Self((pos << 20) + ((len as u64) << 4) + tag as u64)
    }

    pub fn from_info(pos: u64, obj_info: &ObjectInfo) -> Self {
        let len = obj_info.size as u16;
        let tag = obj_info.tag;
        Self::new(pos, len, tag)
    }

    #[inline]
    pub fn get_pos(&self) -> u64 {
        self.0 >> 20
    }

    #[inline]
    pub fn set_pos(&mut self,pos:u64) {
        self.0 = (self.0 & 0xfffff) + (pos << 20);
    }

    #[inline]
    pub fn get_len(&self) -> u16 {
        ((self.0 >> 4) & 0xffff) as u16
    }
    #[inline]
    pub fn set_len(&mut self,size:u16)  {
        self.0 = (self.0 & ((u64::MAX >> 20 << 20) + 0xf)) + ((size as u64) << 4);
    }
    #[inline]
    pub fn add_len(&mut self, size: u16) -> u16 {
        let new_len = ((self.0 >> 4) & 0xffff) as u16 + size;
        self.0 = (self.0 & ((u64::MAX >> 20 << 20) + 0xf)) + ((new_len as u64) << 4);
        new_len
    }

    #[inline]
    pub fn sub_len(&mut self, size: u16) -> u16 {
        let new_len = ((self.0 >> 4) & 0xffff) as u16 - size;
        self.0 = (self.0 & ((u64::MAX >> 20 << 20) + 0xf)) + ((new_len as u64) << 4);
        new_len
    }

    #[inline]
    pub fn get_tag(&self) -> ObjectTag {
        ObjectTag::from((self.0 & 0xf) as u8)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

impl Default for ObjectPos {
    fn default() -> Self {
        Self(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::u16;
    #[test]
    fn test_obj_pos() {
        let obj_pos = ObjectPos::default();
        assert!(obj_pos.is_empty());
        let mut obj_pos = ObjectPos::new(1, 127, ObjectTag::Branch);
        assert_eq!(obj_pos.get_pos(), 1);
        assert_eq!(obj_pos.get_len(), 127);
        assert_eq!(obj_pos.get_tag(), ObjectTag::Branch);
        obj_pos.add_len(1);
        assert_eq!(obj_pos.get_len(), 128);
        assert_eq!(obj_pos.get_pos(), 1);
        assert_eq!(obj_pos.get_tag(), ObjectTag::Branch);
        obj_pos.sub_len(2);
        assert_eq!(obj_pos.get_len(), 126);
        assert_eq!(obj_pos.get_pos(), 1);
        assert_eq!(obj_pos.get_tag(), ObjectTag::Branch);
        let obj_pos = ObjectPos::new(1 << 40, u16::MAX, ObjectTag::Leaf);
        assert_eq!(obj_pos.get_pos(), 1 << 40);
        assert_eq!(obj_pos.get_len(), u16::MAX);
        assert_eq!(obj_pos.get_tag(), ObjectTag::Leaf);
        let mut obj_pos = ObjectPos::new(1, 127, ObjectTag::Entry);
        assert_eq!(obj_pos.get_tag(), ObjectTag::Entry);
    }
}
