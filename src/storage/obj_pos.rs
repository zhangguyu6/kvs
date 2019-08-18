use crate::object::ObjectTag;
use std::io::SeekFrom;

// [20~63)
pub const MAX_DATABASE_SIZE: u64 = (1 << 44) - 1;
// [4~20)
pub const MAX_OBJECT_SIZE: u64 = (1 << 16) - 1;
// [0~4)
pub const MAX_OBJECT_TAG_SIZE: u64 = (1 << 4) - 1;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ObjectPos(pub u64);

impl Into<SeekFrom> for ObjectPos {
    fn into(self) -> SeekFrom {
        SeekFrom::Start(self.get_pos())
    }
}

impl ObjectPos {
    pub fn new(pos: u64, len: usize, tag: ObjectTag) -> Self {
        let tag: u8 = tag.into();
        Self((pos << 20) + ((len as u64) << 4) + (tag as u64))
    }

    #[inline]
    pub fn get_pos(&self) -> u64 {
        self.0 >> 20
    }

    #[inline]
    pub fn get_len(&self) -> usize {
        ((self.0 >> 4) & 0xfffff) as usize
    }

    #[inline]
    pub fn get_tag(&self) -> ObjectTag {
        ObjectTag::from((self.0 & 0xf) as u8)
    }
}

impl Default for ObjectPos {
    fn default() -> Self {
        Self(0)
    }
}

impl ObjectPos {
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}
