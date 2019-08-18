use std::io::SeekFrom;

pub const MAX_DATABASE_SIZE: u64 = 1<< 44;
pub const MAX_OBJECT_SIZE:u64 = 1<<20;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ObjectPos(pub u64);

impl Into<SeekFrom> for ObjectPos {
    fn into(self) -> SeekFrom {
        SeekFrom::Start(self.get_pos())
    }
}

impl ObjectPos {
    pub fn new(pos: u64, len: usize) -> Self {
        Self(((pos as u64) << 20) + len as u64)
    }
    #[inline]
    pub fn get_pos(&self) -> u64 {
        self.0 >> 20
    }

    #[inline]
    pub fn get_len(&self) -> usize {
        (self.0 & 0xfffff) as usize
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
