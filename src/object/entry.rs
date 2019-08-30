use super::{Key, Val, MAX_KEY_SIZE};
use crate::error::TdbError;
use crate::object::{AsObject, Object, ObjectTag};
use crate::storage::{Deserialize, ObjectPos, Serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::io::{Read, Write};
use std::mem;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub val: Val,
    pos: ObjectPos,
}

impl Entry {
    pub fn new(key: Key, val: Val) -> Self {
        assert!(key.len() <= MAX_KEY_SIZE as usize);
        let size = Self::get_header_size() + key.len() + val.len();
        Self {
            key,
            val,
            pos: ObjectPos::new(0, size as u16, ObjectTag::Entry),
        }
    }
    pub fn update(&mut self, val: Val) {
        self.pos.sub_len(self.val.len() as u16);
        self.pos.add_len(val.len() as u16);
        self.val = val;
    }
    pub fn get_key_val(&self) -> (Key, Val) {
        (self.key.clone(), self.val.clone())
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            key: Vec::with_capacity(0),
            val: Vec::with_capacity(0),
            pos: ObjectPos::default(),
        }
    }
}

impl Serialize for Entry {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, TdbError> {
        let mut size = 0;
        // object info
        writer.write_u64::<LittleEndian>(self.pos.0)?;
        size += mem::size_of::<u64>();
        // key len
        writer.write_u8(self.key.len() as u8)?;
        size += mem::size_of::<u8>();
        // key
        writer.write(&self.key)?;
        size += self.key.len();
        // val len
        writer.write_u16::<LittleEndian>(self.val.len() as u16)?;
        size += mem::size_of::<u16>();
        // val
        writer.write(&self.val)?;
        size += self.val.len();
        Ok(size)
    }
}

impl Deserialize for Entry {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        // object pos
        let pos = ObjectPos(reader.read_u64::<LittleEndian>()?);
        // key len
        let key_len: usize = reader.read_u8()?.try_into().unwrap();
        // key
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;
        // val len
        let val_len: usize = reader.read_u16::<LittleEndian>()?.try_into().unwrap();
        // val
        let mut val = vec![0; val_len];
        reader.read_exact(&mut val)?;
        Ok(Entry { key, val, pos })
    }
}

impl AsObject for Entry {
    #[inline]
    fn get_tag(&self) -> ObjectTag {
        ObjectTag::Entry
    }

    #[inline]
    fn get_key(&self) -> &[u8] {
        self.key.as_slice()
    }
    #[inline]
    fn get_ref(obejct_ref: &Object) -> &Self {
        match obejct_ref {
            Object::E(entry) => entry,
            _ => panic!("object isn't entry"),
        }
    }
    #[inline]
    fn get_mut(object_mut: &mut Object) -> &mut Self {
        match object_mut {
            Object::E(entry) => entry,
            _ => panic!("object isn't entry"),
        }
    }
    #[inline]
    fn unwrap(obj: Object) -> Self {
        match obj {
            Object::E(entry) => entry,
            _ => panic!("object isn't entry"),
        }
    }
    #[inline]
    fn is(obejct_ref: &Object) -> bool {
        match obejct_ref {
            Object::E(_) => true,
            _ => false,
        }
    }
    #[inline]
    fn get_pos(&self) -> &ObjectPos {
        &self.pos
    }
    #[inline]
    fn get_pos_mut(&mut self) -> &mut ObjectPos {
        &mut self.pos
    }
    #[inline]
    fn get_header_size() -> usize {
        // obj pos + key len + val len
        mem::size_of::<u64>() + mem::size_of::<u8>() + mem::size_of::<u16>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_entry_serialize_deserialize() {
        // test empty
        let entry0 = Entry::default();
        let mut buf = vec![0; 1024];
        assert!(entry0.serialize(&mut buf.as_mut_slice()).is_ok());
        let entry00 = Entry::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(entry0, entry00);
        // test one
        let entry1 = Entry::new(vec![1, 1, 1], vec![2, 2, 2]);
        assert!(entry1.serialize(&mut buf.as_mut_slice()).is_ok());
        let entry11 = Entry::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(entry1, entry11);
        assert_eq!(entry1.pos.get_len(), 8 + 1 + 2 + 3 + 3);
    }
}
