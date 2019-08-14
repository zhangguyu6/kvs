use super::{Key, Val};
use crate::error::TdbError;
use crate::object::{
    AsObject, Object, ObjectDeserialize, ObjectId, ObjectInfo, ObjectSerialize, ObjectTag,
    UNUSED_OID,
};
use crate::storage::BLOCK_SIZE;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::io::{Read, Write};
use std::mem;
use std::u16;

const MAX_ENTRY_SIZE: usize = u16::MAX as usize * BLOCK_SIZE;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub val: Val,
    pub info: ObjectInfo,
}

impl Entry {
    pub fn new(key: Key, val: Val, oid: ObjectId) -> Self {
        let size = Self::get_header_size() + key.len() + val.len();
        Self {
            key: key,
            val: val,
            info: ObjectInfo {
                oid: oid,
                tag: ObjectTag::Leaf,
                size: size,
            },
        }
    }
    pub fn update(&mut self,val:Val) {
        self.info.size -= self.val.len();
        self.info.size += val.len();
        self.val = val;
    }
} 

impl Default for Entry {
    fn default() -> Self {
        Self {
            key: Vec::with_capacity(0),
            val: Vec::with_capacity(0),
            info: ObjectInfo {
                oid: UNUSED_OID,
                tag: ObjectTag::Leaf,
                size: Self::get_header_size(),
            },
        }
    }
}

impl ObjectSerialize for Entry {
    fn serialize(&self, mut writer: &mut [u8]) -> Result<(), TdbError> {
        assert!(self.get_size() < Self::get_maxsize());
        // object info
        writer.write_u64::<LittleEndian>(self.info.clone().into())?;
        // key len
        writer.write_u16::<LittleEndian>(self.key.len() as u16)?;
        // key
        writer.write(&self.key)?;
        // val len
        writer.write_u32::<LittleEndian>(self.val.len() as u32)?;
        // val
        writer.write(&self.val)?;
        Ok(())
    }
}

impl ObjectDeserialize for Entry {
    fn deserialize(mut reader: &[u8]) -> Result<Self, TdbError> {
        assert!(reader.len() > Self::get_header_size());
        // object info
        let object_info = ObjectInfo::from(reader.read_u64::<LittleEndian>()?);
        // key len
        let key_len: usize = reader.read_u16::<LittleEndian>()?.try_into().unwrap();
        // key
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;
        // val len
        let val_len: usize = reader.read_u32::<LittleEndian>()?.try_into().unwrap();
        // val
        let mut val = vec![0; val_len];
        reader.read_exact(&mut val)?;
        Ok(Entry {
            key: key,
            val: val,
            info: object_info,
        })
    }
}

impl AsObject for Entry {
    #[inline]
    fn get_tag(&self) -> ObjectTag {
        ObjectTag::Entry
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
    fn get_object_info(&self) -> &ObjectInfo {
        &self.info
    }
        #[inline]
    fn get_object_info_mut(&mut self) -> &mut ObjectInfo {
        &mut self.info
    }
    #[inline]
    fn get_header_size() -> usize {
        // object_info + key len + val len
        ObjectInfo::static_size() + mem::size_of::<u16>() + mem::size_of::<u32>()
    }
    #[inline]
    fn get_size(&self) -> usize {
        self.info.size
    }
    #[inline]
    fn get_maxsize() -> usize {
        MAX_ENTRY_SIZE
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
        assert!(entry0.serialize(&mut buf).is_ok());
        let entry00 = Entry::deserialize(&buf).unwrap();
        assert_eq!(entry0, entry00);
        // test one
        let entry1 = Entry::new(vec![1, 1, 1], vec![2, 2, 2], 3);
        assert!(entry1.serialize(&mut buf).is_ok());
        let entry11 = Entry::deserialize(&buf).unwrap();
        assert_eq!(entry1, entry11);
        assert_eq!(entry1.get_size(), 8 + 2 + 4 + 3 + 3);
    }
}
