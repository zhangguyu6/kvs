use super::{Key, Val, MAX_KEY_LEN};
use crate::error::TdbError;
use crate::object::{AsObject, Object, ObjectId, ObjectInfo, ObjectTag, UNUSED_OID};
use crate::storage::{Deserialize, Serialize, StaticSized, MAX_OBJECT_SIZE};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::convert::TryInto;
use std::io::{Read, Write};
use std::mem;

const MAX_ENTRY_SIZE: usize = MAX_OBJECT_SIZE as usize;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub val: Val,
    pub info: ObjectInfo,
}

impl Entry {
    pub fn new(key: Key, val: Val, oid: ObjectId) -> Self {
        assert!(key.len() <= MAX_KEY_LEN);
        let size = Self::get_header_size()
            + key.len()
            + val.len();
        Self {
            key: key,
            val: val,
            info: ObjectInfo {
                oid: oid,
                tag: ObjectTag::Entry,
                size: size,
            },
        }
    }
    pub fn update(&mut self, val: Val) {
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

impl StaticSized for Entry {
    #[inline]
    fn len(&self) -> usize {
        self.info.size
    }
}

impl Serialize for Entry {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), TdbError> {
        // object info
        writer.write_u64::<LittleEndian>(self.info.clone().into())?;
        // key len
        writer.write_u8(self.key.len() as u8)?;
        // key
        writer.write(&self.key)?;
        // val len
        writer.write_u16::<LittleEndian>(self.val.len() as u16)?;
        // val
        writer.write(&self.val)?;
        Ok(())
    }
}

impl Deserialize for Entry {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        // object info
        let object_info = ObjectInfo::from(reader.read_u64::<LittleEndian>()?);
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
        ObjectInfo::static_size() + mem::size_of::<u8>() + mem::size_of::<u16>()
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
        let entry1 = Entry::new(vec![1, 1, 1], vec![2, 2, 2], 3);
        assert!(entry1.serialize(&mut buf.as_mut_slice()).is_ok());
        let entry11 = Entry::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(entry1, entry11);
        assert_eq!(entry1.len(), 8 + 1 + 2 + 3 + 3);
    }
}
