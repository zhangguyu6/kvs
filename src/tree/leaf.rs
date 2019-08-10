use super::{Key, MAX_KEY_LEN};
use crate::error::TdbError;
use crate::object::{
    AsObject, ObejctTag, Object, ObjectDeserialize, ObjectId, ObjectInfo, ObjectSerialize,
    UNUSED_OID,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::{Read, Write};
use std::mem;
use std::u16;

const MAX_LEAF_SIZE: usize = 4096;
// key + key len + nodeid
const MAX_NONSPLIT_LEAF_SIZE: usize =
    MAX_LEAF_SIZE - MAX_KEY_LEN - mem::size_of::<u32>() - mem::size_of::<u16>();

const REBALANCE_LEAF_SIZE: usize = MAX_LEAF_SIZE / 4;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Leaf {
    entrys: Vec<(Key, ObjectId)>,
    info: ObjectInfo,
}

impl Default for Leaf {
    fn default() -> Self {
        Self {
            entrys: Vec::with_capacity(0),
            info: ObjectInfo {
                oid: UNUSED_OID,
                tag: ObejctTag::Leaf,
                size: Leaf::get_header_size(),
            },
        }
    }
}

impl Leaf {
    fn search<K: Borrow<[u8]>>(&self, key: &K) -> Result<ObjectId, usize> {
        match self
            .entrys
            .binary_search_by(|_key| _key.0.as_slice().cmp(key.borrow()))
        {
            Ok(index) => Ok(self.entrys[index].1),
            Err(index) => Err(index),
        }
    }
}
//     fn insert_non_full(&mut self, key: Key, node_id: NodeId) {
//         match self.entrys.binary_search_by(|_key| _key.0.cmp(&key)) {
//             Ok(_) => panic!("insert duplication entry"),
//             Err(index) => {
//                 self.total_size +=
//                     (key.len() + mem::size_of::<u8>() + mem::size_of::<u32>()) as u16;
//                 self.entrys.insert(index, (key, node_id));
//             }
//         }
//     }
//     fn split(&mut self) -> (Key, Node) {
//         assert!(self.total_size as usize > SPLIT_NODE_SIZE);
//         let mut split_index = 0;
//         let mut left_size = Self::get_header_size();
//         for i in 0..self.entrys.len() {
//             left_size += self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<u32>();
//             split_index = i;
//             if left_size > MAX_NODE_SIZE / 2 {
//                 left_size -= self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<u32>();;
//                 break;
//             }
//         }
//         let right_entrys = self.entrys.split_off(split_index);
//         let split_key = right_entrys[0].0.clone();
//         let right_size = self.total_size - left_size as u16 + Self::get_header_size() as u16;
//         self.total_size = left_size as u16;
//         let right_node = Leaf {
//             entrys: right_entrys,
//             total_size: right_size,
//         };
//         (split_key, Node::L(right_node))
//     }
//     fn merge(&mut self, left: &Leaf) {
//         assert!(
//             self.total_size as usize + left.total_size as usize - Leaf::get_header_size()
//                 < SPLIT_NODE_SIZE
//         );
//         for entry in left.entrys.iter() {
//             self.entrys.push(entry.clone());
//         }
//         self.total_size += left.total_size - Self::get_header_size() as u16;
//     }
//     fn rebalance(&mut self, left: &mut Leaf) -> Key {
//         assert!(
//             self.total_size as usize + left.total_size as usize - Self::get_header_size()
//                 > SPLIT_NODE_SIZE
//         );
//         self.entrys.append(&mut left.entrys);
//         self.total_size += left.total_size - Branch::get_header_size() as u16;
//         let mut split_index = 0;
//         let mut left_size = Self::get_header_size();
//         for i in 0..self.entrys.len() {
//             left_size += self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<u32>();
//             split_index = i;
//             if left_size > MAX_NODE_SIZE / 2 {
//                 left_size -= self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<u32>();;
//                 break;
//             }
//         }
//         left.entrys = self.entrys.split_off(split_index);
//         left.total_size = self.total_size - left_size as u16 + Self::get_header_size() as u16;
//         self.total_size = left_size as u16;
//         left.entrys[0].0.clone()
//     }
//     fn should_split(&self) -> bool {
//         (self.total_size as usize) > SPLIT_NODE_SIZE
//     }
//     fn should_rebalance(&self) -> bool {
//         (self.total_size as usize) < REBALANCE_NODE_SIZE
//     }
//     fn get_header_size() -> usize {
//         unimplemented!()
//     }
// }

impl ObjectSerialize for Leaf {
    fn serialize(&self, mut writer: &mut [u8]) -> Result<(), TdbError> {
        assert!(self.get_size() < Self::get_maxsize());
        // object info
        writer.write_u64::<LittleEndian>(self.info.clone().into())?;
        assert!(self.entrys.len() < u16::MAX as usize);
        // entrys num
        writer.write_u16::<LittleEndian>(self.entrys.len() as u16)?;
        // entrys
        for (key, oid) in self.entrys.iter() {
            // key len
            writer.write_u16::<LittleEndian>(key.len() as u16)?;
            // key
            writer.write(&key)?;
            // oid
            writer.write_u32::<LittleEndian>(*oid)?;
        }
        Ok(())
    }
}

impl ObjectDeserialize for Leaf {
    fn deserialize(mut reader: &[u8]) -> Result<Self, TdbError> {
        assert!(reader.len() > Self::get_header_size());
        // object info
        let object_info = ObjectInfo::from(reader.read_u64::<LittleEndian>()?);
        // entrys num
        let entrys_len: usize = reader.read_u16::<LittleEndian>()? as usize;
        let mut entrys = Vec::with_capacity(entrys_len);
        // entrys
        for _ in 0..entrys_len {
            let key_len: usize = reader.read_u16::<LittleEndian>()? as usize;
            let mut key = vec![0; key_len];
            reader.read_exact(&mut key)?;
            let oid = reader.read_u32::<LittleEndian>()? as ObjectId;
            entrys.push((key, oid));
        }
        Ok(Leaf {
            entrys: entrys,
            info: object_info,
        })
    }
}

impl AsObject for Leaf {
    #[inline]
    fn get_tag(&self) -> ObejctTag {
        ObejctTag::Leaf
    }
    #[inline]
    fn get_ref(obejct_ref: &Object) -> &Self {
        match obejct_ref {
            Object::L(leaf) => leaf,
            _ => panic!("object isn't branch"),
        }
    }
    #[inline]
    fn get_mut(object_mut: &mut Object) -> &mut Self {
        match object_mut {
            Object::L(leaf) => leaf,
            _ => panic!("object isn't branch"),
        }
    }
    #[inline]
    fn is(obejct_ref: &Object) -> bool {
        match obejct_ref {
            Object::L(_) => true,
            _ => false,
        }
    }
    #[inline]
    fn get_object_info(&self) -> &ObjectInfo {
        &self.info
    }
    #[inline]
    fn get_header_size() -> usize {
        // object_info + entry num
        ObjectInfo::static_size() + mem::size_of::<u16>()
    }
    #[inline]
    fn get_size(&self) -> usize {
        self.info.size
    }
    #[inline]
    fn get_maxsize() -> usize {
        MAX_LEAF_SIZE
    }
}
