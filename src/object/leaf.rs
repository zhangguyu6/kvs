use super::{Key, MAX_KEY_LEN};
use crate::error::TdbError;
use crate::object::{AsObject, Object, ObjectId, ObjectTag};
use crate::storage::{Deserialize, Serialize,ObjectPos};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::{Read, Write};
use std::mem;
const MAX_LEAF_SIZE: u16 = 4096;
// key + key len + nodeid
const MAX_NONSPLIT_LEAF_SIZE: u16 =
    MAX_LEAF_SIZE - MAX_KEY_LEN - mem::size_of::<ObjectId>() as u16 - mem::size_of::<u8>() as u16;

const REBALANCE_LEAF_SIZE: u16 = MAX_LEAF_SIZE / 4;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Leaf {
    pub entrys: Vec<(Key, ObjectId)>,
    pub pos: ObjectPos,
}

impl Default for Leaf {
    fn default() -> Self {
        Self {
            entrys: Vec::with_capacity(0),
            pos: ObjectPos::new(0,Self::get_header_size() as u16,ObjectTag::Leaf),
        }
    }
}

impl Leaf {
    // Search oid corresponding to key
    // Return oid if find else index for insert
    pub fn search<K: Borrow<[u8]>>(&self, key: &K) -> Result<ObjectId, usize> {
        match self
            .entrys
            .binary_search_by(|_key| _key.0.as_slice().cmp(key.borrow()))
        {
            Ok(index) => Ok(self.entrys[index].1),
            Err(index) => Err(index),
        }
    }
    // Search obj corresponding to key
    // Return index
    pub fn search_index<K: Borrow<[u8]>>(&self, key: &K) -> usize {
        match self
            .entrys
            .binary_search_by(|_key| _key.0.as_slice().cmp(key.borrow()))
        {
            Ok(index) => index,
            Err(index) => index,
        }
    }
    // Insert object to non-full leaf, leaf must be dirty before insert
    pub fn insert_non_full(&mut self, index: usize, key: Key, oid: ObjectId) {
        self.pos.add_len((key.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>()) as u16);
        self.entrys.insert(index, (key, oid));
    }

    // Remove obj corresponding to key
    pub fn remove<K: Borrow<[u8]>>(&mut self, key: &K) -> Option<(Key, ObjectId)> {
        match self
            .entrys
            .binary_search_by(|_key| _key.0.as_slice().cmp(key.borrow()))
        {
            Ok(index) => {
                self.pos.sub_len((
                    key.borrow().len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>()) as u16);
                Some(self.entrys.remove(index))
            }
            Err(_) => None,
        }
    }

    // Split leaf which size bigger than MAX_NONSPLIT_LEAF_SIZE
    // Leaf must be dirty befor split
    // Return split key and split Leaf, solit key is used to insert split Leaf in parent
    pub fn split(&mut self) -> (Key, Self) {
        assert!(self.pos.get_len() > MAX_NONSPLIT_LEAF_SIZE);
        let mut split_index = 0;
        let mut left_size = Self::get_header_size();
        for i in 0..self.entrys.len() {
            left_size += self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>();
            split_index = i;
            if left_size as u16 > MAX_LEAF_SIZE / 2 {
                left_size -=
                    self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>();
                break;
            }
        }
        let right_entrys = self.entrys.split_off(split_index);
        let right_size = self.pos.get_len() - left_size as u16 + Self::get_header_size() as u16;
        let split_key = right_entrys[0].0.clone();
        self.pos.set_len(left_size as u16);
        let mut right_leaf = Leaf::default();
        right_leaf.entrys = right_entrys;
        right_leaf.pos.set_len(right_size);
        (split_key, right_leaf)
    }
    // Merge right leaf if left < REBALANCE_LEAF_SIZE and total size <= MAX_NONSPLIT_LEAF_SIZE
    // right leaf should be marked del after merge
    pub fn merge(&mut self, right_leaf: &mut Leaf) {
        for entry in right_leaf.entrys.iter() {
            self.entrys.push(entry.clone());
        }
        self.pos.sub_len(Self::get_header_size() as u16);
        self.pos.add_len(right_leaf.pos.get_len());
    }
    // Rebalance left and right leaf if left < REBALANCE_LEAF_SIZE and total size > MAX_NONSPLIT_LEAF_SIZE
    // All two left must be dirty
    // return mid key as new key in parrent branch
    pub fn rebalance(&mut self, right_leaf: &mut Leaf) -> Key {
        self.entrys.append(&mut right_leaf.entrys);
        self.pos.sub_len(Self::get_header_size() as u16);
        self.pos.add_len(right_leaf.pos.get_len());
        let mut split_index = 0;
        let mut left_size = Self::get_header_size();
        for i in 0..self.entrys.len() {
            left_size += self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>();
            split_index = i;
            if left_size as u16 > MAX_LEAF_SIZE / 2 {
                left_size -=
                    self.entrys[i].0.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>();;
                break;
            }
        }
        right_leaf.entrys = self.entrys.split_off(split_index);
        right_leaf.pos.add_len(self.pos.get_len() + Self::get_header_size() as u16);
        right_leaf.pos.sub_len(left_size as u16);
        self.pos. set_len(left_size as u16);
        right_leaf.entrys[0].0.clone()
    }
    #[inline]
    pub fn should_split(&self) -> bool {
        self.pos.get_len() as u16 > MAX_NONSPLIT_LEAF_SIZE
    }
    #[inline]
    pub fn should_rebalance_merge(&self) -> bool {
        self.pos.get_len()  < REBALANCE_LEAF_SIZE
    }
    #[inline]
    pub fn should_merge(left_branch: &Leaf, right_branch: &Leaf) -> bool {
        left_branch.pos.get_len() + right_branch.pos.get_len() - Self::get_header_size() as u16
            <= MAX_NONSPLIT_LEAF_SIZE
    }
    #[inline]
    pub fn should_rebalance(left_branch: &Leaf, right_branch: &Leaf) -> bool {
        left_branch.pos.get_len() + right_branch.pos.get_len() - Self::get_header_size() as u16
            > MAX_NONSPLIT_LEAF_SIZE
    }
    #[inline]
    pub fn get_key(&self) -> &Key {
        &self.entrys[0].0
    }
}

impl Serialize for Leaf {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, TdbError> {
        let mut size = 0;
        // object pos
        writer.write_u64::<LittleEndian>(self.pos.0)?;
        size += mem::size_of::<u64>();
        // entrys num
        writer.write_u16::<LittleEndian>(self.entrys.len() as u16)?;
        size += mem::size_of::<u16>();
        // entrys
        for (key, oid) in self.entrys.iter() {
            // key len
            writer.write_u8(key.len() as u8)?;
            size += mem::size_of::<u8>();
            // key
            writer.write(&key)?;
            size += key.len();
            // oid
            writer.write_u32::<LittleEndian>(*oid)?;
            size += mem::size_of::<u32>();
        }
        for _ in size..MAX_LEAF_SIZE as usize {
            writer.write_u8(0)?;
            size += 1
        }
        Ok(size)
    }
}

impl Deserialize for Leaf {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        // object pos
        let pos = ObjectPos(reader.read_u64::<LittleEndian>()?);
        // entrys num
        let entrys_len: usize = reader.read_u16::<LittleEndian>()? as usize;
        let mut entrys = Vec::with_capacity(entrys_len);
        // entrys
        for _ in 0..entrys_len {
            let key_len: usize = reader.read_u8()? as usize;
            let mut key = vec![0; key_len];
            reader.read_exact(&mut key)?;
            let oid = reader.read_u32::<LittleEndian>()? as ObjectId;
            entrys.push((key, oid));
        }
        Ok(Leaf {
            entrys,
            pos,
        })
    }
}

impl AsObject for Leaf {
    #[inline]
    fn get_tag(&self) -> ObjectTag {
        ObjectTag::Leaf
    }
    #[inline]
    fn get_key(&self) -> &[u8] {
        self.entrys[0].0.as_slice()
    }
    #[inline]
    fn get_ref(obejct_ref: &Object) -> &Self {
        match obejct_ref {
            Object::L(leaf) => leaf,
            _ => panic!("object isn't leaf"),
        }
    }
    #[inline]
    fn get_mut(object_mut: &mut Object) -> &mut Self {
        match object_mut {
            Object::L(leaf) => leaf,
            _ => panic!("object isn't leaf"),
        }
    }
    #[inline]
    fn unwrap(obj: Object) -> Self {
        match obj {
            Object::L(leaf) => leaf,
            _ => panic!("object isn't leaf"),
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
    fn get_pos(&self) -> &ObjectPos {
        &self.pos
    }
    #[inline]
    fn get_pos_mut(&mut self) -> &mut ObjectPos {
        &mut self.pos
    }
    #[inline]
    fn get_header_size() -> usize {
        // obj pos + entry num
        mem::size_of::<u64>() + mem::size_of::<u16>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_leaf_serialize_deserialize() {
        // test empty serialize
        let leaf = Leaf::default();
        let mut buf = vec![0; 4096];
        assert!(leaf.serialize(&mut buf.as_mut_slice()).is_ok());
        assert_eq!(leaf, Leaf::deserialize(&mut buf.as_slice()).unwrap());
        // test one
        let mut leaf = Leaf::default();
        leaf.insert_non_full(0, vec![0; 40], 0);
        assert!(leaf.serialize(&mut buf.as_mut_slice()).is_ok());
        assert_eq!(leaf, Leaf::deserialize(&mut buf.as_slice()).unwrap());
        assert_eq!(leaf.pos.get_len(), 8 + 2 + 1 + 40 + 4);
    }

    #[test]
    fn test_leaf_search() {
        let mut leaf = Leaf::default();
        leaf.insert_non_full(0, vec![1; 40], 0);
        assert_eq!(leaf.search(&vec![1; 40]), Ok(0));
        assert_eq!(leaf.search(&vec![2; 40]), Err(1));
        assert_eq!(leaf.search(&vec![0; 40]), Err(0));
    }

    #[test]
    fn test_leaf_split_merge() {
        let mut leaf = Leaf::default();
        for i in 0..100 {
            leaf.insert_non_full(i, vec![i as u8; 40], i as u32);
        }
        assert_eq!(leaf.pos.get_len(), 8 + 2 + 100 * 1 + 100 * 40 + 100 * 4);
        assert!(leaf.should_split());
        let mut leaf1 = leaf.clone();
        let (key, mut leaf11) = leaf1.split();
        assert_eq!(key, vec![45; 40]);
        assert_eq!(leaf1.pos.get_len(), 8 + 2 + 45 * 45);
        leaf1.merge(&mut leaf11);
        assert_eq!(leaf, leaf1);
    }

    #[test]
    fn test_leaf_rebalance() {
        let mut leaf0 = Leaf::default();
        for i in 0..10 {
            leaf0.insert_non_full(i, vec![i as u8; 40], i as u32);
        }
        let mut leaf1 = Leaf::default();
        for i in 0..90 {
            leaf1.insert_non_full(i, vec![(i + 10) as u8; 40], (i + 10) as u32);
        }
        assert!(Leaf::should_rebalance(&leaf0, &leaf1));
        let key = leaf0.rebalance(&mut leaf1);
        assert_eq!(key, vec![45; 40]);
        assert_eq!(leaf0.pos.get_len(), 8 + 2 + 45 * 45);
    }

}
