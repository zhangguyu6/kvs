use super::{Key, MAX_KEY_SIZE};
use crate::error::TdbError;
use crate::object::{AsObject, Object, ObjectId, ObjectTag, DATA_ALIGN};
use crate::storage::{Deserialize, ObjectPos, Serialize};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::{Read, Write};
use std::mem;

const MAX_BRANCH_SIZE: u16 = DATA_ALIGN as u16;
// key + key len + nodeid
const MAX_NONSPLIT_BRANCH_SIZE: u16 = MAX_BRANCH_SIZE
    - MAX_KEY_SIZE
    - mem::size_of::<ObjectId>() as u16
    - mem::size_of::<u8>() as u16;

const REBALANCE_BRANCH_SIZE: u16 = MAX_BRANCH_SIZE / 4;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Branch {
    pub keys: Vec<Key>,
    pub children: Vec<ObjectId>,
    pub pos: ObjectPos,
}

impl Default for Branch {
    fn default() -> Self {
        Self {
            keys: Vec::with_capacity(0),
            children: Vec::with_capacity(0),
            pos: ObjectPos::new(0, Branch::get_header_size() as u16, ObjectTag::Branch),
        }
    }
}
impl Branch {
    pub fn new(key: Key, oid0: ObjectId, oid1: ObjectId) -> Self {
        assert!(key.len() <= MAX_KEY_SIZE as usize);
        let size = Branch::get_header_size()
            + key.len()
            + mem::size_of::<u8>()
            + 2 * mem::size_of::<ObjectId>();
        Self {
            keys: vec![key],
            children: vec![oid0, oid1],
            pos: ObjectPos::new(0, size as u16, ObjectTag::Branch),
        }
    }
    // Return (object,object index) greater or equal to key
    pub fn search<K: Borrow<[u8]>>(&self, key: &K) -> (ObjectId, usize) {
        let index = match self
            .keys
            .binary_search_by(|_key| _key.as_slice().cmp(key.borrow()))
        {
            Ok(index) => index + 1,
            Err(index) => index,
        };
        (self.children[index], index)
    }
    // Remove key at index and oid at index+1
    pub fn remove_index(&mut self, index: usize) -> (Key, ObjectId) {
        let key = self.keys.remove(index);
        let oid = self.children.remove(index + 1);
        self.pos
            .sub_len((key.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>()) as u16);
        (key, oid)
    }
    pub fn update_key(&mut self, index: usize, key: Key) {
        self.pos.sub_len(self.keys[index].len() as u16);
        self.pos.add_len(key.len() as u16);
        self.keys[index] = key;
    }
    // Insert object to non-full branch, branch must be dirty before insert
    pub fn insert_non_full(&mut self, index: usize, key: Key, oid: ObjectId) {
        assert!(key.len() <= MAX_KEY_SIZE as usize);
        // don't use this function for root insert
        assert!(!self.children.is_empty());
        self.pos
            .add_len((key.len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>()) as u16);
        self.keys.insert(index, key);
        self.children.insert(index + 1, oid);
    }
    // Split branch whuch size biggher than MAX_NONSPLIT_BRANCH_SIZE
    // Branch must be dirty befor split
    // Return split key and split Branch, solit key is used to insert split Branch in parent
    pub fn split(&mut self) -> (Key, Self) {
        let mut split_index = 0;
        let mut left_size = Self::get_header_size();
        for i in 0..self.keys.len() {
            left_size += self.keys[i].len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>();
            split_index = i;
            if left_size as u16 > MAX_BRANCH_SIZE / 2 {
                // mid key will be remove and insert to parent branch
                break;
            }
        }
        let right_keys = self.keys.split_off(split_index + 1);
        let right_children = self.children.split_off(split_index + 1);
        let split_key = self.keys.pop().unwrap();
        let right_size = self.pos.get_len() - left_size as u16 + Self::get_header_size() as u16;
        // children num is keys + 1
        left_size -= split_key.len() + mem::size_of::<u8>();
        self.pos.set_len(left_size as u16);
        let mut right_branch = Branch::default();
        right_branch.children = right_children;
        right_branch.keys = right_keys;
        right_branch.pos.set_len(right_size);
        (split_key, right_branch)
    }
    // Merge right branch if left < REBALANCE_BRANCH_SIZE and total size <=  MAX_NONSPLIT_BRANCH_SIZE
    // right_branch should be marked del after merge
    // merge_key is the key of right_branch's first child
    pub fn merge(&mut self, right_branch: &mut Branch, merge_key: Key) {
        self.pos
            .add_len((merge_key.len() + mem::size_of::<u8>()) as u16);
        self.keys.push(merge_key);
        self.keys.append(&mut right_branch.keys);
        self.children.append(&mut right_branch.children);
        self.pos.add_len(right_branch.pos.get_len());
        self.pos.sub_len(Branch::get_header_size() as u16);
    }
    // Rebalance left and right branch if left < REBALANCE_BRANCH_SIZE and total size > MAX_NONSPLIT_BRANCH_SIZE
    // All two branch must be dirty
    // rebalance_key is the key of right_branch's first child
    // return remove key as new key in parrent branch
    pub fn rebalance(&mut self, right_branch: &mut Branch, rebalance_key: Key) -> Key {
        self.pos
            .add_len((rebalance_key.len() + mem::size_of::<u8>()) as u16);
        self.keys.push(rebalance_key);
        self.keys.append(&mut right_branch.keys);
        self.children.append(&mut right_branch.children);
        self.pos.add_len(right_branch.pos.get_len());
        self.pos.sub_len(Branch::get_header_size() as u16);
        let mut split_index = 0;
        let mut left_size = Self::get_header_size();
        for i in 0..self.keys.len() {
            left_size += self.keys[i].len() + mem::size_of::<u8>() + mem::size_of::<ObjectId>();
            split_index = i;
            if left_size as u16 > MAX_BRANCH_SIZE / 2 {
                break;
            }
        }
        right_branch.keys = self.keys.split_off(split_index + 1);
        right_branch.children = self.children.split_off(split_index + 1);
        right_branch
            .pos
            .set_len(self.pos.get_len() - left_size as u16 + Self::get_header_size() as u16);
        let remove_key = self.keys.pop().unwrap();
        self.pos
            .set_len((left_size - remove_key.len() - mem::size_of::<u8>()) as u16);
        remove_key
    }
    #[inline]
    pub fn should_split(&self) -> bool {
        self.pos.get_len() > MAX_NONSPLIT_BRANCH_SIZE
    }
    #[inline]
    pub fn should_rebalance_merge(&self) -> bool {
        self.pos.get_len() < REBALANCE_BRANCH_SIZE
    }
    #[inline]
    pub fn should_merge(left_branch: &Branch, right_branch: &Branch) -> bool {
        left_branch.pos.get_len() + right_branch.pos.get_len() - Branch::get_header_size() as u16
            <= MAX_NONSPLIT_BRANCH_SIZE
    }
    #[inline]
    pub fn should_rebalance(left_branch: &Branch, right_branch: &Branch) -> bool {
        left_branch.pos.get_len() + right_branch.pos.get_len() - Branch::get_header_size() as u16
            > MAX_NONSPLIT_BRANCH_SIZE
    }
    #[inline]
    pub fn get_key(&self) -> &Key {
        &self.keys[0]
    }
}

impl Serialize for Branch {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<usize, TdbError> {
        let mut size = 0;
        // object pos
        writer.write_u64::<LittleEndian>(self.pos.0)?;
        size += mem::size_of::<u64>();
        // keys num
        writer.write_u8(self.keys.len() as u8)?;
        size += mem::size_of::<u8>();
        // keys
        for key in self.keys.iter() {
            writer.write_u8(key.len() as u8)?;
            size += mem::size_of::<u8>();
            writer.write(&key)?;
            size += key.len();
        }
        // children num
        writer.write_u8(self.children.len() as u8)?;
        size += mem::size_of::<u8>();
        // children
        for child in self.children.iter() {
            writer.write_u32::<LittleEndian>(*child)?;
            size += mem::size_of::<u32>();
        }
        // align to 4K
        for _ in size..MAX_BRANCH_SIZE as usize {
            writer.write_u8(0)?;
            size += 1;
        }
        Ok(size)
    }
}

impl Deserialize for Branch {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, TdbError> {
        // object pos
        let pos = ObjectPos(reader.read_u64::<LittleEndian>()?);
        // keys num
        let keys_len: usize = reader.read_u8()? as usize;
        let mut keys = Vec::with_capacity(keys_len);
        // keys
        for _ in 0..keys_len {
            let key_len: usize = reader.read_u8()? as usize;
            let mut key = vec![0; key_len];
            reader.read_exact(&mut key)?;
            keys.push(key);
        }
        // children num
        let children_len: usize = reader.read_u8()? as usize;
        let mut children = Vec::with_capacity(children_len);
        // children
        for _ in 0..children_len {
            let oid = reader.read_u32::<LittleEndian>()? as ObjectId;
            children.push(oid);
        }
        Ok(Branch {
            keys,
            children,
            pos,
        })
    }
}

impl AsObject for Branch {
    #[inline]
    fn get_tag(&self) -> ObjectTag {
        ObjectTag::Branch
    }
    #[inline]
    fn get_key(&self) -> &[u8] {
        self.keys[0].as_slice()
    }
    #[inline]
    fn get_ref(obejct_ref: &Object) -> &Self {
        match obejct_ref {
            Object::B(branch) => branch,
            _ => panic!("object isn't branch"),
        }
    }
    #[inline]
    fn get_mut(object_mut: &mut Object) -> &mut Self {
        match object_mut {
            Object::B(branch) => branch,
            _ => panic!("object isn't branch"),
        }
    }
    #[inline]
    fn unwrap(obj: Object) -> Self {
        match obj {
            Object::B(branch) => branch,
            _ => panic!("object isn't branch"),
        }
    }
    #[inline]
    fn is(obejct_ref: &Object) -> bool {
        match obejct_ref {
            Object::B(_) => true,
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
        // object_pos + key num + child num
        mem::size_of::<u64>() + mem::size_of::<u8>() + mem::size_of::<u8>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_branch_serialize_deserialize() {
        // test empty
        let branch0 = Branch::default();
        let mut buf: Vec<u8> = vec![0; 4096];
        assert!(branch0.serialize(&mut buf.as_mut_slice()).is_ok());
        let branch00 = Branch::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(branch0, branch00);
        // test one
        let mut branch1 = Branch::default();
        branch1.keys.push(vec![1, 2, 3]);
        branch1.children.push(2);
        branch1.children.push(3);
        branch1.pos.add_len(3 + 1 + 4 + 4);
        assert!(branch1.serialize(&mut buf.as_mut_slice()).is_ok());
        let branch11 = Branch::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(branch1, branch11);
    }

    #[test]
    fn test_branch_search() {
        let mut branch = Branch::default();
        for i in 1..10 {
            branch.keys.push(vec![i]);
            branch.children.push(i as u32);
        }
        branch.children.insert(0, 0);
        assert_eq!(branch.search(&vec![0]), (0, 0));
        assert_eq!(branch.search(&vec![1, 2]), (1, 1));
        assert_eq!(branch.search(&vec![10]), (9, 9));
    }
    #[test]
    fn test_branch_insert() {
        let mut branch = Branch::default();
        for i in 1..3 {
            let key = vec![i];
            branch.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch.keys.push(key);
            branch.children.push(i as u32);
        }
        branch.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch.children.insert(0, 0);
        assert_eq!(
            branch.get_pos().get_len(),
            Branch::get_header_size() as u16 + 2 + 2 + 4 * 3
        );
        branch.insert_non_full(2, vec![4], 4);
        assert_eq!(
            branch.get_pos().get_len(),
            Branch::get_header_size() as u16 + 2 + 2 + 4 * 3 + 1 + 1 + 4
        );
        assert_eq!(branch.search(&vec![4]), (4, 3));
    }

    #[test]
    fn test_branch_split() {
        let mut branch = Branch::default();
        for i in 1..3 {
            let key = vec![i; 40];
            branch.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch.keys.push(key);
            branch.children.push(i as u32);
        }
        branch.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch.children.insert(0, 0);
        for i in 2..100 {
            branch.insert_non_full(i, vec![i as u8 + 1; 40], i as u32 + 1);
        }
        assert_eq!(
            branch.get_pos().get_len(),
            Branch::get_header_size() as u16 + 40 * 100 + 1 * 100 + 4 * 101
        );
        let branch0 = branch.clone();
        let (key, mut other) = branch.split();
        assert_eq!(key, vec![46; 40]);
        assert_eq!(branch.children.last().unwrap(), &45);
        assert_eq!(other.keys[0], vec![47; 40]);
        assert_eq!(other.children[0], 46);
        branch.merge(&mut other, vec![46; 40]);
        assert_eq!(branch0, branch);
    }

    #[test]
    fn test_branch_merge() {
        let mut branch0 = Branch::default();
        for i in 1..3 {
            let key = vec![i; 40];
            branch0.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch0.keys.push(key);
            branch0.children.push(i as u32);
        }
        branch0.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch0.children.insert(0, 0);
        let mut branch1 = Branch::default();
        for i in 4..6 {
            let key = vec![i; 40];
            branch1.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch1.keys.push(key);
            branch1.children.push(i as u32);
        }
        branch1.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch1.children.insert(0, 3);
        let mut branch3 = Branch::default();
        for i in 1..6 {
            let key = vec![i; 40];
            branch3.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch3.keys.push(key);
            branch3.children.push(i as u32);
        }
        branch3.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch3.children.insert(0, 0);
        branch0.merge(&mut branch1, vec![3; 40]);
        assert_eq!(branch0, branch3);
    }

    #[test]
    fn test_branch_rebalance() {
        let mut branch0 = Branch::default();
        for i in 1..3 {
            let key = vec![i; 40];
            branch0.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch0.keys.push(key);
            branch0.children.push(i as u32);
        }
        branch0.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch0.children.insert(0, 0);
        for i in 2..9 {
            branch0.insert_non_full(i, vec![i as u8 + 1; 40], i as u32 + 1);
        }
        let mut branch1 = Branch::default();
        for i in 11..13 {
            let key = vec![i; 40];
            branch1.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch1.keys.push(key);
            branch1.children.push(i as u32);
        }
        branch1.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch1.children.insert(0, 10);
        for i in 2..90 {
            branch1.insert_non_full(i, vec![i as u8 + 11; 40], i as u32 + 11);
        }
        assert!(Branch::should_rebalance(&branch0, &branch1));
        let key = branch0.rebalance(&mut branch1, vec![10; 40]);
        assert_eq!(key, vec![46; 40]);
        let mut new_branch0 = branch0.clone();
        let mut new_branch1 = branch1.clone();
        new_branch0.merge(&mut new_branch1, vec![46; 40]);
        let (key, new_branch1) = new_branch0.split();
        assert_eq!(key, vec![46; 40]);
        assert_eq!(branch0, new_branch0);
        assert_eq!(branch1, new_branch1);
        let mut branch = Branch::default();
        for i in 1..3 {
            let key = vec![i; 40];
            branch.pos.add_len(
                key.len() as u16 + mem::size_of::<u8>() as u16 + mem::size_of::<ObjectId>() as u16,
            );
            branch.keys.push(key);
            branch.children.push(i as u32);
        }
        branch.pos.add_len(mem::size_of::<ObjectId>() as u16);
        branch.children.insert(0, 0);
        for i in 2..100 {
            branch.insert_non_full(i, vec![i as u8 + 1; 40], i as u32 + 1);
        }
        let (key, mut other) = branch.split();
        assert_eq!(branch, branch0);
        assert_eq!(other, branch1);
    }

}
