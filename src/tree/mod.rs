use crate::error::TdbError;
use crate::nodetable::{NodeId, G_NAT};
use crate::storage::{BlockDeserialize, BlockId, BlockSerialize};
use std::collections::HashMap;
use std::mem;
use std::ops::Range;
use std::u8;

const KIND_BIT_MASK: u8 = 0b11;
const MAX_KEY_LEN: usize = u8::MAX as usize;
const MAX_NODE_SIZE: usize = 4096;
// key + key len + nodeid
const SPLIT_NODE_SIZE: usize = 4096 - MAX_KEY_LEN - mem::size_of::<u32>() - mem::size_of::<u8>();
const REBALANCE_NODE_SIZE: usize = MAX_NODE_SIZE / 4;

#[derive(PartialEq, Eq, Hash)]
pub enum Node {
    L(Leaf),
    B(Branch),
    E(Entry),
}

impl Node {
    pub fn read(reader: &[u8], node_kind: NodeKind) -> Result<Self, TdbError> {
        match node_kind {
            NodeKind::Leaf => Leaf::deserialize(reader).map(|leaf| Node::L(leaf)),
            NodeKind::Branch => Branch::deserialize(reader).map(|branch| Node::B(branch)),
            NodeKind::Entry => Entry::deserialize(reader).map(|entry| Node::E(entry)),
            _ => unreachable!(),
        }
    }
    pub fn write(&self, writer: &mut [u8]) -> Result<(), TdbError> {
        match self {
            Node::L(leaf) => leaf.serialize(writer),
            Node::B(branch) => branch.serialize(writer),
            Node::E(entry) => entry.serialize(writer),
        }
    }
    pub fn get_kind(&self) -> NodeKind {
        match self {
            Node::L(_) => NodeKind::Leaf,
            Node::B(_) => NodeKind::Branch,
            Node::E(_) => NodeKind::Entry,
        }
    }
    pub fn get_branch(&self) -> Option<&Branch> {
        match self {
            Node::B(branch) => Some(branch),
            _ => None,
        }
    }
    pub fn get_entry(&self) -> Option<&Entry> {
        match self {
            Node::E(entry) => Some(entry),
            _ => None,
        }
    }
    pub fn get_leaf(&self) -> Option<&Leaf> {
        match self {
            Node::L(leaf) => Some(leaf),
            _ => None,
        }
    }
    pub fn get_branch_mut(&mut self) -> Option<&mut Branch> {
        match self {
            Node::B(branch) => Some(branch),
            _ => None,
        }
    }
    pub fn get_entry_mut(&mut self) -> Option<&mut Entry> {
        match self {
            Node::E(entry) => Some(entry),
            _ => None,
        }
    }
    pub fn get_leaf_mut(&mut self) -> Option<&mut Leaf> {
        match self {
            Node::L(leaf) => Some(leaf),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Eq, PartialEq, Copy, Clone)]
pub enum NodeKind {
    Leaf,
    Branch,
    Entry,
    Del,
}

impl From<u8> for NodeKind {
    #[inline]
    fn from(tag: u8) -> Self {
        let kindbits = KIND_BIT_MASK & tag;
        if kindbits == 0b0 {
            NodeKind::Leaf
        } else if kindbits == 0b1 {
            NodeKind::Branch
        } else if kindbits == 0b10 {
            NodeKind::Entry
        } else {
            NodeKind::Del
        }
    }
}

impl Default for NodeKind {
    fn default() -> Self {
        NodeKind::Del
    }
}

#[derive(Eq, PartialEq)]
pub struct NodePos {
    pub block_start: BlockId,
    pub block_len: u16,
    pub offset: u16,
}

impl Default for NodePos {
    fn default() -> Self {
        Self {
            block_start: 0,
            block_len: 0,
            offset: 0,
        }
    }
}

type Key = Vec<u8>;

type Val = Vec<u8>;

#[derive(PartialEq, Eq, Hash)]
pub struct Leaf {
    entrys: Vec<(Key, NodeId)>,
    total_size: u16,
}

impl Leaf {
    fn search(&self, key: &[u8]) -> Option<NodeId> {
        match self
            .entrys
            .binary_search_by(|_key| _key.0.as_slice().cmp(key))
        {
            Ok(index) => Some(self.entrys[index].1),
            _ => None,
        }
    }
}

impl BlockSerialize for Leaf {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError> {
        unimplemented!()
    }
}

impl BlockDeserialize for Leaf {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError> {
        unimplemented!()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct Branch {
    keys: Vec<Key>,
    children: Vec<NodeId>,
    total_size: u16,
}

impl Branch {
    fn search(&self, key: &[u8]) -> (NodeId, usize) {
        let index = match self.keys.binary_search_by(|_key| _key.as_slice().cmp(key)) {
            Ok(index) => index + 1,
            Err(index) => index,
        };
        (self.children[index], index)
    }
    fn insert_non_full(&mut self, index: usize, key: Key, node_id: NodeId) {
        self.total_size += (key.len() + mem::size_of::<u8>() + mem::size_of::<u32>()) as u16;
        self.keys.insert(index, key);
        self.children.insert(index + 1, node_id);
    }
    fn split(&mut self) -> (Key, Node) {
        assert!(self.total_size as usize > SPLIT_NODE_SIZE);
        let mut split_index = 0;
        let mut leaf_size = Self::get_header_size();
        for i in 0..self.keys.len() {
            leaf_size += self.keys[i].len() + mem::size_of::<u8>() + mem::size_of::<u32>();
            split_index = i;
            if leaf_size > MAX_NODE_SIZE / 2 {
                leaf_size -= self.keys[i].len() + mem::size_of::<u8>() + mem::size_of::<u32>();
                break;
            }
        }
        let right_keys = self.keys.split_off(split_index + 1);
        let right_children = self.children.split_off(split_index + 1);
        let split_key = self.keys.pop().unwrap();
        let right_size = self.total_size - leaf_size as u16 + Self::get_header_size() as u16;
        self.total_size = leaf_size as u16;
        let right_node = Branch {
            keys: right_keys,
            children: right_children,
            total_size: right_size,
        };
        (split_key, Node::B(right_node))
    }
    // make left del after merge
    fn merge(&mut self, left: &Branch, merge_key: Key) {
        assert!(
            self.total_size as usize + left.total_size as usize - Branch::get_header_size()
                < SPLIT_NODE_SIZE
        );
        self.keys.push(merge_key);
        for key in left.keys.iter() {
            self.keys.push(key.clone());
        }
        for node_id in left.children.iter() {
            self.children.push(*node_id);
        }
        self.total_size += left.total_size - Branch::get_header_size() as u16;
    }
    fn rebalance(&mut self, left: &mut Branch, rebalance_key: Key) -> Key {
        assert!(
            self.total_size as usize + left.total_size as usize - Branch::get_header_size()
                > SPLIT_NODE_SIZE
        );
        self.keys.push(rebalance_key);
        self.keys.append(&mut left.keys);
        self.children.append(&mut left.children);
        self.total_size += left.total_size - Branch::get_header_size() as u16;
        let mut split_index = 0;
        let mut leaf_size = Self::get_header_size();
        for i in 0..self.keys.len() {
            leaf_size += self.keys[i].len() + mem::size_of::<u8>() + mem::size_of::<u32>();
            split_index = i;
            if leaf_size > MAX_NODE_SIZE / 2 {
                leaf_size -= self.keys[i].len() + mem::size_of::<u8>() + mem::size_of::<u32>();
                break;
            }
        }
        left.keys = self.keys.split_off(split_index + 1);
        left.children = self.children.split_off(split_index + 1);
        left.total_size = self.total_size - leaf_size as u16 + Self::get_header_size() as u16;
        self.total_size = leaf_size as u16;
        self.keys.pop().unwrap()
    }
    fn should_split(&self) -> bool {
        (self.total_size as usize) > SPLIT_NODE_SIZE
    }
    fn should_rebalance(&self) -> bool {
        (self.total_size as usize) < REBALANCE_NODE_SIZE
    }
    fn get_header_size() -> usize {
        unimplemented!()
    }
}

impl BlockSerialize for Branch {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError> {
        unimplemented!()
    }
}

impl BlockDeserialize for Branch {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError> {
        unimplemented!()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct Entry {
    key: Key,
    val: Val,
}

impl BlockSerialize for Entry {
    fn serialize(&self, writer: &mut [u8]) -> Result<(), TdbError> {
        unimplemented!()
    }
}

impl BlockDeserialize for Entry {
    fn deserialize(reader: &[u8]) -> Result<Self, TdbError> {
        unimplemented!()
    }
}

pub struct Tree {
    root: NodeId,
    dirty_nodes: HashMap<NodeId, Node>,
}

impl Tree {
    pub fn get(&self, key: &[u8]) -> Option<NodeId> {
        let mut node_id = self.root;
        loop {
            let node = G_NAT
                .get(node_id)
                .expect("node data error, point to non-existed-data");
            match node.as_ref() {
                Node::L(leaf) => {
                    return leaf.search(key);
                }
                Node::B(branch) => {
                    node_id = branch.search(key).0;
                }
                _ => unreachable!(),
            }
        }
    }

    // insert only if tree contains key
    pub fn insert(&mut self, key: Vec<u8>, node_id: NodeId) {
        unimplemented!()
    }

    // del only if tree contains key
    pub fn remove(&mut self, key: &[u8]) -> NodeId {
        unimplemented!()
    }

    pub fn range(&self, range: Range<&[u8]>) -> Iter {
        unimplemented!()
    }
}

pub struct Iter<'a> {
    path: Vec<(&'a [u8], NodeId)>,
}
