use crate::error::TdbError;
use crate::nodetable::NodeId;
use crate::storage::{BlockDeserialize, BlockId, BlockSerialize};
use std::ops::Range;
const KIND_BIT_MASK: u8 = 0b11;

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
            _ => unreachable!(),
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

pub struct NodePos {
    pub block_start: BlockId,
    pub offset: usize,
    pub len: usize,
}

impl Default for NodePos {
    fn default() -> Self {
        Self {
            block_start: 0,
            offset: 0,
            len: 0,
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
    fn search(&self, key: &[u8]) -> NodeId {
        match self.keys.binary_search_by(|_key| _key.as_slice().cmp(key)) {
            Ok(index) => self.children[index + 1],
            Err(index) => self.children[index],
        }
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
}

impl Tree {
    pub fn get(&self, key: &[u8]) -> Option<NodeId> {
        let mut node_id = self.root;
        unimplemented!()
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
