use crate::nodetable::NodeId;

const KIND_BIT_MASK: u8 = 0b11;

pub enum Node {
    L(Leaf),
    B(Branch),
    E(Entry),
}

#[repr(u8)]
#[derive(Eq,PartialEq)]
pub enum NodeKind {
    Leaf,
    Branch,
    Entry,
    Del
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

type Key = Vec<u8>;

type Val = Vec<u8>;

pub struct Leaf {
    entrys: Vec<(Key, NodeId)>,
    total_size: u16,
}

pub struct Branch {
    keys: Vec<Key>,
    children: Vec<NodeId>,
    total_Size: u16,
}

pub struct Entry {
    key: Key,
    val: Val,
}
