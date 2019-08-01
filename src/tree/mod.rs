use crate::nodetable::NodeId;

pub enum Node {
    L(Leaf),
    B(Branch),
    E(Entry),
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
