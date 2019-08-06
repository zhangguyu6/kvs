use crate::nodetable::{NodeAddressAllocater, NodeAddressTable};
use crate::storage::{BlockDev, RawBlockDev};

pub struct ReadonlyNodeManager<Dev> {
    node_table: NodeAddressTable<Dev>,
}

