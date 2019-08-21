use crate::error::TdbError;
use crate::object::Object;
use crate::storage::Serialize;
use byteorder::WriteBytesExt;
use std::io::Write;
use std::sync::Arc;
const BLOCK_SIZE: usize = 4096;

pub struct ObjectLog {
    index_objs: Vec<Arc<Object>>,
    entry_objs: Vec<Arc<Object>>,
    size: usize,
}

impl Serialize for ObjectLog {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), TdbError> {
        for arc_obj in self.index_objs.iter() {
            arc_obj.write(writer)?;
        }
        for arc_entry in self.entry_objs.iter() {
            arc_entry.write(writer)?;
        }
        if self.size % BLOCK_SIZE != 0 {
            for _ in (self.size % BLOCK_SIZE)..BLOCK_SIZE {
                writer.write_u8(0)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Object, ObjectTag};
    use crate::tree::{Branch, Entry, Leaf};
    #[test]
    fn test_obj_log() {
        let arc_branch = Arc::new(Object::B(Branch::default()));
        let mut leaf = Leaf::default();
        leaf.insert_non_full(0, vec![1; 40], 0);
        let arc_leaf = Arc::new(Object::L(leaf));
        let arc_entry = Arc::new(Object::E(Entry::default()));
        let object_log = ObjectLog {
            index_objs: vec![arc_branch.clone(), arc_leaf.clone()],
            entry_objs: vec![arc_entry.clone()],
            size: arc_branch.get_object_info().size
                + arc_leaf.get_object_info().size
                + arc_entry.get_object_info().size,
        };
        let mut buf = [0; BLOCK_SIZE * 3];
        assert!(object_log.serialize(&mut &mut buf[..]).is_ok());

        assert_eq!(
            &Object::read(&mut &buf[0..4096], &ObjectTag::Branch).unwrap(),
            arc_branch.as_ref()
        );
        assert_eq!(
            &Object::read(&mut &buf[4096..4096 * 2], &ObjectTag::Leaf).unwrap(),
            arc_leaf.as_ref()
        );
        assert_eq!(
            &Object::read(&mut &buf[4096 * 2..], &ObjectTag::Entry).unwrap(),
            arc_entry.as_ref()
        );
    }

}
