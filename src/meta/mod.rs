mod check_point;
mod object_allocater;
mod object_table;
pub use check_point::CheckPoint;pub use object_allocater::ObjectAllocater;
pub use object_table::{
    ObjectTable, ObjectTablePage, OBJECT_TABLE_ENTRY_PRE_PAGE, OBJECT_TABLE_PAGE_SIZE,OBJECT_NUM
};
