mod check_point;
mod immut_table;
mod mut_table;
mod object_allocater;
mod object_manager;
mod object_table;
mod table;
pub use check_point::CheckPoint;
pub use object_allocater::ObjectAllocater;
pub use object_manager::{ObjectAccess, ObjectModify};
pub use object_table::{
    ObjectTable, ObjectTablePage, OBJECT_NUM, OBJECT_TABLE_ENTRY_PRE_PAGE, OBJECT_TABLE_PAGE_SIZE,
};
pub use table::{InnerTable, PageId, MAX_PAGE_NUM, OBJ_PRE_PAGE};
