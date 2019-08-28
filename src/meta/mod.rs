mod check_point;
mod immut_table;
mod mut_table;
mod table;
pub use check_point::CheckPoint;
pub use table::{InnerTable, PageId, MAX_PAGE_NUM, OBJ_PRE_PAGE};
