mod check_point;
mod immut_table;
mod mut_table;
mod table;
pub use check_point::CheckPoint;
pub use immut_table::ImMutTable;
pub use mut_table::MutTable;
pub use table::{InnerTable, PageId, TablePage, MAX_PAGE_NUM, OBJ_PRE_PAGE, TABLE_PAGE_SIZE};
