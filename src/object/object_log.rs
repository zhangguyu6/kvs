use crate::object::Object;
use crate::storage::ObjectPos;

use std::sync::Arc;

pub struct ObjectLog(Vec<Arc<Object>>);
