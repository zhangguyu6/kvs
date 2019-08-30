use crate::object::Object;
use std::sync::Arc;

pub enum MutObject {
    Readonly(Arc<Object>),
    Dirty(Object,Arc<Object>),
    New(Object),
    Del(Arc<Object>),
}

impl MutObject {
    #[inline]
    pub fn get_ref(&self) -> Option<&Object> {
        match self {
            MutObject::Readonly(obj) => Some(&*obj),
            MutObject::Dirty(obj,_) => Some(obj),
            MutObject::New(obj) => Some(obj),
            _ => None,
        }
    }
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut Object> {
        match self {
            MutObject::Dirty(obj,_) => Some(obj),
            MutObject::New(obj) => Some(obj),
            _ => None,
        }
    }
    #[inline]
    pub fn into_arc(self) -> Option<Arc<Object>> {
        match self {
            MutObject::Readonly(obj) => Some(obj.clone()),
            MutObject::Dirty(obj,_) => Some(Arc::new(obj)),
            MutObject::New(obj) => Some(Arc::new(obj)),
            _ => None,
        }
    }
    #[inline]
    pub fn to_dirty(self) -> Self {
        match self {
            MutObject::Readonly(obj) => MutObject::Dirty((*obj).clone(),obj.clone()),
            _ => panic!("object is not readonly"),
        }
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        match self {
            MutObject::Dirty(_,_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_new(&self) -> bool {
        match self {
            MutObject::New(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_del(&self) -> bool {
        match self {
            MutObject::Del(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_readonly(&self) -> bool {
        match self {
            MutObject::Readonly(_) => true,
            _ => false,
        }
    }
}
