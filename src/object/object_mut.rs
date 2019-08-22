use crate::object::Object;
use std::sync::Arc;

pub enum MutObject {
    Readonly(Arc<Object>),
    Dirty(Arc<Object>),
    New(Arc<Object>),
    Del,
}

impl MutObject {
    #[inline]
    pub fn get_ref(&self) -> Option<&Object> {
        match self {
            MutObject::Readonly(obj) => Some(&*obj),
            MutObject::Dirty(obj) => Some(&*obj),
            MutObject::New(obj) => Some(&*obj),
            _ => None,
        }
    }
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut Object> {
        match self {
            MutObject::Dirty(obj) => Some(Arc::get_mut(obj).unwrap()),
            MutObject::New(obj) => Some(Arc::get_mut(obj).unwrap()),
            _ => None,
        }
    }
    #[inline]
    pub fn into_arc(self) -> Option<Arc<Object>> {
        match self {
            MutObject::Readonly(obj) => Some(obj.clone()),
            MutObject::Dirty(obj) => Some(obj.clone()),
            MutObject::New(obj) => Some(obj.clone()),
            _ => None,
        }
    }
    #[inline]
    pub fn to_dirty(self) -> Self {
        match self {
            MutObject::Readonly(obj) => MutObject::Dirty(Arc::new((*obj).clone())),
            _ => panic!("object is not readonly"),
        }
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        match self {
            MutObject::Dirty(_) => true,
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
            MutObject::Del => true,
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
