use crate::object::Object;
use std::sync::Arc;

pub enum ObjectState {
    Readonly(Arc<Object>),
    Dirty(Object, Arc<Object>),
    New(Object),
    Del(Arc<Object>),
}

impl ObjectState {
    #[inline]
    pub fn get_ref(&self) -> Option<&Object> {
        match self {
            ObjectState::Readonly(obj) => Some(&*obj),
            ObjectState::Dirty(obj, _) => Some(obj),
            ObjectState::New(obj) => Some(obj),
            _ => None,
        }
    }
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut Object> {
        match self {
            ObjectState::Dirty(obj, _) => Some(obj),
            ObjectState::New(obj) => Some(obj),
            _ => None,
        }
    }
    #[inline]
    pub fn into_arc(self) -> Option<Arc<Object>> {
        match self {
            ObjectState::Readonly(obj) => Some(obj.clone()),
            ObjectState::Dirty(obj, _) => Some(Arc::new(obj)),
            ObjectState::New(obj) => Some(Arc::new(obj)),
            _ => None,
        }
    }
    #[inline]
    pub fn to_dirty(self) -> Self {
        match self {
            ObjectState::Readonly(obj) => ObjectState::Dirty((*obj).clone(), obj.clone()),
            _ => panic!("object is not readonly"),
        }
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        match self {
            ObjectState::Dirty(_, _) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_new(&self) -> bool {
        match self {
            ObjectState::New(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_del(&self) -> bool {
        match self {
            ObjectState::Del(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_readonly(&self) -> bool {
        match self {
            ObjectState::Readonly(_) => true,
            _ => false,
        }
    }
}
