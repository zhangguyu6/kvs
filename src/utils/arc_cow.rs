use std::sync::Arc;

pub enum ArcCow<'a, T> {
    Borrowed(&'a T),
    Owned(Arc<T>),
}

impl<'a, T: Clone> ArcCow<'a, T> {
    pub fn into_owned(self) -> T {
        match self {
            ArcCow::Owned(t) => t.as_ref().clone(),
            ArcCow::Borrowed(t) => t.clone(),
        }
    }

    pub fn is_owned(&self) -> bool {
        match self {
            ArcCow::Owned(_) => true,
            _ => false,
        }
    }
}

impl<'a, T> AsRef<T> for ArcCow<'a, T>
where
    T: Clone,
{
    fn as_ref(&self) -> &T {
        match self {
            ArcCow::Owned(t) => t,
            ArcCow::Borrowed(arc_t) => arc_t,
        }
    }
}

impl<'a, T> From<Arc<T>> for ArcCow<'a, T> {
    fn from(t: Arc<T>) -> Self {
        ArcCow::Owned(t)
    }
}

impl<'a, T> From<&'a T> for ArcCow<'a, T> {
    fn from(t: &'a T) -> Self {
        ArcCow::Borrowed(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_arccow() {
        let a: u8 = 1;
        let cow_a: ArcCow<u8> = ArcCow::from(&a);
        assert!(!cow_a.is_owned());
        assert_eq!(cow_a.as_ref(), &1);
        assert_eq!(cow_a.into_owned(), 1);
    }
}
