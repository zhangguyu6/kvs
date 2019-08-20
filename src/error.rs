use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum TdbError {
    IoError(io::Error),
    ExceedMaxCap,
    SerializeError,
    DeserializeError,
    Restart,
    NoSpace,
    NotFindObject,
}

impl PartialEq for TdbError {
    fn eq(&self, other: &TdbError) -> bool {
        use TdbError::*;
        match (self, other) {
            (ExceedMaxCap, ExceedMaxCap) => true,
            (SerializeError, SerializeError) => true,
            (DeserializeError, DeserializeError) => true,
            (Restart, Restart) => true,
            (NoSpace, NoSpace) => true,
            (NotFindObject, NotFindObject) => true,
            (IoError(e1), IoError(e2)) => e1.kind() == e2.kind(),
            _ => false,
        }
    }
}

impl Eq for TdbError {}

impl fmt::Display for TdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnError")
    }
}

impl Error for TdbError {
    fn description(&self) -> &str {
        "TxnError"
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use TdbError::*;
        match self {
            IoError(ioerror) => Some(ioerror),
            _ => None,
        }
    }
}

impl From<io::Error> for TdbError {
    fn from(err: io::Error) -> Self {
        Self::IoError(err)
    }
}
