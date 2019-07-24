use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum TdbError {
    IoError(io::Error),
    SerializeError,
    DeserializeError,
    Restart,
}

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
