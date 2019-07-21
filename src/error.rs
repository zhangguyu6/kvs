use std::error::Error;
use std::io;
use std::fmt;


#[derive(Debug)]
pub enum TxnError {
    IoError(io::Error),
    SerializeError
}


impl fmt::Display for TxnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnError")
    }
}

impl Error for TxnError {
    fn description(&self) -> &str {
        "TxnError"
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use TxnError::*;
        match self {
            IoError(ioerror) => Some(ioerror),
            _ => None
        }
    }
}
