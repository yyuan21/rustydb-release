use std::error::Error as StdError;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    BitStreamIOError(io::Error),
    BitReaderError(&'static str),
    AppendOrderError,
    AppendDurationError,
    BadDimensionError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::BitStreamIOError(_) => f.write_str("BitStream-IO Error"),
            Error::AppendOrderError => f.write_str("Appending out of order item"),
            Error::AppendDurationError => f.write_str("Appending item with excessive duration"),
            Error::BitReaderError(_) => f.write_str("BitStreamReader error"),
            Error::BadDimensionError => f.write_str("Entry dimension must match that of writer"),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match self {
            Error::BitStreamIOError(_) => "BitStream-IO error",
            Error::AppendOrderError => "Append order error",
            Error::AppendDurationError => "Append excess duration",
            Error::BitReaderError(_) => "BitStreamReader error",
            Error::BadDimensionError => "Bad Dimension error",
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::BitStreamIOError(error)
    }
}
