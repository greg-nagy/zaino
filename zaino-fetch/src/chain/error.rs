//! Hold error types for the BlockCache and related functionality.

/// Parser Error Type.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// Io Error.
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid Data Error
    #[error("Invalid Data Error: {0}")]
    InvalidData(String),

    /// UTF-8 conversion error.
    #[error("UTF-8 Error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    /// UTF-8 conversion error.
    #[error("UTF-8 Conversion Error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    /// Hexadecimal parsing error.
    #[error("Hex Parse Error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    /// Errors originating from prost decodings.
    #[error("Prost Decode Error: {0}")]
    ProstDecodeError(#[from] prost::DecodeError),

    /// Integer conversion error.
    #[error("Integer conversion error: {0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    /// Unecpected read order for sequential binary data
    #[error("Sequential binary data read: field {field} expected on position {expected_order} of transaction, read on {actual_order}")]
    InvalidParseOrder {
        field: &'static str,
        expected_order: u8,
        actual_order: u8,
    },

    /// Unexpected field size during parsing
    #[error("Field {field} expected size {expected} bytes, but advanced {actual} bytes")]
    UnexpectedFieldSize {
        field: &'static str,
        expected: usize,
        actual: usize,
    },

    /// Field not found in reader
    #[error("Field not found: {0}")]
    FieldNotFound(String),

    /// Field not parsed yet
    #[error("Field not parsed: {0}")]
    FieldNotParsed(&'static str),

    /// Consensus validation error
    #[error("Consensus error: {0}")]
    ConsensusError(#[from] crate::chain::transaction::ConsensusError),
}
