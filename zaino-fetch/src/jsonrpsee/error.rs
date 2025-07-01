//! Hold error types for the JsonRpSeeConnector and related functionality.

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// General error type for handling JsonRpSeeConnector errors.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Type for errors without an underlying source.
    #[error("Error: {0}")]
    JsonRpSeeClientError(String),

    /// Reqwest Based Errors.
    #[error("Error: HTTP Request Error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    /// Invalid URI Errors.
    #[error("Error: Invalid URI: {0}")]
    InvalidUriError(#[from] http::uri::InvalidUri),

    /// URL Parse Errors.
    #[error("Error: Invalid URL:{0}")]
    UrlParseError(#[from] url::ParseError),

    /// std::io::Error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl TransportError {
    /// Constructor for errors without an underlying source
    pub fn new(msg: impl Into<String>) -> Self {
        TransportError::JsonRpSeeClientError(msg.into())
    }

    /// Converts TransportError to tonic::Status
    ///
    /// TODO: This impl should be changed to return the correct status [https://github.com/zcash/lightwalletd/issues/497] before release,
    ///       however propagating the server error is useful during development.
    pub fn to_grpc_status(&self) -> tonic::Status {
        // TODO: Hide server error from clients before release. Currently useful for dev purposes.
        tonic::Status::internal(format!("Error: JsonRpSee Client Error: {}", self))
    }
}

impl From<TransportError> for tonic::Status {
    fn from(err: TransportError) -> Self {
        err.to_grpc_status()
    }
}

impl From<serde_json::Error> for TransportError {
    fn from(e: serde_json::Error) -> Self {
        TransportError::JsonRpSeeClientError(e.to_string())
    }
}
