//! gRPC config settings

use std::path::PathBuf;

// #[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
/// gRPC TLS settings
pub struct GrpcZainoConfig {
    /// wrapper for TLS config, optional
    pub grpc_tls: Option<GrpcTlsConfig>,
}

/// Inner type, when a Zaino is configured with gRPC tls, it has paths to key and certificate.
pub struct GrpcTlsConfig {
    /// Path to the TLS certificate file.
    pub tls_cert_path: PathBuf,
    /// Path to the TLS private key file.
    pub tls_key_path: PathBuf,
}

impl Default for GrpcZainoConfig {
    fn default() -> Self {
        // TODO may be dangerous as a default.
        Self { grpc_tls: None }
    }
}
