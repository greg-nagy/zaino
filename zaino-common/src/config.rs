//! gRPC config settings

use std::path::PathBuf;

/// when a Zaino is configured with gRPC tls, it has paths to key and certificate.
/// gRPC TLS settings
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GrpcTlsConfig {
    /// Path to the TLS certificate file.
    // TODO maybe these need to be options because of GrpcConfig type...?
    // or.. I need to just load THAT type in here.
    pub tls_cert_path: PathBuf,
    /// Path to the TLS private key file.
    pub tls_key_path: PathBuf,
}

// TODO do we need a default?
impl Default for GrpcTlsConfig {
    fn default() -> Self {
        // TODO may be dangerous as a default.
        Self {
            tls_cert_path: "/".into(),
            tls_key_path: "/".into(),
        }
    }
}
