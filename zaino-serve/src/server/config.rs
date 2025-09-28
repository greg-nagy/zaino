//! Server configuration data.

/*
use serde::{
    // de::{self, Deserializer},
    Deserialize,
    Serialize,
};
*/

use std::{net::SocketAddr, path::PathBuf};

use tonic::transport::{Identity, ServerTlsConfig};

use super::error::ServerError;

/// when a Zaino is configured with gRPC tls, it has paths to key and certificate.
/// gRPC TLS settings
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GrpcTls {
    /// Path to the TLS certificate file.
    // TODO maybe these need to be options because of GrpcConfig type...?
    // or.. I need to just load THAT type in here.
    /// Path to the TLS certificate file in PEM format.
    pub tls_cert_path: PathBuf,
    /// Path to the TLS private key file in PEM format.
    pub tls_key_path: PathBuf,
}

/*
// TODO do we need a default?
impl Default for GrpcTlsSettings {
    fn default() -> Self {
        // TODO may be dangerous as a default.
        Self {
            tls_cert_path: "/".into(),
            tls_key_path: "/".into(),
        }
    }
}
*/

/// Configuration data for Zaino's gRPC server.
pub struct GrpcConfig {
    /// gRPC server bind addr.
    // TODO : commented this out because serde didn't want to be portable (orphan rule?)
    // can we work aorund this, or do we have to wrap it or something
    // #[serde(deserialize_with = "deserialize_socketaddr_from_string")]
    pub grpc_listen_address: SocketAddr,
    /// Enables TLS.
    pub tls: Option<GrpcTls>,
}

impl GrpcConfig {
    /// If TLS is enabled, reads the certificate and key files and returns a valid
    /// `ServerTlsConfig`. If TLS is not enabled, returns `Ok(None)`.
    // TODO now this return type is wrong, it used to get an Option<&str> IIUC
    pub async fn get_valid_tls(&self) -> Result<Option<ServerTlsConfig>, ServerError> {
        match self.tls.clone() {
            Some(t) => {
                if !t.tls_cert_path.exists() {
                    return Err(ServerError::ServerConfigError(
                        "TLS enabled but tls_cert_path does not exist".into(),
                    ));
                }
                let cert_path = t.tls_cert_path;

                if !t.tls_key_path.exists() {
                    return Err(ServerError::ServerConfigError(
                        "TLS enabled but tls_key_path does not exist".into(),
                    ));
                }
                let key_path = t.tls_key_path;

                // Read the certificate and key files asynchronously.
                let cert = tokio::fs::read(cert_path).await.map_err(|e| {
                    ServerError::ServerConfigError(format!("Failed to read TLS certificate: {e}"))
                })?;
                let key = tokio::fs::read(key_path).await.map_err(|e| {
                    ServerError::ServerConfigError(format!("Failed to read TLS key: {e}"))
                })?;

                // Build the identity and TLS configuration.
                let tls_id = Identity::from_pem(cert, key);
                let tls_config = ServerTlsConfig::new().identity(tls_id);
                Ok(Some(tls_config))
            }
            None => Ok(None),
        }
    }
}

/// Configuration data for Zaino's gRPC server.
pub struct JsonRpcConfig {
    /// Server bind addr.
    pub json_rpc_listen_address: SocketAddr,

    /// Enable cookie-based authentication.
    pub enable_cookie_auth: bool,

    /// Directory to store authentication cookie file.
    pub cookie_dir: Option<PathBuf>,
}
