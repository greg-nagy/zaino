//! Validator type for Zaino configuration.

// use serde::{Deserialize, Serialize};
// use zebra_chain::parameters::testnet::ConfiguredActivationHeights;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Validator (full-node) type for Zaino configuration.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
// #[serde(from = "ValidatorDeserialize")]
pub struct ValidatorConfig {
    // jsonrpc and grpc addresses always known.
    /// Full node / validator gprc listen port.
    //#[serde(deserialize_with = "deserialize_socketaddr_from_string")]
    pub validator_grpc_listen_address: SocketAddr,

    /// Full node / validator listen port.
    // #[serde(deserialize_with = "deserialize_socketaddr_from_string")]
    pub validator_jsonrpc_listen_address: SocketAddr,

    /// Enable validator rpc cookie authentication with Some
    /// Path to the validator cookie file.
    // TODO changed to PathBuf
    pub validator_cookie_path: Option<PathBuf>,
    /// Full node / validator Username.
    pub validator_user: Option<String>,
    /// full node / validator Password.
    pub validator_password: Option<String>,
}
