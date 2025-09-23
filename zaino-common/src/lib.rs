//! Common types and configurations shared across Zaino crates.
//!
//! This crate provides shared configuration types, network abstractions,
//! and common utilities used across the Zaino blockchain indexer ecosystem.

pub mod config;
pub mod network;
pub mod service;
pub mod storage;

// Re-export commonly used types for convenience

// TODO is GrpcClientConfig a better name? Something else?
pub use config::GrpcTlsConfig;
pub use network::Network;
pub use service::ServiceConfig;
pub use storage::{CacheConfig, DatabaseConfig, DatabaseSize, StorageConfig};
