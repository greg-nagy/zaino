pub mod network;
pub mod service;
pub mod storage;
pub mod validator;

// Re-export commonly used types at module root for ergonomic imports.
// This allows both `use zaino_common::config::Network` and
// `use zaino_common::config::network::Network` to work.
pub use network::{ActivationHeights, Network, ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS};
pub use service::ServiceConfig;
pub use storage::{CacheConfig, DatabaseConfig, DatabaseSize, StorageConfig};
pub use validator::ValidatorConfig;
