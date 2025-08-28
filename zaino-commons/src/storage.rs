//! Storage configuration types shared across Zaino services.

use std::path::PathBuf;

/// Cache configuration for DashMaps.
///
/// Used to configure the capacity and sharding of concurrent hash maps
/// used throughout Zaino services.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct CacheConfig {
    /// Capacity of the DashMaps used for caching.
    pub capacity: Option<usize>,
    /// Number of shards used in the DashMap.
    ///
    /// shard_amount should be greater than 0 and be a power of two.
    /// If a shard_amount which is not a power of two is provided, the function will panic.
    pub shard_amount: Option<usize>,
}

/// Database size limit configuration.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseSize {
    /// No size limit - database can grow indefinitely
    Unlimited,
    /// Limited to a specific size in GB
    Limited { gb: usize },
}

impl Default for DatabaseSize {
    fn default() -> Self {
        DatabaseSize::Unlimited
    }
}

/// Database configuration.
///
/// Configures the file path and size limits for persistent storage
/// used by Zaino services.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DatabaseConfig {
    /// Database file path.
    pub path: PathBuf,
    /// Database size limit.
    pub size: DatabaseSize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./zaino_cache"),
            size: DatabaseSize::default(),
        }
    }
}

/// Storage configuration combining cache and database settings.
///
/// This is used by services that need both in-memory caching and persistent storage.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct StorageConfig {
    /// Cache configuration for DashMaps
    pub cache: CacheConfig,
    /// Database configuration
    pub database: DatabaseConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache: CacheConfig::default(),
            database: DatabaseConfig::default(),
        }
    }
}