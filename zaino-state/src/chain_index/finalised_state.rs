//! Holds the Finalised portion of the chain index on disk.

// TODO / FIX - ROMOVE THIS ONCE CHAININDEX LANDS!
#![allow(dead_code)]

pub(crate) mod capability;
pub(crate) mod db;
pub(crate) mod entry;
pub(crate) mod migrations;
pub(crate) mod reader;
pub(crate) mod router;

use capability::*;
use db::DbBackend;
use migrations::MigrationManager;
use reader::*;
use router::Router;
use tracing::info;
use zebra_chain::parameters::NetworkKind;

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, ChainBlock, Hash, Height, StatusType,
};

use std::{sync::Arc, time::Duration};
use tokio::time::{interval, MissedTickBehavior};

use super::source::BlockchainSourceInterface;

pub(crate) struct ZainoDB {
    db: Arc<Router>,
    cfg: BlockCacheConfig,
}

impl ZainoDB {
    // ***** DB control *****

    /// Spawns a ZainoDB, opens an existing database if a path is given in the config else creates a new db.
    ///
    /// Peeks at the db metadata store to load correct database version.
    pub(crate) async fn spawn<T>(
        cfg: BlockCacheConfig,
        source: T,
    ) -> Result<Self, FinalisedStateError>
    where
        T: BlockchainSourceInterface,
    {
        let version_opt = dbg!(Self::try_find_current_db_version(&cfg).await);

        let target_db_version = match cfg.db_version {
            0 => DbVersion {
                major: 0,
                minor: 0,
                patch: 0,
            },
            _ => DbVersion {
                major: 1,
                minor: 0,
                patch: 0,
            },
        };

        let backend = match version_opt {
            Some(version) => {
                info!("Opening ZainoDBv{} from file.", version);
                match version {
                    0 => DbBackend::spawn_v0(&cfg).await?,
                    1 => DbBackend::spawn_v1(&cfg).await?,
                    _ => {
                        return Err(FinalisedStateError::Custom(format!(
                            "unsupported database version: DbV{version}"
                        )))
                    }
                }
            }
            None => {
                info!("Creating new ZainoDBv{}.", target_db_version);
                match target_db_version.major() {
                    0 => DbBackend::spawn_v0(&cfg).await?,
                    1 => DbBackend::spawn_v1(&cfg).await?,
                    _ => {
                        return Err(FinalisedStateError::Custom(format!(
                            "unsupported database version: DbV{target_db_version}"
                        )))
                    }
                }
            }
        };
        let current_db_version = backend.get_metadata().await?.version();

        let router = Arc::new(Router::new(Arc::new(backend)));

        if version_opt.is_some() && current_db_version < target_db_version {
            info!(
                "Starting ZainoDB migration manager, migratiing database from v{} to v{}.",
                current_db_version, target_db_version
            );
            MigrationManager::migrate_to(
                Arc::clone(&router),
                &cfg,
                current_db_version,
                target_db_version,
                source,
            )
            .await?;
        }

        Ok(Self { db: router, cfg })
    }

    /// Gracefully shuts down the running ZainoDB, closing all child processes.
    pub(crate) async fn shutdown(&self) -> Result<(), FinalisedStateError> {
        self.db.shutdown().await
    }

    /// Returns the status of the running ZainoDB.
    pub(crate) async fn status(&self) -> StatusType {
        self.db.status().await
    }

    /// Waits until the ZainoDB returns a Ready status.
    pub(crate) async fn wait_until_ready(&self) {
        let mut ticker = interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if self.db.status().await == StatusType::Ready {
                break;
            }
        }
    }

    /// Creates a read-only viewer onto the running ZainoDB.
    ///
    /// NOTE: **ALL** chain fetch should use DbReader instead of directly using ZainoDB.
    pub(crate) fn to_reader(self: &Arc<Self>) -> DbReader {
        DbReader {
            inner: Arc::clone(self),
        }
    }

    /// Look for kown dirs to find current db version.
    ///
    /// The oldest version is returned as the database may have been closed mid migration.
    ///
    /// * `Some(version)` – DB exists, version returned.
    /// * `None`      – directory or key is missing -> fresh DB.
    pub async fn try_find_current_db_version(cfg: &BlockCacheConfig) -> Option<u32> {
        let legacy_dir = match cfg.network.kind() {
            NetworkKind::Mainnet => "live",
            NetworkKind::Testnet => "test",
            NetworkKind::Regtest => "local",
        };
        let legacy_path = cfg.db_path.join(legacy_dir);
        if legacy_path.join("data.mdb").exists() && legacy_path.join("lock.mdb").exists() {
            return Some(0);
        }

        let net_dir = match cfg.network.kind() {
            NetworkKind::Mainnet => "mainnet",
            NetworkKind::Testnet => "testnet",
            NetworkKind::Regtest => "regtest",
        };
        let net_path = cfg.db_path.join(net_dir);
        if net_path.exists() && net_path.is_dir() {
            let version_dirs = ["v1"];
            for (i, version_dir) in version_dirs.iter().enumerate() {
                let db_path = net_path.join(version_dir);
                let data_file = db_path.join("data.mdb");
                let lock_file = db_path.join("lock.mdb");
                if data_file.exists() && lock_file.exists() {
                    let version = (i + 1) as u32;
                    return Some(version);
                }
            }
        }

        None
    }

    /// Returns the internal db backend for the given db capability.
    ///
    /// Used by DbReader to route calls to the correct database during major migrations.
    #[inline]
    pub(crate) fn backend_for_cap(
        &self,
        cap: Capability,
    ) -> Result<Arc<DbBackend>, FinalisedStateError> {
        self.db.backend(cap)
    }

    // ***** Db Core Write *****

    /// Writes a block to the database.
    ///
    /// This **MUST** be the *next* block in the chain (db_tip_height + 1).
    pub(crate) async fn write_block(&self, b: ChainBlock) -> Result<(), FinalisedStateError> {
        self.db.write_block(b).await
    }

    /// Deletes a block from the database by height.
    ///
    /// This **MUST** be the *top* block in the db.
    ///
    /// Uses `delete_block` internally, fails if the block to be deleted cannot be correctly built.
    /// If this happens, the block to be deleted must be fetched from the validator and given to `delete_block`
    /// to ensure the block has been completely wiped from the database.
    pub(crate) async fn delete_block_at_height(
        &self,
        h: Height,
    ) -> Result<(), FinalisedStateError> {
        self.db.delete_block_at_height(h).await
    }

    /// Deletes a given block from the database.
    ///
    /// This **MUST** be the *top* block in the db.
    pub(crate) async fn delete_block(&self, b: &ChainBlock) -> Result<(), FinalisedStateError> {
        self.db.delete_block(b).await
    }

    // ***** DB Core Read *****

    /// Returns the highest block height held in the database.
    pub(crate) async fn db_height(&self) -> Result<Option<Height>, FinalisedStateError> {
        self.db.db_height().await
    }

    /// Returns the block height for the given block hash *if* present in the finalised state.
    ///
    /// TODO: Should theis return `Result<Option<Height>, FinalisedStateError>`?
    pub(crate) async fn get_block_height(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        self.db.get_block_height(hash).await
    }

    /// Returns the block block hash for the given block height *if* present in the finlaised state.
    pub(crate) async fn get_block_hash(&self, h: Height) -> Result<Hash, FinalisedStateError> {
        self.db.get_block_hash(h).await
    }

    /// Returns metadata for the running ZainoDB.
    pub(crate) async fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        self.db.get_metadata().await
    }
}
