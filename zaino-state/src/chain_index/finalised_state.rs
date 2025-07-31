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
use entry::*;
use migrations::MigrationManager;
use reader::*;
use router::Router;

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, ChainBlock, Hash, Height, StatusType,
    ZainoVersionedSerialise as _,
};

use lmdb::{Environment, EnvironmentFlags, Transaction};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::time::{interval, MissedTickBehavior};

pub(crate) struct ZainoDB {
    db: Arc<Router>,
    cfg: BlockCacheConfig,
}

impl ZainoDB {
    // ***** DB control *****

    /// Spawns a ZainoDB, opens an existing database if a path is given in the config else creates a new db.
    ///
    /// Peeks at the db metadata store to load correct database version.
    pub(crate) async fn spawn(cfg: BlockCacheConfig) -> Result<Self, FinalisedStateError> {
        /* as before … pick backend based on on-disk metadata */
        let meta_opt = Self::peek_metadata(&cfg.db_path).await?;

        // TODO: add db_version to cfg.
        // let current_version = match meta_opt {
        //     Some(meta) => meta.version(),
        //     None => cfg.target_version,
        // };

        let backend = match meta_opt {
            Some(meta) => match meta.version.major() {
                0 => DbBackend::spawn_v0(&cfg).await?,
                1 => DbBackend::spawn_v1(&cfg).await?,
                _ => {
                    return Err(FinalisedStateError::Custom(format!(
                        "unsupported version {meta:?}"
                    )))
                }
            },
            None => DbBackend::spawn_v1(&cfg).await?,
        };

        let router = Arc::new(Router::new(Arc::new(backend)));

        // if meta_opt.is_some() && current_version < cfg.target_version {
        //     MigrationManager::migrate_to(
        //         Arc::clone(&router),
        //         &cfg,
        //         current_version,
        //         cfg.target_version,
        //     )
        //     .await?;
        // }

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

    /// Try to read the metadata entry.
    ///
    /// * `Ok(Some(meta))` – DB exists, metadata decoded.
    /// * `Ok(None)`      – directory or key is missing ⇒ fresh DB.
    /// * `Err(e)`        – any other LMDB / decode failure.
    async fn peek_metadata(path: &Path) -> Result<Option<DbMetadata>, FinalisedStateError> {
        if !path.exists() {
            return Ok(None);
        }

        let env = match Environment::new()
            .set_max_dbs(4)
            .set_flags(EnvironmentFlags::READ_ONLY)
            .open(path)
        {
            Ok(env) => env,
            Err(lmdb::Error::Other(2)) => return Ok(None),
            Err(e) => return Err(FinalisedStateError::LmdbError(e)),
        };

        tokio::task::block_in_place(|| {
            let txn = env.begin_ro_txn()?;

            // Try metadata DB
            match env.open_db(Some("metadata")) {
                Ok(meta_dbi) => match txn.get(meta_dbi, b"metadata") {
                    Ok(raw) => {
                        let entry =
                            StoredEntryFixed::<DbMetadata>::from_bytes(raw).map_err(|e| {
                                FinalisedStateError::Custom(format!("metadata decode error: {e}"))
                            })?;
                        return Ok(Some(entry.item));
                    }
                    Err(lmdb::Error::NotFound) => {} // fall through and check legacy
                    Err(e) => return Err(FinalisedStateError::LmdbError(e)),
                },
                Err(lmdb::Error::NotFound) => {} // metadata db missing, check legacy
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            }

            // Fallback: detect legacy v0 by checking if known v0 DBs exist
            let has_v0 = env.open_db(Some("heights_to_hashes")).is_ok()
                && env.open_db(Some("hashes_to_blocks")).is_ok();

            if has_v0 {
                return Ok(Some(DbMetadata {
                    version: DbVersion {
                        major: 0,
                        minor: 0,
                        patch: 0,
                    },
                    schema_hash: [0u8; 32],
                }));
            }

            Ok(None)
        })
    }

    /// Returns the internal db backend for the given db capability.
    ///
    /// Used by DbReader to route calls to the correct database during major migrations.
    #[inline]
    pub(crate) fn backend_for_cap(&self, cap: Capability) -> Arc<DbBackend> {
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
