//! Holds the Finalised portion of the chain index on disk.

// TODO / FIX - ROMOVE THIS ONCE CHAININDEX LANDS!
#![allow(dead_code)]

pub(crate) mod capability;
pub(crate) mod db;
pub(crate) mod entry;
pub(crate) mod migrations;
pub(crate) mod reader;

use capability::*;
use db::v1::*;
use entry::*;
use reader::*;

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, ChainBlock, Hash, Height, StatusType,
    ZainoVersionedSerialise as _,
};

use lmdb::{Environment, EnvironmentFlags, Transaction};
use std::{path::Path, sync::Arc, time::Duration};

/// ZainoDB: Versioned database holding the finalised portion of the blockchain.
pub struct ZainoDB {
    db: Arc<dyn DbCore + Send + Sync>,
    caps: Capability,
    cfg: BlockCacheConfig,
}

impl ZainoDB {
    // ***** Db Control *****
    pub(crate) async fn spawn(cfg: BlockCacheConfig) -> Result<Self, FinalisedStateError> {
        // TODO: remove path, look in cfg..
        let meta = Self::peek_metadata(&cfg.db_path).await?;
        let caps = meta.version.capability();

        let db: Arc<dyn DbCore + Send + Sync> = match meta.version.major() {
            1 => Arc::new(DbV1::spawn(&cfg).await?) as _,
            _ => {
                return Err(FinalisedStateError::Custom(
                    "unsupported version found".to_string(),
                ))
            }
        };

        Ok(Self { db, caps, cfg })
    }

    pub(crate) async fn shutdown(&self) -> Result<(), FinalisedStateError> {
        self.db.shutdown().await
    }

    pub(crate) async fn status(&self) -> StatusType {
        self.db.status().await
    }

    pub(crate) async fn wait_until_ready(&self) {
        loop {
            if self.db.status().await == StatusType::Ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub(crate) fn to_reader(self: &Arc<Self>) -> DbReader {
        DbReader {
            inner: Arc::clone(self),
        }
    }

    async fn peek_metadata(path: &Path) -> Result<DbMetadata, FinalisedStateError> {
        let env = Environment::new()
            .set_max_dbs(4)
            .set_flags(EnvironmentFlags::READ_ONLY)
            .open(path)
            .map_err(FinalisedStateError::LmdbError)?;

        tokio::task::block_in_place(|| {
            let meta_dbi = env.open_db(Some("metadata"))?;
            let txn = env.begin_ro_txn()?;
            let raw = txn.get(meta_dbi, b"metadata")?;
            let entry = StoredEntryFixed::<DbMetadata>::from_bytes(raw)
                .map_err(|e| FinalisedStateError::Custom(format!("metadata decode error: {e}")))?;
            Ok(entry.item)
        })
    }

    // ***** Db Core Write *****
    pub(crate) async fn write_block(&self, block: ChainBlock) -> Result<(), FinalisedStateError> {
        self.db.write_block(block).await
    }

    pub(crate) async fn delete_block_at_height(
        &self,
        height: Height,
    ) -> Result<(), FinalisedStateError> {
        self.db.delete_block_at_height(height).await
    }

    pub(crate) async fn delete_block(&self, block: &ChainBlock) -> Result<(), FinalisedStateError> {
        self.db.delete_block(block).await
    }

    // ***** DB Core Read *****

    pub(crate) async fn db_height(&self) -> Result<Option<Height>, FinalisedStateError> {
        self.db.db_height().await
    }

    pub(crate) async fn get_block_height(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        self.db.get_block_height(hash).await
    }

    pub(crate) async fn get_block_hash(&self, height: Height) -> Result<Hash, FinalisedStateError> {
        self.db.get_block_hash(height).await
    }

    pub(crate) async fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        self.db.get_metadata().await
    }

    // ***** DB Extension methods *****

    #[inline]
    fn downcast<T: 'static>(&self) -> Option<&T> {
        self.db.as_any().downcast_ref::<T>()
    }

    pub(crate) fn block_core(&self) -> Option<&dyn BlockCoreExt> {
        if self.caps.has(Capability::BLOCK_CORE_EXT) {
            let backend = self.downcast::<DbV1>().unwrap();
            Some(backend as &dyn BlockCoreExt)
        } else {
            None
        }
    }

    pub(crate) fn block_transparent(&self) -> Option<&dyn BlockTransparentExt> {
        if self.caps.has(Capability::BLOCK_TRANSPARENT_EXT) {
            let backend = self.downcast::<DbV1>().unwrap();
            Some(backend as &dyn BlockTransparentExt)
        } else {
            None
        }
    }

    pub(crate) fn block_shielded(&self) -> Option<&dyn BlockShieldedExt> {
        if self.caps.has(Capability::BLOCK_SHIELDED_EXT) {
            let backend = self.downcast::<DbV1>().unwrap();
            Some(backend as &dyn BlockShieldedExt)
        } else {
            None
        }
    }

    pub(crate) fn compact_block(&self) -> Option<&dyn CompactBlockExt> {
        if self.caps.has(Capability::COMPACT_BLOCK_EXT) {
            let backend = self.downcast::<DbV1>().unwrap();
            Some(backend as &dyn CompactBlockExt)
        } else {
            None
        }
    }

    pub(crate) fn chain_block(&self) -> Option<&dyn ChainBlockExt> {
        if self.caps.has(Capability::CHAIN_BLOCK_EXT) {
            let backend = self.downcast::<DbV1>().unwrap();
            Some(backend as &dyn ChainBlockExt)
        } else {
            None
        }
    }

    pub(crate) fn transparent_hist(&self) -> Option<&dyn TransparentHistExt> {
        if self.caps.has(Capability::TRANSPARENT_HIST_EXT) {
            let backend = self.downcast::<DbV1>().unwrap();
            Some(backend as &dyn TransparentHistExt)
        } else {
            None
        }
    }
}
