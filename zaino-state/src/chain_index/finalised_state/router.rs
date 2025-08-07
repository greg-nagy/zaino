//! Implements the ZainoDB Router, used to selectively route database capabilities during major migrations.
//!
//! The Router allows incremental database migrations by splitting read and write capability groups between primary and shadow databases.
//! This design enables partial migrations without duplicating the entire chain database,
//! greatly reducing disk usage and ensuring minimal downtime.

use super::{
    capability::{Capability, DbCore, DbMetadata, DbRead, DbWrite},
    db::DbBackend,
};

use crate::{error::FinalisedStateError, ChainBlock, Hash, Height, StatusType};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

pub(crate) struct Router {
    /// Primary active database.
    primary: ArcSwap<DbBackend>,
    /// Shadow database, new version to be built during major migration.
    shadow: ArcSwap<Option<Arc<DbBackend>>>,
    /// Capability mask for primary database.
    primary_mask: AtomicU32,
    /// Capability mask dictating what database capalility (if any) should be served by the shadow.
    shadow_mask: AtomicU32,
}

/// Database version router.
///
/// Routes database capability to the correct database during major migrations.
impl Router {
    // ***** Router creation *****

    /// Creatues a new database router, setting primary the given database.
    ///
    /// Shadow is spawned as none and should only be set to some during major database migrations.
    pub(crate) fn new(primary: Arc<DbBackend>) -> Self {
        let cap = primary.capability();
        Self {
            primary: ArcSwap::from(primary),
            shadow: ArcSwap::from(Arc::new(None)),
            primary_mask: AtomicU32::new(cap.bits()),
            shadow_mask: AtomicU32::new(0),
        }
    }

    // ***** Capability router *****

    /// Return the database backend for a given capability, or an error if none is available.
    #[inline]
    pub(crate) fn backend(&self, cap: Capability) -> Result<Arc<DbBackend>, FinalisedStateError> {
        let bit = cap.bits();

        if self.shadow_mask.load(Ordering::Acquire) & bit != 0 {
            if let Some(shadow_db) = self.shadow.load().as_ref() {
                return Ok(Arc::clone(shadow_db));
            }
        }

        if self.primary_mask.load(Ordering::Acquire) & bit != 0 {
            return Ok(self.primary.load_full());
        }

        let feature_name = match cap {
            Capability::READ_CORE => "READ_CORE",
            Capability::WRITE_CORE => "WRITE_CORE",
            Capability::BLOCK_CORE_EXT => "BLOCK_CORE_EXT",
            Capability::BLOCK_TRANSPARENT_EXT => "BLOCK_TRANSPARENT_EXT",
            Capability::BLOCK_SHIELDED_EXT => "BLOCK_SHIELDED_EXT",
            Capability::COMPACT_BLOCK_EXT => "COMPACT_BLOCK_EXT",
            Capability::CHAIN_BLOCK_EXT => "CHAIN_BLOCK_EXT",
            Capability::TRANSPARENT_HIST_EXT => "TRANSPARENT_HIST_EXT",
            _ => "UNKNOWN_CAPABILITY",
        };

        Err(FinalisedStateError::FeatureUnavailable(feature_name))
    }

    // ***** Shadow database control *****
    //
    // These methods should only ever be used by the migration manager.

    /// Sets the shadow to the given database.
    pub(crate) fn set_shadow(&self, shadow: Arc<DbBackend>, caps: Capability) {
        self.shadow.store(Some(shadow).into());
        self.shadow_mask.store(caps.bits(), Ordering::Release);
    }

    /// Move additional capability bits to the *current* shadow.
    pub(crate) fn extend_shadow_caps(&self, caps: Capability) {
        self.shadow_mask.fetch_or(caps.bits(), Ordering::AcqRel);
    }

    /// Promotes the shadow database to primary, resets shadow,
    /// and updates the primary capability mask from the new backend.
    ///
    /// Used at the und of major migrations to move the active database to the new version.
    pub(crate) fn promote_shadow(&self) -> Arc<DbBackend> {
        let new_primary = self
            .shadow
            .swap(None.into())
            .as_ref()
            .clone()
            .expect("shadow missing during promote()");

        let cap = new_primary.capability();

        let old_primary = self.primary.swap(Arc::clone(&new_primary));
        self.primary_mask.store(cap.bits(), Ordering::Release);
        self.shadow_mask.store(0, Ordering::Release);

        old_primary
    }

    // ***** Primary database capability control *****

    /// Disables specific capabilities on the primary backend.
    pub(crate) fn limit_primary_caps(&self, caps: Capability) {
        self.primary_mask.fetch_and(!caps.bits(), Ordering::AcqRel);
    }

    /// Enables specific capabilities on the primary backend.
    pub(crate) fn extend_primary_caps(&self, caps: Capability) {
        self.primary_mask.fetch_or(caps.bits(), Ordering::AcqRel);
    }

    /// Overwrites the entire primary capability mask.
    pub(crate) fn set_primary_mask(&self, new_mask: Capability) {
        self.primary_mask.store(new_mask.bits(), Ordering::Release);
    }
}

// ***** Core DB functionality *****

#[async_trait]
impl DbCore for Router {
    async fn status(&self) -> StatusType {
        match self.backend(Capability::READ_CORE) {
            Ok(backend) => backend.status().await,
            Err(_) => StatusType::Busy,
        }
    }

    async fn shutdown(&self) -> Result<(), FinalisedStateError> {
        let primary_shutdown_result = self.primary.load_full().shutdown().await;

        let shadow_option = self.shadow.load();
        let shadow_shutdown_result = match shadow_option.as_ref() {
            Some(shadow_database) => shadow_database.shutdown().await,
            None => Ok(()),
        };

        primary_shutdown_result?;
        shadow_shutdown_result
    }
}

#[async_trait]
impl DbWrite for Router {
    async fn write_block(&self, blk: ChainBlock) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE)?.write_block(blk).await
    }

    async fn delete_block_at_height(&self, h: Height) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE)?
            .delete_block_at_height(h)
            .await
    }

    async fn delete_block(&self, blk: &ChainBlock) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE)?
            .delete_block(blk)
            .await
    }

    async fn update_metadata(&self, metadata: DbMetadata) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE)?
            .update_metadata(metadata)
            .await
    }
}

#[async_trait]
impl DbRead for Router {
    async fn db_height(&self) -> Result<Option<Height>, FinalisedStateError> {
        self.backend(Capability::READ_CORE)?.db_height().await
    }

    async fn get_block_height(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        self.backend(Capability::READ_CORE)?
            .get_block_height(hash)
            .await
    }

    async fn get_block_hash(&self, h: Height) -> Result<Hash, FinalisedStateError> {
        self.backend(Capability::READ_CORE)?.get_block_hash(h).await
    }

    async fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        self.backend(Capability::READ_CORE)?.get_metadata().await
    }
}
