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
    /// Capability mask dictating what database capalility (if any) should be served by the shadow.
    mask: AtomicU32,
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
        Self {
            primary: ArcSwap::from(primary),
            shadow: ArcSwap::from(Arc::new(None)),
            mask: AtomicU32::new(0),
        }
    }

    // ***** Capability router *****

    /// Return the database for a given capability.
    #[inline]
    pub(crate) fn backend(&self, cap: Capability) -> Arc<DbBackend> {
        if self.mask.load(Ordering::Acquire) & cap.bits() != 0 {
            self.shadow.load().as_ref().clone().unwrap()
        } else {
            self.primary.load_full()
        }
    }

    // ***** Shadow database control *****
    //
    // These methods should only ever be used by the migration manager.

    /// Sets the shadow to the given database.
    pub(crate) fn set_shadow(&self, shadow: Arc<DbBackend>, caps: Capability) {
        self.shadow.store(Some(shadow).into());
        self.mask.store(caps.bits(), Ordering::Release);
    }

    /// Move additional capability bits to the *current* shadow.
    pub(crate) fn extend_shadow_caps(&self, caps: Capability) {
        self.mask.fetch_or(caps.bits(), Ordering::AcqRel);
    }

    /// Promotes the shadow database to primary.
    ///
    /// Used at the und of major migrations to move the active database to the new version.
    pub(crate) fn promote_shadow(&self) {
        let new_primary = self
            .shadow
            .swap(None.into())
            .as_ref()
            .clone()
            .expect("shadow missing during promote()");

        self.primary.store(new_primary.into());
        self.mask.store(0, Ordering::Release);
    }
}

// ***** Core DB functionality *****

#[async_trait]
impl DbCore for Router {
    async fn status(&self) -> StatusType {
        self.backend(Capability::READ_CORE).status().await
    }
    async fn shutdown(&self) -> Result<(), FinalisedStateError> {
        self.backend(Capability::READ_CORE).shutdown().await
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self as &dyn std::any::Any
    }
}

#[async_trait]
impl DbWrite for Router {
    async fn write_block(&self, blk: ChainBlock) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE).write_block(blk).await
    }

    async fn delete_block_at_height(&self, h: Height) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE)
            .delete_block_at_height(h)
            .await
    }

    async fn delete_block(&self, blk: &ChainBlock) -> Result<(), FinalisedStateError> {
        self.backend(Capability::WRITE_CORE).delete_block(blk).await
    }
}

#[async_trait]
impl DbRead for Router {
    async fn db_height(&self) -> Result<Option<Height>, FinalisedStateError> {
        self.backend(Capability::READ_CORE).db_height().await
    }

    async fn get_block_height(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        self.backend(Capability::READ_CORE)
            .get_block_height(hash)
            .await
    }

    async fn get_block_hash(&self, h: Height) -> Result<Hash, FinalisedStateError> {
        self.backend(Capability::READ_CORE).get_block_hash(h).await
    }

    async fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        self.backend(Capability::READ_CORE).get_metadata().await
    }
}
