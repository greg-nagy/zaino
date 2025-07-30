//! Implements the ZainoDB Router, used to selectively route database capabilities during major migrations.
//!
//! The Router allows incremental database migrations by splitting read and write capability groups between primary and shadow databases.
//! This design enables partial migrations without duplicating the entire chain database,
//! greatly reducing disk usage and ensuring minimal downtime.

use super::capability::{Capability, DbCore};

use arc_swap::ArcSwap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

/// Database version router.
///
/// Routes database capability to the correct database during major migrations.
struct Router {
    /// Primary active database.
    primary: ArcSwap<Arc<dyn DbCore + Send + Sync>>,
    /// Shadow database, new version to be built during major migration.
    shadow: ArcSwap<Option<Arc<dyn DbCore + Send + Sync>>>,
    /// Capability mask dictating what database capalility (if any) should be served by the shadow.
    mask: AtomicU32,
}

impl Router {
    // ***** Router creation *****

    /// Creatues a new database router, setting primary the given database.
    ///
    /// Shadow is spawned as none and should only be set to some during major database migrations.
    fn new(primary: Arc<dyn DbCore + Send + Sync>) -> Self {
        Self {
            primary: ArcSwap::from(Arc::new(primary)),
            shadow: ArcSwap::from(Arc::new(None)),
            mask: AtomicU32::new(0),
        }
    }

    // Database capability router

    /// Return the database for a given capability.
    #[inline]
    fn backend(&self, bit: Capability) -> Arc<dyn DbCore + Send + Sync> {
        if self.mask.load(Ordering::Acquire) & bit.bits() != 0 {
            self.shadow.load().as_ref().clone().unwrap()
        } else {
            self.primary.load_full().as_ref().clone()
        }
    }

    // Shadow database control.
    //
    // These methods should only ever be used by the migration manager.

    /// Sets the shadow to the given database.
    fn set_shadow(&self, shadow: Arc<dyn DbCore + Send + Sync + 'static>, caps: Capability) {
        self.shadow.store(Some(shadow).into());
        self.mask.store(caps.bits(), Ordering::Release);
    }

    /// Move additional capability bits to the *current* shadow.
    fn extend_shadow_caps(&self, caps: Capability) {
        self.mask.fetch_or(caps.bits(), Ordering::AcqRel);
    }

    /// Promotes the shadow database to primary.
    ///
    /// Used at the und of major migrations to move the active databsa to the new version.
    fn promote_shadow(&self) {
        let new_primary = <std::option::Option<Arc<dyn DbCore + Send + Sync>> as Clone>::clone(
            &self.shadow.swap(None.into()),
        )
        .expect("shadow missing");

        self.primary.store(new_primary.into());
        self.mask.store(0, Ordering::Release);
    }
}
