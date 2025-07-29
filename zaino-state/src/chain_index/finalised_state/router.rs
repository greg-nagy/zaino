//! Implements the ZainoDB Router, used to selectively route database capabilities during major migrations.
//!
//! The Router allows incremental database migrations by splitting read and write capabilities between primary and shadow databases.
//! This design enables partial migrations without duplicating the entire chain database, greatly reducing disk usage and ensuring minimal downtime.

use super::capability::{Capability, DbCore};

use arc_swap::{ArcSwap, ArcSwapOption};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

pub struct Router {
    primary: ArcSwap<dyn DbCore + Send + Sync + 'static>,
    shadow: ArcSwapOption<dyn DbCore + Send + Sync + 'static>,
    mask: AtomicU32,
}

impl Router {
    /// Create a router with only the primary backend active.
    pub fn new(primary: Arc<dyn DbCore + Send + Sync + 'static>) -> Self {
        Self {
            primary: ArcSwap::from(primary),
            shadow: ArcSwapOption::from(None::<Arc<dyn DbCore + Send + Sync + 'static>>),
            mask: AtomicU32::new(0),
        }
    }

    /// Lock‑free lookup.
    #[inline]
    pub fn backend(&self, bit: Capability) -> Arc<dyn DbCore + Send + Sync + 'static> {
        if self.mask.load(Ordering::Acquire) & bit.bits() != 0 {
            self.shadow.load().expect("mask bit set but shadow == None")
        } else {
            self.primary.load()
        }
    }

    /* ---------------- management helpers (used by migration code) ---------------- */

    /// Install `shadow` and route the selected capability bits there.
    pub fn set_shadow(&self, shadow: Arc<dyn DbCore + Send + Sync + 'static>, caps: Capability) {
        self.shadow.store(Some(shadow));
        self.mask.store(caps.bits(), Ordering::Release);
    }

    /// Atomic cut‑over: shadow → new primary, mask cleared, shadow set to None.
    pub fn promote_shadow(&self) {
        let new_primary = self
            .shadow
            .swap(None) // take the Arc, leave None
            .expect("shadow missing on promote");
        self.primary.store(new_primary);
        self.mask.store(0, Ordering::Release);
    }
}
