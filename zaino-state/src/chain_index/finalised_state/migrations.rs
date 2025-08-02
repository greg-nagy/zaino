//! This mod will hold the migration manager and migration implementations.
//!
//! This will be added in a subsequest pr.

use super::{
    capability::{Capability, DbVersion},
    db::DbBackend,
    router::Router,
};

use crate::{
    chain_index::source::BlockchainSourceInterface, config::BlockCacheConfig,
    error::FinalisedStateError,
};

use async_trait::async_trait;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationType {
    Patch,
    Minor,
    Major,
}

#[async_trait]
pub trait MigrationStep<T: BlockchainSourceInterface> {
    const FROM_VERSION: DbVersion;
    const TO_VERSION: DbVersion;
    const MIGRATION_TYPE: MigrationType;

    async fn migrate(
        router: Arc<Router>,
        cfg: BlockCacheConfig,
        source: T,
    ) -> Result<(), FinalisedStateError>;
}

struct Migration<T: BlockchainSourceInterface> {
    from: DbVersion,
    to: DbVersion,
    migration_type: MigrationType,
    migrate_fn: fn(
        Arc<Router>,
        &BlockCacheConfig,
        T,
    ) -> futures::future::BoxFuture<'static, Result<(), FinalisedStateError>>,
}

fn get_migration_step<T: BlockchainSourceInterface>(version: DbVersion) -> Option<Migration<T>> {
    let from = <Migration0_0_0To1_0_0 as MigrationStep<T>>::FROM_VERSION;
    if version == from {
        Some(Migration {
            from,
            to: <Migration0_0_0To1_0_0 as MigrationStep<T>>::TO_VERSION,
            migration_type: <Migration0_0_0To1_0_0 as MigrationStep<T>>::MIGRATION_TYPE,
            migrate_fn: |router, cfg, source| {
                Box::pin(Migration0_0_0To1_0_0::migrate(router, cfg.clone(), source))
            },
        })
    } else {
        None
    }
}

pub struct MigrationManager;

impl MigrationManager {
    pub async fn migrate_to<T>(
        router: Arc<Router>,
        cfg: &BlockCacheConfig,
        mut current_version: DbVersion,
        target_version: DbVersion,
        source: T,
    ) -> Result<(), FinalisedStateError>
    where
        T: BlockchainSourceInterface,
    {
        while current_version < target_version {
            let step = get_migration_step(current_version).ok_or_else(|| {
                FinalisedStateError::Custom(format!(
                    "Missing migration from version {}",
                    current_version
                ))
            })?;

            (step.migrate_fn)(Arc::clone(&router), cfg, source.clone()).await?;

            current_version = step.to;
        }

        if current_version != target_version {
            return Err(FinalisedStateError::Custom(format!(
                "Could not migrate fully: stopped at {} but target is {}",
                current_version, target_version
            )));
        }

        Ok(())
    }
}

// ***** Migrations *****

struct Migration0_0_0To1_0_0;

#[async_trait]
impl<T: BlockchainSourceInterface> MigrationStep<T> for Migration0_0_0To1_0_0 {
    const FROM_VERSION: DbVersion = DbVersion {
        major: 0,
        minor: 0,
        patch: 0,
    };
    const TO_VERSION: DbVersion = DbVersion {
        major: 1,
        minor: 0,
        patch: 0,
    };
    const MIGRATION_TYPE: MigrationType = MigrationType::Minor;

    /// The V0 database that we are migrating from was a lightwallet specific database
    /// that only built compact block data from sapling activation onwards.
    /// DbV1 is required to be built from genasis to correctly build the transparent address indexes.
    /// For this reason we do not do any partial builds in the V0 to V1 migration.
    /// We just run V0 as primary until V1 is fully built in shadow, then switch primary, deleting V0.
    async fn migrate(
        router: Arc<Router>,
        cfg: BlockCacheConfig,
        source: T,
    ) -> Result<(), FinalisedStateError> {
        let shadow = Arc::new(DbBackend::spawn_v1(&cfg).await?);
        router.set_shadow(Arc::clone(&shadow), Capability::empty());

        // Add chain source

        // build new db to db height

        // switch database
        router.promote_shadow();

        // Delete V0

        Ok(())
    }
}
