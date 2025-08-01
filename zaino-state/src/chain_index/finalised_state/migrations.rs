//! This mod will hold the migration manager and migration implementations.
//!
//! This will be added in a subsequest pr.

use super::{
    capability::{Capability, DbVersion},
    db::DbBackend,
    router::Router,
};

use crate::{config::BlockCacheConfig, error::FinalisedStateError};

use async_trait::async_trait;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationType {
    Patch,
    Minor,
    Major,
}

#[async_trait]
pub trait MigrationStep {
    const FROM_VERSION: DbVersion;
    const TO_VERSION: DbVersion;
    const MIGRATION_TYPE: MigrationType;

    async fn migrate(router: Arc<Router>, cfg: BlockCacheConfig)
        -> Result<(), FinalisedStateError>;
}

struct Migration {
    from: DbVersion,
    to: DbVersion,
    migration_type: MigrationType,
    migrate_fn: fn(
        Arc<Router>,
        &BlockCacheConfig,
    ) -> futures::future::BoxFuture<'static, Result<(), FinalisedStateError>>,
}

// Returns the *next* migration step to be executed.
fn get_migration_step(version: DbVersion) -> Option<Migration> {
    match version {
        v if v == Migration0_0_0To1_0_0::FROM_VERSION => Some(Migration {
            from: Migration0_0_0To1_0_0::FROM_VERSION,
            to: Migration0_0_0To1_0_0::TO_VERSION,
            migration_type: Migration0_0_0To1_0_0::MIGRATION_TYPE,
            migrate_fn: |router, cfg| Box::pin(Migration0_0_0To1_0_0::migrate(router, cfg.clone())),
        }),

        _ => None,
    }
}

pub struct MigrationManager;

impl MigrationManager {
    pub async fn migrate_to(
        router: Arc<Router>,
        cfg: &BlockCacheConfig,
        mut current_version: DbVersion,
        target_version: DbVersion,
    ) -> Result<(), FinalisedStateError> {
        while current_version < target_version {
            let step = get_migration_step(current_version).ok_or_else(|| {
                FinalisedStateError::Custom(format!(
                    "Missing migration from version {}",
                    current_version
                ))
            })?;

            match step.migration_type {
                MigrationType::Patch | MigrationType::Minor => {
                    (step.migrate_fn)(Arc::clone(&router), cfg).await?;
                }
                MigrationType::Major => {
                    let shadow_db = Arc::new(DbBackend::spawn_v1(cfg).await?);
                    router.set_shadow(Arc::clone(&shadow_db), Capability::empty());
                    (step.migrate_fn)(Arc::clone(&router), cfg).await?;
                    router.promote_shadow();
                }
            }

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
impl MigrationStep for Migration0_0_0To1_0_0 {
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

    async fn migrate(
        _router: Arc<Router>,
        _cfg: BlockCacheConfig,
    ) -> Result<(), FinalisedStateError> {
        // partially build new database with old data

        // move capability to shadow
        // (how are new blocks added at this point?)
        // do we move to primary here?

        // build new database from block data from validator

        // switch database

        Ok(())

        // NOTE: this needs a blocksource.
        // NOTE: use DbV1 directly here?

        // NOTE / WARNING: add from_db method for DbBackend
        // NOTE / WARNING: create DbV1 in this fn, clone into router and use internal for migration
        // NOTE / WARNING: create, set, extend, and promote shadow thi this fn.
    }
}
