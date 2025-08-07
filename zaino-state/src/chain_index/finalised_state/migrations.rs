//! This mod will hold the migration manager and migration implementations.
//!
//! This will be added in a subsequest pr.

use super::{
    capability::{
        BlockCoreExt, Capability, DbCore as _, DbRead, DbVersion, DbWrite, MigrationStatus,
    },
    db::DbBackend,
    router::Router,
};

use crate::{
    chain_index::source::BlockchainSourceInterface, config::BlockCacheConfig,
    error::FinalisedStateError, ChainBlock, ChainWork, Hash, Height,
};

use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;
use zebra_chain::parameters::NetworkKind;

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

#[allow(clippy::type_complexity)]
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
                    "Missing migration from version {current_version}"
                ))
            })?;

            (step.migrate_fn)(Arc::clone(&router), cfg, source.clone()).await?;

            current_version = step.to;
        }

        if current_version != target_version {
            return Err(FinalisedStateError::Custom(format!(
                "Could not migrate fully: stopped at {current_version} but target is {target_version}"
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
        info!("Starting v0.0.0 to v1.0.0 migration.");
        // Open V1 as shadow
        let shadow = Arc::new(DbBackend::spawn_v1(&cfg).await?);
        router.set_shadow(Arc::clone(&shadow), Capability::empty());

        let migration_status = shadow.get_metadata().await?.migration_status();

        match migration_status {
            MigrationStatus::Empty
            | MigrationStatus::PartialBuidInProgress
            | MigrationStatus::PartialBuildComplete
            | MigrationStatus::FinalBuildInProgress => {
                // build shadow to primary_db_height,
                // start from shadow_db_height in case database what shutdown mid-migration.
                let mut parent_chain_work = ChainWork::from_u256(0.into());

                let mut shadow_db_height = shadow.db_height().await?.unwrap_or(Height(0));
                let mut primary_db_height = router.db_height().await?.unwrap_or(Height(0));

                info!(
                    "Starting shadow database build, current database tips: v0:{} v1:{}",
                    primary_db_height, shadow_db_height
                );

                loop {
                    if shadow_db_height >= primary_db_height {
                        break;
                    }

                    if shadow_db_height.0 > 0 {
                        parent_chain_work = *shadow
                            .get_block_header(shadow_db_height)
                            .await?
                            .index()
                            .chainwork();
                    }

                    for height in (shadow_db_height.0 + 1)..=primary_db_height.0 {
                        let block = source
                            .get_block(zebra_state::HashOrHeight::Height(
                                zebra_chain::block::Height(height),
                            ))
                            .await?
                            .ok_or_else(|| {
                                FinalisedStateError::Custom(format!(
                                    "block not found at height {height}"
                                ))
                            })?;
                        let hash = Hash::from(block.hash().0);

                        let (sapling_root_data, orchard_root_data) =
                            source.get_commitment_tree_roots(hash).await?;
                        let (sapling_root, sapling_root_size) =
                            sapling_root_data.ok_or_else(|| {
                                FinalisedStateError::Custom(format!(
                        "sapling commitment tree data missing for block {hash:?} at height {height}"
                    ))
                            })?;
                        let (orchard_root, orchard_root_size) =
                            orchard_root_data.ok_or_else(|| {
                                FinalisedStateError::Custom(format!(
                        "orchard commitment tree data missing for block {hash:?} at height {height}"
                    ))
                            })?;

                        let chain_block = ChainBlock::try_from((
                            (*block).clone(),
                            sapling_root,
                            sapling_root_size as u32,
                            orchard_root,
                            orchard_root_size as u32,
                            parent_chain_work,
                            cfg.network.clone(),
                        ))
                        .map_err(FinalisedStateError::Custom)?;

                        shadow.write_block(chain_block).await?;
                    }

                    std::thread::sleep(std::time::Duration::from_millis(100));

                    shadow_db_height = shadow.db_height().await?.unwrap_or(Height(0));
                    primary_db_height = router.db_height().await?.unwrap_or(Height(0));
                }

                // update db metadata migration status
                let mut metadata = shadow.get_metadata().await?;
                metadata.migration_status = MigrationStatus::Complete;
                shadow.update_metadata(metadata).await?;

                info!("v1 database build complete.");
            }

            MigrationStatus::Complete => {
                // Migration complete, continue with DbV0 deletion.
            }
        }

        info!("promoting v1 database to primary.");

        // Promote V1 to primary
        let db_v0 = router.promote_shadow()?;

        // Delete V0
        tokio::spawn(async move {
            // Wait until all Arc<DbBackend> clones are dropped
            while Arc::strong_count(&db_v0) > 1 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }

            // shutdown database
            if let Err(e) = db_v0.shutdown().await {
                tracing::warn!("Old primary shutdown failed: {e}");
            }

            // Now safe to delete old database files
            let db_path_dir = match cfg.network.kind() {
                NetworkKind::Mainnet => "live",
                NetworkKind::Testnet => "test",
                NetworkKind::Regtest => "local",
            };
            let db_path = cfg.db_path.join(db_path_dir);

            info!("Wiping v0 database fropm disk.");

            match tokio::fs::remove_dir_all(&db_path).await {
                Ok(_) => tracing::info!("Deleted old database at {}", db_path.display()),
                Err(e) => tracing::error!(
                    "Failed to delete old database at {}: {}",
                    db_path.display(),
                    e
                ),
            }
        });

        info!("v0.0.0 to v1.0.0 migration complete.");

        Ok(())
    }
}
