//! Holds Database implementations by *major* version.

pub(crate) mod v0;
pub(crate) mod v1;

use v0::DbV0;
use v1::DbV1;

use crate::{
    chain_index::finalised_state::capability::{
        BlockCoreExt, BlockShieldedExt, BlockTransparentExt, ChainBlockExt, CompactBlockExt,
        DbCore, DbMetadata, DbRead, DbWrite, TransparentHistExt,
    },
    config::BlockCacheConfig,
    error::FinalisedStateError,
    AddrScript, BlockHeaderData, ChainBlock, CommitmentTreeData, Hash, Height, OrchardCompactTx,
    OrchardTxList, Outpoint, SaplingCompactTx, SaplingTxList, StatusType, TransparentCompactTx,
    TransparentTxList, TxLocation, TxidList,
};

use async_trait::async_trait;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

/// All concrete database implementations.
pub(crate) enum DbBackend {
    V0(DbV0),
    V1(DbV1),
}

// ***** Core database functionality *****

impl DbBackend {
    /// Spawn a v0 database.
    pub(crate) async fn spawn_v0(cfg: &BlockCacheConfig) -> Result<Self, FinalisedStateError> {
        Ok(Self::V0(DbV0::spawn(cfg).await?))
    }

    /// Spawn a v1 database.
    pub(crate) async fn spawn_v1(cfg: &BlockCacheConfig) -> Result<Self, FinalisedStateError> {
        Ok(Self::V1(DbV1::spawn(cfg).await?))
    }

    /// Waits until the ZainoDB returns a Ready status.
    pub(crate) async fn wait_until_ready(&self) {
        let mut ticker = interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            if self.status().await == StatusType::Ready {
                break;
            }
        }
    }
}

#[async_trait]
impl DbCore for DbBackend {
    async fn status(&self) -> StatusType {
        match self {
            Self::V0(db) => db.status().await,
            Self::V1(db) => db.status().await,
        }
    }

    async fn shutdown(&self) -> Result<(), FinalisedStateError> {
        match self {
            Self::V0(db) => db.shutdown().await,
            Self::V1(db) => db.shutdown().await,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        match self {
            Self::V0(db) => db,
            Self::V1(db) => db,
        }
    }
}

#[async_trait]
impl DbRead for DbBackend {
    async fn db_height(&self) -> Result<Option<Height>, FinalisedStateError> {
        match self {
            Self::V0(db) => db.db_height().await,
            Self::V1(db) => db.db_height().await,
        }
    }

    async fn get_block_height(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        match self {
            Self::V0(db) => db.get_block_height(hash).await,
            Self::V1(db) => db.get_block_height(hash).await,
        }
    }

    async fn get_block_hash(&self, height: Height) -> Result<Hash, FinalisedStateError> {
        match self {
            Self::V0(db) => db.get_block_hash(height).await,
            Self::V1(db) => db.get_block_hash(height).await,
        }
    }

    async fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        match self {
            Self::V0(db) => db.get_metadata().await,
            Self::V1(db) => db.get_metadata().await,
        }
    }
}

#[async_trait]
impl DbWrite for DbBackend {
    async fn write_block(&self, block: ChainBlock) -> Result<(), FinalisedStateError> {
        match self {
            Self::V0(db) => db.write_block(block).await,
            Self::V1(db) => db.write_block(block).await,
        }
    }

    async fn delete_block_at_height(&self, height: Height) -> Result<(), FinalisedStateError> {
        match self {
            Self::V0(db) => db.delete_block_at_height(height).await,
            Self::V1(db) => db.delete_block_at_height(height).await,
        }
    }

    async fn delete_block(&self, block: &ChainBlock) -> Result<(), FinalisedStateError> {
        match self {
            Self::V0(db) => db.delete_block(block).await,
            Self::V1(db) => db.delete_block(block).await,
        }
    }
}

// ***** Database capability extension traits *****

#[async_trait]
impl BlockCoreExt for DbBackend {
    async fn get_block_header(&self, h: Height) -> Result<BlockHeaderData, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_header(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_core")),
        }
    }

    async fn get_block_range_headers(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<BlockHeaderData>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_range_headers(s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_core")),
        }
    }

    async fn get_block_txids(&self, h: Height) -> Result<TxidList, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_txids(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_core")),
        }
    }

    async fn get_block_range_txids(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<TxidList>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_range_txids(s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_core")),
        }
    }

    async fn get_txid(&self, idx: TxLocation) -> Result<Hash, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_txid(idx).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_core")),
        }
    }

    async fn get_tx_location(
        &self,
        txid: &Hash,
    ) -> Result<Option<TxLocation>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_tx_location(txid).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_core")),
        }
    }
}

#[async_trait]
impl BlockTransparentExt for DbBackend {
    async fn get_transparent(
        &self,
        idx: TxLocation,
    ) -> Result<Option<TransparentCompactTx>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_transparent(idx).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_transparent")),
        }
    }

    async fn get_block_transparent(
        &self,
        h: Height,
    ) -> Result<TransparentTxList, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_transparent(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_transparent")),
        }
    }

    async fn get_block_range_transparent(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<TransparentTxList>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_range_transparent(s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_transparent")),
        }
    }
}

#[async_trait]
impl BlockShieldedExt for DbBackend {
    async fn get_sapling(
        &self,
        idx: TxLocation,
    ) -> Result<Option<SaplingCompactTx>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_sapling(idx).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_block_sapling(&self, h: Height) -> Result<SaplingTxList, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_sapling(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_block_range_sapling(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<SaplingTxList>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_range_sapling(s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_orchard(
        &self,
        idx: TxLocation,
    ) -> Result<Option<OrchardCompactTx>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_orchard(idx).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_block_orchard(&self, h: Height) -> Result<OrchardTxList, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_orchard(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_block_range_orchard(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<OrchardTxList>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_range_orchard(s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_block_commitment_tree_data(
        &self,
        h: Height,
    ) -> Result<CommitmentTreeData, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_commitment_tree_data(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }

    async fn get_block_range_commitment_tree_data(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<CommitmentTreeData>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_block_range_commitment_tree_data(s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("block_shielded")),
        }
    }
}

#[async_trait]
impl CompactBlockExt for DbBackend {
    async fn get_compact_block(
        &self,
        h: Height,
    ) -> Result<zaino_proto::proto::compact_formats::CompactBlock, FinalisedStateError> {
        #[allow(unreachable_patterns)]
        match self {
            Self::V0(db) => db.get_compact_block(h).await,
            Self::V1(db) => db.get_compact_block(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("compact_block")),
        }
    }
}

#[async_trait]
impl ChainBlockExt for DbBackend {
    async fn get_chain_block(&self, h: Height) -> Result<ChainBlock, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_chain_block(h).await,
            _ => Err(FinalisedStateError::FeatureUnavailable("chain_block")),
        }
    }
}

#[async_trait]
impl TransparentHistExt for DbBackend {
    async fn addr_records(
        &self,
        script: AddrScript,
    ) -> Result<Option<Vec<crate::chain_index::types::AddrEventBytes>>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.addr_records(script).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }

    async fn addr_and_index_records(
        &self,
        script: AddrScript,
        idx: TxLocation,
    ) -> Result<Option<Vec<crate::chain_index::types::AddrEventBytes>>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.addr_and_index_records(script, idx).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }

    async fn addr_tx_locations_by_range(
        &self,
        script: AddrScript,
        s: Height,
        e: Height,
    ) -> Result<Option<Vec<TxLocation>>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.addr_tx_locations_by_range(script, s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }

    async fn addr_utxos_by_range(
        &self,
        script: AddrScript,
        s: Height,
        e: Height,
    ) -> Result<Option<Vec<(TxLocation, u16, u64)>>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.addr_utxos_by_range(script, s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }

    async fn addr_balance_by_range(
        &self,
        script: AddrScript,
        s: Height,
        e: Height,
    ) -> Result<i64, FinalisedStateError> {
        match self {
            Self::V1(db) => db.addr_balance_by_range(script, s, e).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }

    async fn get_outpoint_spender(
        &self,
        o: Outpoint,
    ) -> Result<Option<TxLocation>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_outpoint_spender(o).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }

    async fn get_outpoint_spenders(
        &self,
        outs: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxLocation>>, FinalisedStateError> {
        match self {
            Self::V1(db) => db.get_outpoint_spenders(outs).await,
            _ => Err(FinalisedStateError::FeatureUnavailable(
                "transparent_history",
            )),
        }
    }
}
