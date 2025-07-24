//! ZainoDbReader: Read only view onto a running ZainoDB
//!
//! This should be used to fetch chain data in *all* cases.

use crate::{
    chain_index::types::AddrEventBytes, error::FinalisedStateError, AddrScript, BlockHeaderData,
    ChainBlock, CommitmentTreeData, Hash, Height, OrchardCompactTx, OrchardTxList, Outpoint,
    SaplingCompactTx, SaplingTxList, StatusType, TransparentCompactTx, TransparentTxList, TxIndex,
    TxidList,
};

use super::{
    capability::{
        BlockCoreExt, BlockShieldedExt, BlockTransparentExt, ChainBlockExt, CompactBlockExt,
        DbMetadata, TransparentHistExt,
    },
    ZainoDB,
};

use std::sync::Arc;

/// Immutable view onto an already-running [`ZainoDB`].
///
/// Carries a plain reference with the same lifetime as the parent DB
pub(crate) struct DbReader {
    pub(crate) inner: Arc<ZainoDB>,
}

impl DbReader {
    // ***** DB Core *****

    /// Returns the status of the serving ZainoDB.
    pub(crate) async fn status(&self) -> StatusType {
        self.inner.status().await
    }

    /// Returns the greatest block `Height` stored in the db
    /// (`None` if the DB is still empty).
    pub(crate) async fn db_height(&self) -> Result<Option<Height>, FinalisedStateError> {
        self.inner.db_height().await
    }

    /// Fetch database metadata.
    pub(crate) async fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        self.inner.get_metadata().await
    }

    /// Awaits untile the DB returns a Ready status.
    pub(crate) async fn wait_until_ready(&self) {
        self.inner.wait_until_ready().await
    }

    /// Fetch the block height in the main chain for a given block hash.
    pub(crate) async fn get_block_height(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        self.inner.get_block_height(hash).await
    }

    /// Fetch the block hash in the main chain for a given block height.
    pub(crate) async fn get_block_hash(&self, height: Height) -> Result<Hash, FinalisedStateError> {
        self.inner.get_block_hash(height).await
    }

    // /// Fetch the height range for the given block hashes.
    // pub(crate) async fn get_block_range_by_hash(
    //     &self,
    //     start_hash: Hash,
    //     end_hash: Hash,
    // ) -> Result<(Height, Height), FinalisedStateError> {
    //     self.inner
    //         .get_block_range_by_hash(start_hash, end_hash)
    //         .await
    // }

    // ***** Block Core Ext *****

    fn block_core_ext(&self) -> Result<&dyn BlockCoreExt, FinalisedStateError> {
        self.inner
            .block_core()
            .ok_or_else(|| FinalisedStateError::FeatureUnavailable("block_core"))
    }

    /// Fetch the TxIndex for the given txid, transaction data is indexed by Txidex internally.
    pub(crate) async fn get_tx_index(
        &self,
        txid: &Hash,
    ) -> Result<Option<TxIndex>, FinalisedStateError> {
        self.block_core_ext()?.get_tx_index(txid).await
    }

    /// Fetch block header data by height.
    pub async fn get_block_header(
        &self,
        height: Height,
    ) -> Result<BlockHeaderData, FinalisedStateError> {
        self.block_core_ext()?.get_block_header(height).await
    }

    /// Fetches block headers for the given height range.
    pub async fn get_block_range_headers(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<BlockHeaderData>, FinalisedStateError> {
        self.block_core_ext()?
            .get_block_range_headers(start, end)
            .await
    }

    /// Fetch the txid bytes for a given TxIndex.
    pub async fn get_txid(&self, idx: TxIndex) -> Result<Hash, FinalisedStateError> {
        self.block_core_ext()?.get_txid(idx).await
    }

    /// Fetch block txids by height.
    pub async fn get_block_txids(&self, h: Height) -> Result<TxidList, FinalisedStateError> {
        self.block_core_ext()?.get_block_txids(h).await
    }

    /// Fetches block txids for the given height range.
    pub async fn get_block_range_txids(
        &self,
        s: Height,
        e: Height,
    ) -> Result<Vec<TxidList>, FinalisedStateError> {
        self.block_core_ext()?.get_block_range_txids(s, e).await
    }

    // ***** Block Transparent Ext *****

    fn block_transparent_ext(&self) -> Result<&dyn BlockTransparentExt, FinalisedStateError> {
        self.inner
            .block_transparent()
            .ok_or_else(|| FinalisedStateError::FeatureUnavailable("block_transparent"))
    }

    /// Fetch the serialized TransparentCompactTx for the given TxIndex, if present.
    pub(crate) async fn get_transparent(
        &self,
        tx_index: TxIndex,
    ) -> Result<Option<TransparentCompactTx>, FinalisedStateError> {
        self.block_transparent_ext()?
            .get_transparent(tx_index)
            .await
    }

    /// Fetch block transparent transaction data by height.
    pub(crate) async fn get_block_transparent(
        &self,
        height: Height,
    ) -> Result<TransparentTxList, FinalisedStateError> {
        self.block_transparent_ext()?
            .get_block_transparent(height)
            .await
    }

    /// Fetches block transparent tx data for the given height range.
    pub(crate) async fn get_block_range_transparent(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<TransparentTxList>, FinalisedStateError> {
        self.block_transparent_ext()?
            .get_block_range_transparent(start, end)
            .await
    }

    // ***** Block shielded Ext *****

    fn block_shielded_ext(&self) -> Result<&dyn BlockShieldedExt, FinalisedStateError> {
        self.inner
            .block_shielded()
            .ok_or_else(|| FinalisedStateError::FeatureUnavailable("block_shielded"))
    }

    /// Fetch the serialized SaplingCompactTx for the given TxIndex, if present.
    pub(crate) async fn get_sapling(
        &self,
        tx_index: TxIndex,
    ) -> Result<Option<SaplingCompactTx>, FinalisedStateError> {
        self.block_shielded_ext()?.get_sapling(tx_index).await
    }

    /// Fetch block sapling transaction data by height.
    pub(crate) async fn get_block_sapling(
        &self,
        height: Height,
    ) -> Result<SaplingTxList, FinalisedStateError> {
        self.block_shielded_ext()?.get_block_sapling(height).await
    }

    /// Fetches block sapling tx data for the given height range.
    pub(crate) async fn get_block_range_sapling(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<SaplingTxList>, FinalisedStateError> {
        self.block_shielded_ext()?
            .get_block_range_sapling(start, end)
            .await
    }

    /// Fetch the serialized OrchardCompactTx for the given TxIndex, if present.
    pub(crate) async fn get_orchard(
        &self,
        tx_index: TxIndex,
    ) -> Result<Option<OrchardCompactTx>, FinalisedStateError> {
        self.block_shielded_ext()?.get_orchard(tx_index).await
    }

    /// Fetch block orchard transaction data by height.
    pub(crate) async fn get_block_orchard(
        &self,
        height: Height,
    ) -> Result<OrchardTxList, FinalisedStateError> {
        self.block_shielded_ext()?.get_block_orchard(height).await
    }

    /// Fetches block orchard tx data for the given height range.
    pub(crate) async fn get_block_range_orchard(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<OrchardTxList>, FinalisedStateError> {
        self.block_shielded_ext()?
            .get_block_range_orchard(start, end)
            .await
    }

    /// Fetch block commitment tree data by height.
    pub(crate) async fn get_block_commitment_tree_data(
        &self,
        height: Height,
    ) -> Result<CommitmentTreeData, FinalisedStateError> {
        self.block_shielded_ext()?
            .get_block_commitment_tree_data(height)
            .await
    }

    /// Fetches block commitment tree data for the given height range.
    pub(crate) async fn get_block_range_commitment_tree_data(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<CommitmentTreeData>, FinalisedStateError> {
        self.block_shielded_ext()?
            .get_block_range_commitment_tree_data(start, end)
            .await
    }

    // ***** Transparent Hist Ext *****
    fn transparent_hist_ext(&self) -> Result<&dyn TransparentHistExt, FinalisedStateError> {
        self.inner
            .transparent_hist()
            .ok_or_else(|| FinalisedStateError::FeatureUnavailable("transparent_hist"))
    }

    /// Fetch all address history records for a given transparent address.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more valid records exist,
    /// - `Ok(None)` if no records exist (not an error),
    /// - `Err(...)` if any decoding or DB error occurs.
    pub(crate) async fn addr_records(
        &self,
        addr_script: AddrScript,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        self.transparent_hist_ext()?.addr_records(addr_script).await
    }

    /// Fetch all address history records for a given address and TxIndex.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more matching records are found at that index,
    /// - `Ok(None)` if no matching records exist (not an error),
    /// - `Err(...)` on decode or DB failure.
    pub(crate) async fn addr_and_index_records(
        &self,
        addr_script: AddrScript,
        tx_index: TxIndex,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        self.transparent_hist_ext()?
            .addr_and_index_records(addr_script, tx_index)
            .await
    }

    /// Fetch all distinct `TxIndex` values for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more matching records are found,
    /// - `Ok(None)` if no matches found (not an error),
    /// - `Err(...)` on decode or DB failure.
    pub(crate) async fn addr_tx_indexes_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<TxIndex>>, FinalisedStateError> {
        self.transparent_hist_ext()?
            .addr_tx_indexes_by_range(addr_script, start_height, end_height)
            .await
    }

    /// Fetch all UTXOs (unspent mined outputs) for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Each entry is `(TxIndex, vout, value)`.
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more UTXOs are found,
    /// - `Ok(None)` if none found (not an error),
    /// - `Err(...)` on decode or DB failure.
    pub(crate) async fn addr_utxos_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<(TxIndex, u16, u64)>>, FinalisedStateError> {
        self.transparent_hist_ext()?
            .addr_utxos_by_range(addr_script, start_height, end_height)
            .await
    }

    /// Computes the transparent balance change for `addr_script` over the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Includes:
    /// - `+value` for mined outputs
    /// - `−value` for spent inputs
    ///
    /// Returns the signed net value as `i64`, or error on failure.
    pub(crate) async fn addr_balance_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        self.transparent_hist_ext()?
            .addr_balance_by_range(addr_script, start_height, end_height)
            .await
    }

    /// Fetch the `TxIndex` that spent a given outpoint, if any.
    ///
    /// Returns:
    /// - `Ok(Some(TxIndex))` if the outpoint is spent.
    /// - `Ok(None)` if no entry exists (not spent or not known).
    /// - `Err(...)` on deserialization or DB error.
    pub(crate) async fn get_outpoint_spender(
        &self,
        outpoint: Outpoint,
    ) -> Result<Option<TxIndex>, FinalisedStateError> {
        self.transparent_hist_ext()?
            .get_outpoint_spender(outpoint)
            .await
    }

    /// Fetch the `TxIndex` entries for a batch of outpoints.
    ///
    /// For each input:
    /// - Returns `Some(TxIndex)` if spent,
    /// - `None` if not found,
    /// - or returns `Err` immediately if any DB or decode error occurs.
    pub(crate) async fn get_outpoint_spenders(
        &self,
        outpoints: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxIndex>>, FinalisedStateError> {
        self.transparent_hist_ext()?
            .get_outpoint_spenders(outpoints)
            .await
    }

    // ***** ChainBlock Ext *****

    fn chain_block_ext(&self) -> Result<&dyn ChainBlockExt, FinalisedStateError> {
        self.inner
            .chain_block()
            .ok_or_else(|| FinalisedStateError::FeatureUnavailable("chain_block"))
    }

    /// Returns the ChainBlock for the given Height.
    ///
    /// TODO: Add separate range fetch method!
    pub(crate) async fn get_chain_block(
        &self,
        height: Height,
    ) -> Result<ChainBlock, FinalisedStateError> {
        self.chain_block_ext()?.get_chain_block(height).await
    }

    // ***** CompactBlock Ext *****

    fn compact_block_ext(&self) -> Result<&dyn CompactBlockExt, FinalisedStateError> {
        self.inner
            .compact_block()
            .ok_or_else(|| FinalisedStateError::FeatureUnavailable("compact_block"))
    }

    /// Returns the CompactBlock for the given Height.
    ///
    /// TODO: Add separate range fetch method!
    pub(crate) async fn get_compact_block(
        &self,
        height: Height,
    ) -> Result<zaino_proto::proto::compact_formats::CompactBlock, FinalisedStateError> {
        self.compact_block_ext()?.get_compact_block(height).await
    }
}
