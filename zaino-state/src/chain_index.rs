//! Holds Zaino's local chain index.
//!
//! Components:
//! - Mempool: Holds mempool transactions
//! - NonFinalisedState: Holds block data for the top 100 blocks of all chains.
//! - FinalisedState: Holds block data for the remainder of the best chain.
//!
//! - Chain: Holds chain / block structs used internally by the ChainIndex.
//!   - Holds fields required to:
//!     - a. Serve CompactBlock data dirctly.
//!     - b. Build trasparent tx indexes efficiently
//!   - NOTE: Full transaction and block data is served from the backend finalizer.

use source::{BlockchainSource, BlockchainSourceInterface};

pub mod encoding;
/// All state at least 100 blocks old
pub mod finalised_state;
/// The public interface to the chain index
pub mod interface;
/// State in the mempool, not yet on-chain
pub mod mempool;
/// State less than 100 blocks old, stored separately as it may be reorged
pub mod non_finalised_state;
/// BlockchainSource
pub mod source;
/// Common types used by the rest of this module
pub mod types;

#[cfg(test)]
mod tests;

/// The interface to the chain index
pub trait ChainIndex {
    /// A snapshot of the nonfinalized state, needed for atomic access
    type Snapshot;

    /// How it can fail
    type Error;

    /// Takes a snapshot of the non_finalized state. All NFS-interfacing query
    /// methods take a snapshot. The query will check the index
    /// it existed at the moment the snapshot was taken.
    fn snapshot_nonfinalized_state(&self) -> Self::Snapshot;

    /// Given inclusive start and end heights, stream all blocks
    /// between the given heights.
    /// Returns None if the specified end height
    /// is greater than the snapshot's tip
    #[allow(clippy::type_complexity)]
    fn get_block_range(
        &self,
        nonfinalized_snapshot: &Self::Snapshot,
        start: types::Height,
        end: Option<types::Height>,
    ) -> Option<impl futures::Stream<Item = Result<Vec<u8>, Self::Error>>>;
    /// Finds the newest ancestor of the given block on the main
    /// chain, or the block itself if it is on the main chain.
    fn find_fork_point(
        &self,
        snapshot: &Self::Snapshot,
        block_hash: &types::Hash,
    ) -> Result<Option<(types::Hash, types::Height)>, Self::Error>;
    /// given a transaction id, returns the transaction
    fn get_raw_transaction(
        &self,
        snapshot: &Self::Snapshot,
        txid: [u8; 32],
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, Self::Error>>;
    /// Given a transaction ID, returns all known hashes and heights of blocks
    /// containing that transaction. Height is None for blocks not on the best chain.
    fn get_transaction_status(
        &self,
        snapshot: &Self::Snapshot,
        txid: [u8; 32],
    ) -> impl std::future::Future<
        Output = Result<std::collections::HashMap<types::Hash, Option<types::Height>>, Self::Error>,
    >;
}
/// The combined index. Contains a view of the mempool, and the full
/// chain state, both finalized and non-finalized, to allow queries over
/// the entire chain at once. Backed by a source of blocks, either
/// a zebra ReadStateService (direct read access to a running
/// zebrad's database) or a jsonRPC connection to a validator.
///
/// Currently does not support mempool operations
pub struct NodeBackedChainIndex<Source: BlockchainSourceInterface = BlockchainSource> {
    // TODO: mempool
    non_finalized_state: std::sync::Arc<crate::NonFinalizedState<Source>>,
    finalized_db: std::sync::Arc<finalised_state::ZainoDB>,
    finalized_state: finalised_state::reader::DbReader,
}

impl<Source: BlockchainSourceInterface> NodeBackedChainIndex<Source> {
    /// Creates a new chainindex from a connection to a validator
    /// Currently this is a ReadStateService or JsonRpSeeConnector
    pub async fn new(
        source: Source,
        config: crate::config::BlockCacheConfig,
    ) -> Result<Self, crate::InitError>
where {
        use futures::TryFutureExt as _;

        let (non_finalized_state, finalized_db) = futures::try_join!(
            crate::NonFinalizedState::initialize(source.clone(), config.network.clone()),
            finalised_state::ZainoDB::spawn(config, source)
                .map_err(crate::InitError::FinalisedStateInitialzationError)
        )?;
        let finalized_db = std::sync::Arc::new(finalized_db);
        let chain_index = Self {
            non_finalized_state: std::sync::Arc::new(non_finalized_state),
            finalized_state: finalized_db.to_reader(),
            finalized_db,
        };
        chain_index.start_sync_loop();
        Ok(chain_index)
    }
}
