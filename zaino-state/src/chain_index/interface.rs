use crate::error::{ChainIndexError, ChainIndexErrorKind, FinalisedStateError};
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use super::non_finalised_state::NonfinalizedBlockCacheSnapshot;
use super::source::BlockchainSourceInterface;
use super::types::{self, ChainBlock};
use super::{ChainIndex, NodeBackedChainIndex};
use futures::Stream;
use tokio_stream::StreamExt;
pub use zebra_chain::parameters::Network as ZebraNetwork;
use zebra_chain::serialization::ZcashSerialize;
use zebra_state::HashOrHeight;

impl NodeBackedChainIndex {
    pub(super) fn start_sync_loop(
        &self,
    ) -> tokio::task::JoinHandle<
        Result<std::convert::Infallible, super::non_finalised_state::SyncError>,
    > {
        let nfs = self.non_finalized_state.clone();
        tokio::task::spawn(async move {
            loop {
                nfs.sync().await?;
                //TODO: configure
                tokio::time::sleep(Duration::from_millis(500)).await
            }
        })
    }
    async fn get_fullblock_bytes_from_node(
        &self,
        id: HashOrHeight,
    ) -> Result<Option<Vec<u8>>, ChainIndexError> {
        self.non_finalized_state
            .source
            .get_block(id)
            .await
            .map_err(ChainIndexError::backing_validator)?
            .map(|bk| {
                bk.zcash_serialize_to_vec()
                    .map_err(ChainIndexError::backing_validator)
            })
            .transpose()
    }
    async fn hash_to_height(
        &self,
        non_finalized_snapshot: NonfinalizedBlockCacheSnapshot,
        hash: types::Hash,
    ) -> Result<Option<types::Height>, FinalisedStateError> {
        match non_finalized_snapshot
            .blocks
            .iter()
            .find_map(|(block_hash, block)| {
                if *block_hash == hash {
                    Some(block.height())
                } else {
                    None
                }
            }) {
            Some(height) => Ok(height),
            None => self.finalized_state.get_block_height(hash).await,
        }
    }
    async fn get_chainblock_by_hashorheight<'snapshot, 'self_lt, 'output>(
        &'self_lt self,
        non_finalized_snapshot: &'snapshot NonfinalizedBlockCacheSnapshot,
        hashorheight: &HashOrHeight,
    ) -> Result<Option<Cow<'output, ChainBlock>>, FinalisedStateError>
    where
        'snapshot: 'output,
        'self_lt: 'output,
    {
        if let Some(block) = non_finalized_snapshot.get_chainblock_by_hashorheight(hashorheight) {
            Ok(Some(Cow::Borrowed(block)))
        } else {
            let height = match hashorheight {
                HashOrHeight::Hash(hash) => {
                    match self
                        .finalized_state
                        .get_block_height(types::Hash::from(hash.0))
                        .await?
                    {
                        Some(h) => h,
                        None => return Ok(None),
                    }
                }
                HashOrHeight::Height(height) => types::Height(height.0),
            };
            (&self.finalized_state.to_reader())
                .get_chain_block(height)
                .await
                .map(|b| b.map(Cow::Owned))
        }
    }

    fn blocks_containing_transaction<'snapshot, 'self_lt, 'iter>(
        &'self_lt self,
        snapshot: &'snapshot NonfinalizedBlockCacheSnapshot,
        txid: [u8; 32],
    ) -> impl Iterator<Item = &'iter ChainBlock>
    where
        'snapshot: 'iter,
        'self_lt: 'iter,
    {
        //TODO: finalized state, mempool
        snapshot.blocks.values().filter_map(move |block| {
            block.transactions().iter().find_map(|transaction| {
                if *transaction.txid() == txid {
                    Some(block)
                } else {
                    None
                }
            })
        })
    }

    async fn get_range_boundary_hash(
        &self,
        boundary: types::Height,
        non_finalized_snapshot: &NonfinalizedBlockCacheSnapshot,
    ) -> Result<Option<types::Hash>, ChainIndexError> {
        match self
            .get_chainblock_by_hashorheight(
                non_finalized_snapshot,
                &HashOrHeight::Height(boundary.into()),
            )
            .await
            .map(|chain_block| chain_block.map(|cb| *cb.hash()))
        {
            Ok(hash) => Ok(hash),
            Err(FinalisedStateError::FeatureUnavailable(_)) => self
                .non_finalized_state
                .source
                .hash_or_height_to_hash(HashOrHeight::Height(boundary.into()))
                .await
                .map_err(|_e| ChainIndexError {
                    kind: ChainIndexErrorKind::InternalServerError,
                    message: "zaino still syncing, fallback to \
                            fetch from backing node failed"
                        .to_string(),
                    source: None,
                }),
            Err(e) => Err(e.into()),
        }
    }
}

impl ChainIndex for NodeBackedChainIndex {
    type Snapshot = Arc<NonfinalizedBlockCacheSnapshot>;
    type Error = ChainIndexError;

    /// Takes a snapshot of the non_finalized state. All NFS-interfacing query
    /// methods take a snapshot. The query will check the index
    /// it existed at the moment the snapshot was taken.
    fn snapshot_nonfinalized_state(&self) -> Self::Snapshot {
        self.non_finalized_state.get_snapshot()
    }

    /// Given inclusive start and end heights, stream all blocks
    /// between the given heights.
    /// Returns None if the specified end height
    /// is greater than the snapshot's tip
    fn get_block_range(
        &self,
        nonfinalized_snapshot: &Self::Snapshot,
        start: types::Height,
        end: std::option::Option<types::Height>,
    ) -> Option<impl Stream<Item = Result<Vec<u8>, Self::Error>>> {
        let end = end.unwrap_or(nonfinalized_snapshot.best_tip.0);
        if end <= nonfinalized_snapshot.best_tip.0 {
            Some(
                futures::stream::iter((start.0)..=(end.0)).then(move |height| async move {
                    match self
                        .finalized_state
                        .to_reader()
                        .get_block_hash(types::Height(height))
                        .await
                    {
                        Ok(Some(hash)) => {
                            return self
                                .get_fullblock_bytes_from_node(HashOrHeight::Hash(hash.into()))
                                .await?
                                .ok_or(ChainIndexError::database_hole(hash))
                        }
                        Err(e) => {
                            return Err(ChainIndexError {
                                kind: ChainIndexErrorKind::InternalServerError,
                                message: "".to_string(),
                                source: Some(Box::new(e)),
                            })
                        }
                        Ok(None) => {
                            match nonfinalized_snapshot
                                .get_chainblock_by_height(&types::Height(height))
                            {
                                Some(block) => {
                                    return self
                                        .get_fullblock_bytes_from_node(HashOrHeight::Hash(
                                            (*block.hash()).into(),
                                        ))
                                        .await?
                                        .ok_or(ChainIndexError::database_hole(block.hash()))
                                }
                                None => return Err(ChainIndexError::database_hole(height)),
                            }
                        }
                    }
                }),
            )
        } else {
            None
        }
    }

    /// Finds the newest ancestor of the given block on the main
    /// chain, or the block itself if it is on the main chain.
    fn find_fork_point(
        &self,
        snapshot: &Self::Snapshot,
        block_hash: &types::Hash,
    ) -> Result<Option<(types::Hash, types::Height)>, Self::Error> {
        let Some(block) = snapshot.as_ref().get_chainblock_by_hash(block_hash) else {
            // No fork point found. This is not an error,
            // as zaino does not guarentee knowledge of all sidechain data.
            return Ok(None);
        };
        if let Some(height) = block.height() {
            Ok(Some((*block.hash(), height)))
        } else {
            self.find_fork_point(snapshot, block.index().parent_hash())
        }
    }

    /// given a transaction id, returns the transaction
    async fn get_raw_transaction(
        &self,
        snapshot: &Self::Snapshot,
        txid: [u8; 32],
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(block) = self.blocks_containing_transaction(snapshot, txid).next() else {
            return Ok(None);
        };
        let full_block = self
            .non_finalized_state
            .source
            .get_block(HashOrHeight::Hash((*block.index().hash()).into()))
            .await
            .map_err(ChainIndexError::backing_validator)?
            .ok_or_else(|| ChainIndexError::database_hole(block.index().hash()))?;
        full_block
            .transactions
            .iter()
            .find(|transaction| transaction.hash().0 == txid)
            .map(ZcashSerialize::zcash_serialize_to_vec)
            .ok_or_else(|| ChainIndexError::database_hole(block.index().hash()))?
            .map_err(ChainIndexError::backing_validator)
            .map(Some)
    }

    /// Given a transaction ID, returns all known blocks containing this transaction
    /// At most one of these blocks will be on the best chain
    ///
    fn get_transaction_status(
        &self,
        snapshot: &Self::Snapshot,
        txid: [u8; 32],
    ) -> Result<
        HashMap<super::types::Hash, std::option::Option<super::types::Height>>,
        ChainIndexError,
    > {
        Ok(self
            .blocks_containing_transaction(snapshot, txid)
            .map(|block| (*block.hash(), block.height()))
            .collect())
    }
}

/// A snapshot of the non-finalized state, for consistent queries
pub trait NonFinalizedSnapshot {
    /// Hash -> block
    fn get_chainblock_by_hash(&self, target_hash: &types::Hash) -> Option<&ChainBlock>;
    /// Height -> block
    fn get_chainblock_by_height(&self, target_height: &types::Height) -> Option<&ChainBlock>;
}

trait NonFinalizedSnapshotGetHashOrHeight: NonFinalizedSnapshot {
    fn get_chainblock_by_hashorheight(&self, target: &HashOrHeight) -> Option<&ChainBlock> {
        match target {
            HashOrHeight::Hash(hash) => self.get_chainblock_by_hash(&types::Hash::from(*hash)),
            HashOrHeight::Height(height) => self.get_chainblock_by_height(&types::Height(height.0)),
        }
    }
}

impl<T: NonFinalizedSnapshot> NonFinalizedSnapshotGetHashOrHeight for T {}

impl NonFinalizedSnapshot for NonfinalizedBlockCacheSnapshot {
    fn get_chainblock_by_hash(&self, target_hash: &types::Hash) -> Option<&ChainBlock> {
        self.blocks.iter().find_map(|(hash, chainblock)| {
            if hash == target_hash {
                Some(chainblock)
            } else {
                None
            }
        })
    }
    fn get_chainblock_by_height(&self, target_height: &types::Height) -> Option<&ChainBlock> {
        self.heights_to_hashes.iter().find_map(|(height, hash)| {
            if height == target_height {
                self.get_chainblock_by_hash(hash)
            } else {
                None
            }
        })
    }
}
