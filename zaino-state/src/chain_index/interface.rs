use crate::error::{ChainIndexError, ChainIndexErrorKind, FinalisedStateError};
use std::future::Future;
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::non_finalised_state::NonfinalizedBlockCacheSnapshot;
use super::source::BlockchainSourceInterface;
use super::types::{self, ChainBlock};
use super::{ChainIndex, NodeBackedChainIndex};
use futures::Stream;
use tokio_stream::StreamExt;
pub use zebra_chain::parameters::Network as ZebraNetwork;
use zebra_chain::serialization::ZcashSerialize;
use zebra_state::HashOrHeight;

impl<Source: BlockchainSourceInterface> NodeBackedChainIndex<Source> {
    pub(super) fn start_sync_loop(
        &self,
    ) -> tokio::task::JoinHandle<
        Result<std::convert::Infallible, super::non_finalised_state::SyncError>,
    > {
        let nfs = self.non_finalized_state.clone();
        let fs = self.finalized_db.clone();
        tokio::task::spawn(async move {
            loop {
                nfs.sync(fs.clone()).await?;
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

    async fn blocks_containing_transaction<'snapshot, 'self_lt, 'iter>(
        &'self_lt self,
        snapshot: &'snapshot NonfinalizedBlockCacheSnapshot,
        txid: [u8; 32],
    ) -> Result<impl Iterator<Item = ChainBlock> + use<'iter, Source>, FinalisedStateError>
    where
        'snapshot: 'iter,
        'self_lt: 'iter,
    {
        Ok(snapshot
            .blocks
            .values()
            .filter_map(move |block| {
                block.transactions().iter().find_map(|transaction| {
                    if *transaction.txid() == txid {
                        Some(block)
                    } else {
                        None
                    }
                })
            })
            .cloned()
            .chain(
                match self
                    .finalized_state
                    .get_tx_location(&crate::Hash(txid))
                    .await?
                {
                    Some(tx_location) => {
                        self.finalized_state
                            .get_chain_block(crate::Height(tx_location.block_height()))
                            .await?
                    }

                    None => None,
                }
                .into_iter(),
            ))
    }
}

impl<Source: BlockchainSourceInterface> ChainIndex for NodeBackedChainIndex<Source> {
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
        let Some(block) = self
            .blocks_containing_transaction(snapshot, txid)
            .await?
            .next()
        else {
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
    ) -> impl Future<
        Output = Result<
            HashMap<super::types::Hash, std::option::Option<super::types::Height>>,
            ChainIndexError,
        >,
    > {
        async move {
            Ok(self
                .blocks_containing_transaction(snapshot, txid)
                .await?
                .map(|block| (*block.hash(), block.height()))
                .collect())
        }
    }
}

/// A snapshot of the non-finalized state, for consistent queries
pub trait NonFinalizedSnapshot {
    /// Hash -> block
    fn get_chainblock_by_hash(&self, target_hash: &types::Hash) -> Option<&ChainBlock>;
    /// Height -> block
    fn get_chainblock_by_height(&self, target_height: &types::Height) -> Option<&ChainBlock>;
}

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

#[cfg(test)]
mod mockchain_tests {
    use std::path::PathBuf;

    use tempfile::TempDir;
    use zaino_proto::proto::compact_formats::CompactBlock;
    use zebra_chain::serialization::ZcashDeserializeInto;

    use crate::{
        bench::BlockCacheConfig,
        chain_index::{
            source::test::MockchainSource,
            tests::vectors::{build_mockchain_source, load_test_vectors},
        },
    };

    async fn load_test_vectors_and_sync_chain_index() -> (
        Vec<(
            u32,
            ChainBlock,
            CompactBlock,
            zebra_chain::block::Block,
            (
                zebra_chain::sapling::tree::Root,
                u64,
                zebra_chain::orchard::tree::Root,
                u64,
            ),
        )>,
        NodeBackedChainIndex<MockchainSource>,
    ) {
        let (blocks, _faucet, _recipient) = load_test_vectors().unwrap();

        let source = build_mockchain_source(blocks.clone());
        let temp_dir: TempDir = tempfile::tempdir().unwrap();
        let db_path: PathBuf = temp_dir.path().to_path_buf();

        let config = BlockCacheConfig {
            map_capacity: None,
            map_shard_amount: None,
            db_version: 1,
            db_path,
            db_size: None,
            network: zebra_chain::parameters::Network::new_regtest(
                zebra_chain::parameters::testnet::ConfiguredActivationHeights {
                    before_overwinter: Some(1),
                    overwinter: Some(1),
                    sapling: Some(1),
                    blossom: Some(1),
                    heartwood: Some(1),
                    canopy: Some(1),
                    nu5: Some(1),
                    nu6: Some(1),
                    // see https://zips.z.cash/#nu6-1-candidate-zips for info on NU6.1
                    nu6_1: None,
                    nu7: None,
                },
            ),
            no_sync: false,
            no_db: false,
        };

        let indexer = NodeBackedChainIndex::new(source, config).await.unwrap();
        loop {
            let nonfinalized_snapshot = ChainIndex::snapshot_nonfinalized_state(&indexer);
            if nonfinalized_snapshot.blocks.len() != 1 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        (blocks, indexer)
    }

    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_mock_range() {
        let (blocks, indexer) = load_test_vectors_and_sync_chain_index().await;
        let nonfinalized_snapshot = indexer.snapshot_nonfinalized_state();

        let start = crate::Height(0);

        let indexer_blocks =
            ChainIndex::get_block_range(&indexer, &nonfinalized_snapshot, start, None)
                .unwrap()
                .collect::<Vec<_>>()
                .await;

        for (i, block) in indexer_blocks.into_iter().enumerate() {
            let parsed_block = block
                .unwrap()
                .zcash_deserialize_into::<zebra_chain::block::Block>()
                .unwrap();

            let expected_block = &blocks[i].3;
            assert_eq!(&parsed_block, expected_block);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_raw_transaction() {
        let (blocks, indexer) = load_test_vectors_and_sync_chain_index().await;
        let nonfinalized_snapshot = indexer.snapshot_nonfinalized_state();
        for expected_transaction in blocks
            .into_iter()
            .flat_map(|block| block.3.transactions.into_iter())
        {
            let zaino_transaction = indexer
                .get_raw_transaction(&nonfinalized_snapshot, expected_transaction.hash().0)
                .await
                .unwrap()
                .unwrap()
                .zcash_deserialize_into::<zebra_chain::transaction::Transaction>()
                .unwrap();
            assert_eq!(expected_transaction.as_ref(), &zaino_transaction)
        }
    }
}
