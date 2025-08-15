//! Traits and types for the blockchain source thats serves zaino, commonly a validator connection.

use std::{str::FromStr as _, sync::Arc};

use crate::chain_index::types::Hash;
use async_trait::async_trait;
use futures::{future::join, TryFutureExt as _};
use tower::{Service, ServiceExt as _};
use zaino_fetch::jsonrpsee::{
    connector::{JsonRpSeeConnector, RpcRequestError},
    response::{GetBlockError, GetBlockResponse, GetTransactionResponse, GetTreestateResponse},
};
use zcash_primitives::merkle_tree::read_commitment_tree;
use zebra_chain::serialization::ZcashDeserialize;
use zebra_state::{HashOrHeight, ReadResponse, ReadStateService};

/// A trait for accessing blockchain data from different backends.
#[async_trait]
pub trait BlockchainSource: Clone + Send + Sync + 'static {
    /// Returns the block by hash or height
    async fn get_block(
        &self,
        id: HashOrHeight,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::block::Block>>>;

    /// Returns the block commitment tree data by hash or height
    async fn get_commitment_tree_roots(
        &self,
        id: Hash,
    ) -> BlockchainSourceResult<(
        Option<(zebra_chain::sapling::tree::Root, u64)>,
        Option<(zebra_chain::orchard::tree::Root, u64)>,
    )>;

    /// Returns the complete list of txids currently in the mempool.
    async fn get_mempool_txids(
        &self,
    ) -> BlockchainSourceResult<Option<Vec<zebra_chain::transaction::Hash>>>;

    /// Returns the transaction by txid
    async fn get_transaction(
        &self,
        txid: Hash,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::transaction::Transaction>>>;

    /// Returns the hash of the block at the tip of the best chain.
    async fn get_best_block_hash(&self)
        -> BlockchainSourceResult<Option<zebra_chain::block::Hash>>;
}

/// An error originating from a blockchain source.
#[derive(Debug, thiserror::Error)]
pub enum BlockchainSourceError {
    /// Unrecoverable error.
    // TODO: Add logic for handling recoverable errors if any are identified
    // one candidate may be ephemerable network hiccoughs
    #[error("critical error in backing block source: {0}")]
    Unrecoverable(String),
}

/// Error type returned when invalid data is returned by the validator.
#[derive(thiserror::Error, Debug)]
#[error("data from validator invalid: {0}")]
pub struct InvalidData(String);

type BlockchainSourceResult<T> = Result<T, BlockchainSourceError>;

/// Currently the Mempool cannot utilise the mempool change endpoint in the ReadStateService,
/// for this reason the legacy jsonrpc interface is used until the Mempool updates required can be implemented.
///
/// Due to the difference between the mempool interface provided by the ReadStateService and the Json RPC service,
/// two seperate Mempool implementations will likely be required.
#[derive(Clone, Debug)]
pub struct State {
    /// Used to fetch chain data.
    pub read_state_service: ReadStateService,
    /// Temporarily used to fetch mempool data.
    pub mempool_fetcher: JsonRpSeeConnector,
}

/// A connection to a validator.
#[derive(Clone, Debug)]
pub enum ValidatorConnector {
    /// The connection is via direct read access to a zebrad's data file
    ///
    /// NOTE: See docs for State struct.
    State(State),
    /// We are connected to a zebrad, zcashd, or other zainod via JsonRpSee
    Fetch(JsonRpSeeConnector),
}

/// Methods that will dispatch to a ReadStateService or JsonRpSeeConnector
impl ValidatorConnector {
    pub(super) async fn get_block(
        &self,
        id: HashOrHeight,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::block::Block>>> {
        match self {
            ValidatorConnector::State(state) => match state
                .read_state_service
                .clone()
                .call(zebra_state::ReadRequest::Block(id))
                .await
            {
                Ok(zebra_state::ReadResponse::Block(block)) => Ok(block),
                Ok(otherwise) => panic!(
                    "Read Request of Block returned Read Response of {otherwise:#?} \n\
                    This should be deterministically unreachable"
                ),
                Err(e) => Err(BlockchainSourceError::Unrecoverable(e.to_string())),
            },
            ValidatorConnector::Fetch(json_rp_see_connector) => {
                match json_rp_see_connector
                    .get_block(id.to_string(), Some(0))
                    .await
                {
                    Ok(GetBlockResponse::Raw(raw_block)) => Ok(Some(Arc::new(
                        zebra_chain::block::Block::zcash_deserialize(raw_block.as_ref())
                            .map_err(|e| BlockchainSourceError::Unrecoverable(e.to_string()))?,
                    ))),
                    Ok(_) => unreachable!(),
                    Err(e) => match e {
                        RpcRequestError::Method(GetBlockError::MissingBlock(_)) => Ok(None),
                        RpcRequestError::ServerWorkQueueFull => Err(BlockchainSourceError::Unrecoverable("Work queue full. not yet implemented: handling of ephemeral network errors.".to_string())),
                        _ => Err(BlockchainSourceError::Unrecoverable(e.to_string())),
                    },
                }
            }
        }
    }

    pub(super) async fn get_commitment_tree_roots(
        &self,
        id: Hash,
    ) -> BlockchainSourceResult<(
        Option<(zebra_chain::sapling::tree::Root, u64)>,
        Option<(zebra_chain::orchard::tree::Root, u64)>,
    )> {
        match self {
            ValidatorConnector::State(state) => {
                let (sapling_tree_response, orchard_tree_response) =
                    join(
                        state.read_state_service.clone().call(
                            zebra_state::ReadRequest::SaplingTree(HashOrHeight::Hash(id.into())),
                        ),
                        state.read_state_service.clone().call(
                            zebra_state::ReadRequest::OrchardTree(HashOrHeight::Hash(id.into())),
                        ),
                    )
                    .await;
                let (sapling_tree, orchard_tree) = match (
                    //TODO: Better readstateservice error handling
                    sapling_tree_response
                        .map_err(|e| BlockchainSourceError::Unrecoverable(e.to_string()))?,
                    orchard_tree_response
                        .map_err(|e| BlockchainSourceError::Unrecoverable(e.to_string()))?,
                ) {
                    (ReadResponse::SaplingTree(saptree), ReadResponse::OrchardTree(orctree)) => {
                        (saptree, orctree)
                    }
                    (_, _) => panic!("Bad response"),
                };

                Ok((
                    sapling_tree
                        .as_deref()
                        .map(|tree| (tree.root(), tree.count())),
                    orchard_tree
                        .as_deref()
                        .map(|tree| (tree.root(), tree.count())),
                ))
            }
            ValidatorConnector::Fetch(json_rp_see_connector) => {
                let tree_responses = json_rp_see_connector
                    .get_treestate(id.to_string())
                    .await
                    // As MethodError contains a GetTreestateError, which is an enum with no variants,
                    // we don't need to account for it at all here
                    .map_err(|e| match e {
                        RpcRequestError::ServerWorkQueueFull => {
                            BlockchainSourceError::Unrecoverable(
                                "Not yet implemented: handle backing validator\
                                full queue"
                                    .to_string(),
                            )
                        }
                        _ => BlockchainSourceError::Unrecoverable(e.to_string()),
                    })?;
                let GetTreestateResponse {
                    sapling, orchard, ..
                } = tree_responses;
                let sapling_frontier = sapling
                    .commitments()
                    .final_state()
                    .as_ref()
                    .map(hex::decode)
                    .transpose()
                    .map_err(|_e| {
                        BlockchainSourceError::Unrecoverable(
                            InvalidData(format!("could not interpret sapling tree of block {id}"))
                                .to_string(),
                        )
                    })?
                    .as_deref()
                    .map(read_commitment_tree::<zebra_chain::sapling::tree::Node, _, 32>)
                    .transpose()
                    .map_err(|e| BlockchainSourceError::Unrecoverable(format!("io error: {e}")))?;
                let orchard_frontier = orchard
                    .commitments()
                    .final_state()
                    .as_ref()
                    .map(hex::decode)
                    .transpose()
                    .map_err(|_e| {
                        BlockchainSourceError::Unrecoverable(
                            InvalidData(format!("could not interpret orchard tree of block {id}"))
                                .to_string(),
                        )
                    })?
                    .as_deref()
                    .map(read_commitment_tree::<zebra_chain::orchard::tree::Node, _, 32>)
                    .transpose()
                    .map_err(|e| BlockchainSourceError::Unrecoverable(format!("io error: {e}")))?;
                let sapling_root = sapling_frontier
                    .map(|tree| {
                        zebra_chain::sapling::tree::Root::try_from(*tree.root().as_ref())
                            .map(|root| (root, tree.size() as u64))
                    })
                    .transpose()
                    .map_err(|e| {
                        BlockchainSourceError::Unrecoverable(format!("could not deser: {e}"))
                    })?;
                let orchard_root = orchard_frontier
                    .map(|tree| {
                        zebra_chain::orchard::tree::Root::try_from(tree.root().to_repr())
                            .map(|root| (root, tree.size() as u64))
                    })
                    .transpose()
                    .map_err(|e| {
                        BlockchainSourceError::Unrecoverable(format!("could not deser: {e}"))
                    })?;
                Ok((sapling_root, orchard_root))
            }
        }
    }

    pub(super) async fn get_mempool_txids(
        &self,
    ) -> BlockchainSourceResult<Option<Vec<zebra_chain::transaction::Hash>>> {
        let mempool_fetcher = match self {
            ValidatorConnector::State(state) => &state.mempool_fetcher,
            ValidatorConnector::Fetch(json_rp_see_connector) => json_rp_see_connector,
        };

        let txid_strings = mempool_fetcher
            .get_raw_mempool()
            .await
            .map_err(|e| {
                BlockchainSourceError::Unrecoverable(format!("could not fetch mempool data: {e}"))
            })?
            .transactions;

        let txids: Vec<zebra_chain::transaction::Hash> = txid_strings
            .into_iter()
            .map(|txid_str| {
                zebra_chain::transaction::Hash::from_str(&txid_str).map_err(|e| {
                    BlockchainSourceError::Unrecoverable(format!(
                        "invalid transaction id '{txid_str}': {e}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Some(txids))
    }

    pub(super) async fn get_transaction(
        &self,
        txid: Hash,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::transaction::Transaction>>> {
        match self {
            ValidatorConnector::State(State {
                read_state_service,
                mempool_fetcher,
            }) => {
                // Check state for transaction
                let mut read_state_service = read_state_service.clone();
                let mempool_fetcher = mempool_fetcher.clone();

                let txid_tr: zebra_chain::transaction::Hash =
                    zebra_chain::transaction::Hash::from(txid.0);

                let resp = read_state_service
                    .ready()
                    .and_then(|svc| svc.call(zebra_state::ReadRequest::Transaction(txid_tr)))
                    .await
                    .map_err(|e| {
                        BlockchainSourceError::Unrecoverable(format!("state read failed: {e}"))
                    })?;

                if let zebra_state::ReadResponse::Transaction(opt) = resp {
                    if let Some(found) = opt {
                        return Ok(Some((found).tx.clone()));
                    }
                } else {
                    unreachable!("unmatched response to a `Transaction` read request");
                }

                // Else check mempool for transaction.
                let mempool_txids = self.get_mempool_txids().await?.ok_or_else(|| {
                    BlockchainSourceError::Unrecoverable(
                        "could not fetch mempool transaction ids: none returned".to_string(),
                    )
                })?;

                if mempool_txids.contains(&txid_tr) {
                    let serialized_transaction = if let GetTransactionResponse::Raw(
                        serialized_transaction,
                    ) = mempool_fetcher
                        .get_raw_transaction(txid_tr.to_string(), Some(0))
                        .await
                        .map_err(|e| {
                            BlockchainSourceError::Unrecoverable(format!(
                                "could not fetch transaction data: {e}"
                            ))
                        })? {
                        serialized_transaction
                    } else {
                        return Err(BlockchainSourceError::Unrecoverable(
                            "could not fetch transaction data: non-raw response".to_string(),
                        ));
                    };
                    let transaction: zebra_chain::transaction::Transaction =
                        zebra_chain::transaction::Transaction::zcash_deserialize(
                            std::io::Cursor::new(serialized_transaction.as_ref()),
                        )
                        .map_err(|e| {
                            BlockchainSourceError::Unrecoverable(format!(
                                "could not deserialize transaction data: {e}"
                            ))
                        })?;
                    Ok(Some(transaction.into()))
                } else {
                    Ok(None)
                }
            }
            ValidatorConnector::Fetch(json_rp_see_connector) => {
                let serialized_transaction =
                    if let GetTransactionResponse::Raw(serialized_transaction) =
                        json_rp_see_connector
                            .get_raw_transaction(txid.to_string(), Some(0))
                            .await
                            .map_err(|e| {
                                BlockchainSourceError::Unrecoverable(format!(
                                    "could not fetch transaction data: {e}"
                                ))
                            })?
                    {
                        serialized_transaction
                    } else {
                        return Err(BlockchainSourceError::Unrecoverable(
                            "could not fetch transaction data: non-raw response".to_string(),
                        ));
                    };
                let transaction: zebra_chain::transaction::Transaction =
                    zebra_chain::transaction::Transaction::zcash_deserialize(std::io::Cursor::new(
                        serialized_transaction.as_ref(),
                    ))
                    .map_err(|e| {
                        BlockchainSourceError::Unrecoverable(format!(
                            "could not deserialize transaction data: {e}"
                        ))
                    })?;
                Ok(Some(transaction.into()))
            }
        }
    }

    pub(super) async fn get_best_block_hash(
        &self,
    ) -> BlockchainSourceResult<Option<zebra_chain::block::Hash>> {
        match self {
            ValidatorConnector::State(State {
                read_state_service,
                mempool_fetcher,
            }) => {
                match read_state_service.best_tip() {
                    Some((_height, hash)) => Ok(Some(hash)),
                    None => {
                        // try RPC if state read fails:
                        Ok(Some(
                            mempool_fetcher
                                .get_best_blockhash()
                                .await
                                .map_err(|e| {
                                    BlockchainSourceError::Unrecoverable(format!(
                                        "could not fetch best block hash from validator: {e}"
                                    ))
                                })?
                                .0,
                        ))
                    }
                }
            }
            ValidatorConnector::Fetch(json_rp_see_connector) => Ok(Some(
                json_rp_see_connector
                    .get_best_blockhash()
                    .await
                    .map_err(|e| {
                        BlockchainSourceError::Unrecoverable(format!(
                            "could not fetch best block hash from validator: {e}"
                        ))
                    })?
                    .0,
            )),
        }
    }
}

#[async_trait]
impl BlockchainSource for ValidatorConnector {
    async fn get_block(
        &self,
        id: HashOrHeight,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::block::Block>>> {
        self.get_block(id).await
    }

    async fn get_commitment_tree_roots(
        &self,
        id: Hash,
    ) -> BlockchainSourceResult<(
        Option<(zebra_chain::sapling::tree::Root, u64)>,
        Option<(zebra_chain::orchard::tree::Root, u64)>,
    )> {
        self.get_commitment_tree_roots(id).await
    }

    async fn get_mempool_txids(
        &self,
    ) -> BlockchainSourceResult<Option<Vec<zebra_chain::transaction::Hash>>> {
        self.get_mempool_txids().await
    }

    async fn get_transaction(
        &self,
        txid: Hash,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::transaction::Transaction>>> {
        self.get_transaction(txid).await
    }

    async fn get_best_block_hash(
        &self,
    ) -> BlockchainSourceResult<Option<zebra_chain::block::Hash>> {
        self.get_best_block_hash().await
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };
    use zebra_chain::{block::Block, orchard::tree as orchard, sapling::tree as sapling};
    use zebra_state::HashOrHeight;

    /// A test-only mock implementation of BlockchainReader using ordered lists by height.
    #[derive(Clone)]
    #[allow(clippy::type_complexity)]
    pub(crate) struct MockchainSource {
        blocks: Vec<Arc<Block>>,
        roots: Vec<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>,
        hashes: Vec<Hash>,
        active_chain_height: Arc<AtomicU32>,
    }

    impl MockchainSource {
        /// Creates a new MockchainSource.
        /// All inputs must be the same length, and ordered by ascending height starting from 0.
        #[allow(clippy::type_complexity)]
        pub(crate) fn new(
            blocks: Vec<Arc<Block>>,
            roots: Vec<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>,
            hashes: Vec<Hash>,
        ) -> Self {
            assert!(
                blocks.len() == roots.len() && roots.len() == hashes.len(),
                "All input vectors must be the same length"
            );

            let tip_index = blocks.len().saturating_sub(1) as u32;
            Self {
                blocks,
                roots,
                hashes,
                active_chain_height: Arc::new(AtomicU32::new(tip_index)),
            }
        }

        /// Creates a new MockchainSource, *with* an active chain height.
        ///
        /// Block will only be served up to the active chain height, with mempool data coming from
        /// the *next block in the chain.
        ///
        /// Blocks must be "mined" to extend the active chain height.
        ///
        /// All inputs must be the same length, and ordered by ascending height starting from 0.
        #[allow(clippy::type_complexity)]
        pub(crate) fn new_with_active_height(
            blocks: Vec<Arc<Block>>,
            roots: Vec<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>,
            hashes: Vec<Hash>,
            active_chain_height: u32,
        ) -> Self {
            assert!(blocks.len() == roots.len() && roots.len() == hashes.len());

            let max_tip = blocks.len().saturating_sub(1) as u32;
            assert!(
                active_chain_height <= max_tip,
                "active_chain_height must be in 0..=len-1"
            );

            Self {
                blocks,
                roots,
                hashes,
                active_chain_height: Arc::new(AtomicU32::new(active_chain_height)),
            }
        }

        pub(crate) fn mine_blocks(&self, blocks: u32) {
            let max_tip = self.blocks.len().saturating_sub(1) as u32;
            let _ = self.active_chain_height.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |current| {
                    let target = current.saturating_add(blocks).min(max_tip);
                    if target == current {
                        None
                    } else {
                        Some(target)
                    }
                },
            );
        }

        pub(crate) fn max_chain_height(&self) -> u32 {
            self.blocks.len().saturating_sub(1) as u32
        }

        pub(crate) fn active_height(&self) -> u32 {
            self.active_chain_height.load(Ordering::SeqCst)
        }

        fn height_to_index(&self, height: u32) -> Result<usize, BlockchainSourceError> {
            let index = height as usize;

            if index >= self.blocks.len() {
                return Err(BlockchainSourceError::Unrecoverable(format!(
                    "Height {height} is out of range (max height = {})",
                    self.blocks.len().saturating_sub(1)
                )));
            }

            Ok(index)
        }

        fn hash_to_index(
            &self,
            hash: &zebra_chain::block::Hash,
        ) -> Result<usize, BlockchainSourceError> {
            self.hashes
                .iter()
                .position(|h| h.0 == hash.0)
                .ok_or_else(|| {
                    BlockchainSourceError::Unrecoverable("Block hash not found".to_string())
                })
        }

        async fn get_block(&self, id: HashOrHeight) -> BlockchainSourceResult<Option<Arc<Block>>> {
            let active_tip = self.active_height() as usize;

            match id {
                HashOrHeight::Height(height) => {
                    let index = self.height_to_index(height.0)?;
                    if index <= active_height - 1 {
                        Ok(Some(Arc::clone(&self.blocks[index])))
                    } else {
                        Ok(None)
                    }
                }
                HashOrHeight::Hash(hash) => {
                    let index = self.hash_to_index(&hash)?;
                    if index <= active_height - 1 {
                        Ok(Some(Arc::clone(&self.blocks[index])))
                    } else {
                        Ok(None)
                    }
                }
            }
        }

        async fn get_commitment_tree_roots(
            &self,
            id: Hash,
        ) -> BlockchainSourceResult<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>
        {
            let active_height = self.active_height() as usize; // serve up to active tip

            if let Some(index) = self.hashes.iter().position(|h| h == &id) {
                if index <= active_tip {
                    Ok(self.roots[index])
                } else {
                    Ok((None, None))
                }
            } else {
                Ok((None, None))
            }
        }

        async fn get_mempool_txids(
            &self,
        ) -> BlockchainSourceResult<Option<Vec<zebra_chain::transaction::Hash>>> {
            let mempool_height = self.active_height() as usize + 1;

            let txids = if mempool_index < self.blocks.len() {
                self.blocks[mempool_index]
                    .transactions
                    .iter()
                    .map(|transaction| transaction.hash())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };

            Ok(Some(txids))
        }

        async fn get_transaction(
            &self,
            txid: Hash,
        ) -> BlockchainSourceResult<Option<Arc<zebra_chain::transaction::Transaction>>> {
            let txid_tr: zebra_chain::transaction::Hash =
                zebra_chain::transaction::Hash::from(txid.0);

            let active_height = self.active_height() as usize;
            let mempool_height = active_height + 1;

            for index in 0..=active_height {
                if index >= self.blocks.len() {
                    break;
                }
                if let Some(found) = self.blocks[index]
                    .transactions
                    .iter()
                    .find(|transaction| transaction.hash() == txid_tr)
                {
                    return Ok(Some(Arc::clone(found)));
                }
            }

            if mempool_index < self.blocks.len() {
                if let Some(found) = self.blocks[mempool_index]
                    .transactions
                    .iter()
                    .find(|transaction| transaction.hash() == txid_tr)
                {
                    return Ok(Some(Arc::clone(found)));
                }
            }

            Ok(None)
        }

        async fn get_best_block_hash(
            &self,
        ) -> BlockchainSourceResult<Option<zebra_chain::block::Hash>> {
            let active_tip = self.active_height() as usize;

            if self.blocks.is_empty() || active_tip >= self.blocks.len() {
                return Ok(None);
            }

            Ok(Some(self.blocks[active_tip].hash()))
        }
    }

    #[async_trait]
    impl BlockchainSource for MockchainSource {
        async fn get_block(
            &self,
            id: HashOrHeight,
        ) -> BlockchainSourceResult<Option<Arc<zebra_chain::block::Block>>> {
            self.get_block(id).await
        }

        async fn get_commitment_tree_roots(
            &self,
            id: Hash,
        ) -> BlockchainSourceResult<(
            Option<(zebra_chain::sapling::tree::Root, u64)>,
            Option<(zebra_chain::orchard::tree::Root, u64)>,
        )> {
            self.get_commitment_tree_roots(id).await
        }

        async fn get_mempool_txids(
            &self,
        ) -> BlockchainSourceResult<Option<Vec<zebra_chain::transaction::Hash>>> {
            self.get_mempool_txids().await
        }

        async fn get_transaction(
            &self,
            txid: Hash,
        ) -> BlockchainSourceResult<Option<Arc<zebra_chain::transaction::Transaction>>> {
            self.get_transaction(txid).await
        }

        async fn get_best_block_hash(
            &self,
        ) -> BlockchainSourceResult<Option<zebra_chain::block::Hash>> {
            self.get_best_block_hash().await
        }
    }
}
