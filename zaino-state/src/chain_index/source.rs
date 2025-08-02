//! BlockchainSource is the connection zaino holds the the serving validator / finaliser.

use std::sync::Arc;

use crate::chain_index::types::Hash;
use async_trait::async_trait;
use futures::future::join;
use tower::Service;
use zaino_fetch::jsonrpsee::{
    connector::{JsonRpSeeConnector, RpcRequestError},
    response::{GetBlockError, GetBlockResponse, GetTreestateResponse},
};
use zcash_primitives::merkle_tree::read_commitment_tree;
use zebra_chain::serialization::ZcashDeserialize;
use zebra_state::{HashOrHeight, ReadResponse, ReadStateService};

/// A trait for accessing blockchain data from different backends.
#[async_trait]
pub trait BlockchainSourceInterface: Clone + Send + Sync + 'static {
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
}

/// A connection to a validator.
#[derive(Clone)]
pub enum BlockchainSource {
    /// The connection is via direct read access to a zebrad's data file
    State(ReadStateService),
    /// We are connected to a zebrad, zcashd, or other zainod via JsonRpSee
    Fetch(JsonRpSeeConnector),
}

/// An error originating from a blockchain source.
#[derive(Debug, thiserror::Error)]
pub enum BlockchainSourceError {
    /// TODO: Add logic for handling recoverable errors if any are identified
    /// one candidate may be ephemerable network hiccoughs
    #[error("critical error in backing block source: {0}")]
    Unrecoverable(String),
}

/// Error type returned when invalid data is returned by the validator.
#[derive(thiserror::Error, Debug)]
#[error("data from validator invalid: {0}")]
pub struct InvalidData(String);

type BlockchainSourceResult<T> = Result<T, BlockchainSourceError>;

/// Methods that will dispatch to a ReadStateService or JsonRpSeeConnector
impl BlockchainSource {
    async fn get_block(
        &self,
        id: HashOrHeight,
    ) -> BlockchainSourceResult<Option<Arc<zebra_chain::block::Block>>> {
        match self {
            BlockchainSource::State(read_state_service) => match read_state_service
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
            BlockchainSource::Fetch(json_rp_see_connector) => {
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

    async fn get_commitment_tree_roots(
        &self,
        id: Hash,
    ) -> BlockchainSourceResult<(
        Option<(zebra_chain::sapling::tree::Root, u64)>,
        Option<(zebra_chain::orchard::tree::Root, u64)>,
    )> {
        match self {
            BlockchainSource::State(read_state_service) => {
                let (sapling_tree_response, orchard_tree_response) = join(
                    read_state_service
                        .clone()
                        .call(zebra_state::ReadRequest::SaplingTree(HashOrHeight::Hash(
                            id.into(),
                        ))),
                    read_state_service
                        .clone()
                        .call(zebra_state::ReadRequest::OrchardTree(HashOrHeight::Hash(
                            id.into(),
                        ))),
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
            BlockchainSource::Fetch(json_rp_see_connector) => {
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
}

#[async_trait]
impl BlockchainSourceInterface for BlockchainSource {
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
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use zebra_chain::{block::Block, orchard::tree as orchard, sapling::tree as sapling};
    use zebra_state::HashOrHeight;

    /// A test-only mock implementation of BlockchainReader using ordered lists by height.
    #[derive(Clone)]
    pub(crate) struct MockchainSource {
        blocks: Vec<Arc<Block>>,
        roots: Vec<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>,
        hashes: Vec<Hash>,
    }

    impl MockchainSource {
        /// Creates a new MockchainSource.
        /// All inputs must be the same length, and ordered by ascending height starting from `height_offset`.
        pub(crate) fn new(
            blocks: Vec<Arc<Block>>,
            roots: Vec<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>,
            hashes: Vec<Hash>,
        ) -> Self {
            assert!(
                blocks.len() == roots.len() && roots.len() == hashes.len(),
                "All input vectors must be the same length"
            );

            Self {
                blocks,
                roots,
                hashes,
            }
        }

        fn height_to_index(&self, height: u32) -> Result<usize, BlockchainSourceError> {
            if height == 0 {
                return Err(BlockchainSourceError::Unrecoverable(
                    "Block height must be >= 1".to_string(),
                ));
            }

            // Block height indexing starts at 1, vec indexing starts at 0..
            let index = (height - 1) as usize;

            if index >= self.blocks.len() {
                return Err(BlockchainSourceError::Unrecoverable(format!(
                    "Height {height} is out of range (max height = {})",
                    self.blocks.len()
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
    }

    #[async_trait]
    impl BlockchainSourceInterface for MockchainSource {
        async fn get_block(&self, id: HashOrHeight) -> BlockchainSourceResult<Option<Arc<Block>>> {
            match id {
                HashOrHeight::Height(h) => {
                    let i = self.height_to_index(h.0)?;
                    Ok(Some(Arc::clone(&self.blocks[i])))
                }
                HashOrHeight::Hash(hash) => {
                    let i = self.hash_to_index(&hash)?;
                    Ok(Some(Arc::clone(&self.blocks[i])))
                }
            }
        }

        async fn get_commitment_tree_roots(
            &self,
            id: Hash,
        ) -> BlockchainSourceResult<(Option<(sapling::Root, u64)>, Option<(orchard::Root, u64)>)>
        {
            let index = self.hashes.iter().position(|h| h == &id);
            Ok(index.map(|i| self.roots[i].clone()).unwrap_or((None, None)))
        }
    }
}
