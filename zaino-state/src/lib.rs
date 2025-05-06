//! Zaino's core mempool and chain-fetching Library.
//!
//! Built to use a configurable backend:
//! - FetchService
//!    - Built using the Zcash Json RPC Services for backwards compatibility with Zcashd and other JsonRPC based validators.
//! - StateService
//!    - Built using Zebra's ReadStateService for efficient chain access.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

// Zaino's Indexer library frontend.
pub(crate) mod indexer;

pub use indexer::{
    IndexerService, IndexerSubscriber, LightWalletIndexer, LightWalletService, ZcashIndexer,
    ZcashService,
};

pub(crate) mod backends;

pub use backends::{
    fetch::{FetchService, FetchServiceSubscriber},
    state::{StateService, StateServiceSubscriber},
};

// TODO: Rework local_cache -> ChainIndex
pub(crate) mod local_cache;

#[cfg(feature = "bench")]
pub use local_cache::*;

pub(crate) mod config;

pub use config::{BackendConfig, BackendType, FetchServiceConfig, StateServiceConfig};

#[cfg(feature = "bench")]
pub use config::BlockCacheConfig;

pub(crate) mod error;

pub use error::{FetchServiceError, StateServiceError};

pub(crate) mod status;

pub use status::{AtomicStatus, StatusType};

pub(crate) mod stream;

pub use stream::{
    AddressStream, CompactBlockStream, CompactTransactionStream, RawTransactionStream,
    SubtreeRootReplyStream, UtxoReplyStream,
};

pub(crate) mod broadcast;

pub(crate) mod utils;
