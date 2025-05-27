//! Holds the Finalised portion of the chain index on disk.

use crate::{config::BlockCacheConfig, error::FinalisedStateError, AtomicStatus, StatusType};

use zebra_chain::parameters::NetworkKind;

use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use dcbor::{CBORCase, CBORTagged, CBORTaggedDecodable, CBORTaggedEncodable, Tag, CBOR};
use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags};
use std::{fs, sync::Arc};
use tracing::info;

/// Zaino’s Finalised state.
/// Implements a persistent LMDB-backed chain index for fast read access and verified data.
pub struct ZainoDB {
    /// Shared LMDB environment.
    env: Arc<Environment>,

    /// Block headers: Hash -> StoredEntry<BlockHeaderData>
    headers_db: Database,
    /// Transactions: Hash -> StoredEntry<Vec<TxData>>
    ///
    /// Stored per-block, in order.
    transactions_db: Database,
    /// Spent outpoints: Hash -> StoredEntry<Vec<SpentOutpoint>>
    ///
    /// Stored per-block
    spent_db: Database,
    /// Best chain index: Height -> Hash
    ///
    /// Used for height based fetch of the best chain.
    best_chain_db: Database,
    /// Subtree roots: Index -> StoredEntry<ShardRoot>
    roots_db: Database,
    /// Metadata: singleton entry "metadata" -> StoredEntry<DbMetadata>
    metadata_db: Database,

    /// Database handler task handle.
    db_handler: Option<tokio::task::JoinHandle<()>>,
    /// ZainoDB status.
    status: AtomicStatus,
    /// BlockCache config data.
    config: BlockCacheConfig,
}

impl ZainoDB {
    /// Spawns a new [`ZainoDB`] and syncs the FinalisedState to the servers finalised state.
    ///
    /// Uses ReadStateService to fetch chain data if given else uses JsonRPC client.
    ///
    /// Inputs:
    /// - config: ChainIndexConfig.
    pub async fn spawn(config: &BlockCacheConfig) -> Result<Self, FinalisedStateError> {
        info!("Launching ZainoDB");
        let db_size = config.db_size.unwrap_or(128);
        let db_size_bytes = db_size * 1024 * 1024 * 1024;
        let db_path_dir = match config.network.kind() {
            NetworkKind::Mainnet => "mainnet",
            NetworkKind::Testnet => "testnet",
            NetworkKind::Regtest => "regtest",
        };
        let db_path = config.db_path.join(db_path_dir);
        if !db_path.exists() {
            fs::create_dir_all(&db_path)?;
        }

        let env = Environment::new()
            .set_max_dbs(6)
            .set_map_size(db_size_bytes)
            .set_flags(EnvironmentFlags::NO_TLS)
            .open(&db_path)?;

        let headers_db = Self::open_or_create_db(&env, "headers", DatabaseFlags::empty())?;
        let transactions_db =
            Self::open_or_create_db(&env, "transactions", DatabaseFlags::empty())?;
        let spent_db = Self::open_or_create_db(&env, "spent", DatabaseFlags::empty())?;
        let best_chain_db =
            Self::open_or_create_db(&env, "best_chain", DatabaseFlags::INTEGER_KEY)?;
        let roots_db = Self::open_or_create_db(&env, "roots", DatabaseFlags::empty())?;
        let metadata_db = Self::open_or_create_db(&env, "metadata", DatabaseFlags::empty())?;

        let mut zaino_db = Self {
            env: Arc::new(env),
            headers_db,
            transactions_db,
            spent_db,
            best_chain_db,
            roots_db,
            metadata_db,
            db_handler: None,
            status: AtomicStatus::new(StatusType::Spawning.into()),
            config: config.clone(),
        };

        // check schema / version, perform conversions

        zaino_db.spawn_handler().await?;

        Ok(zaino_db)
    }

    /// Spawns a task to handle background validation and cleanup.
    async fn spawn_handler(&mut self) -> Result<(), FinalisedStateError> {
        let mut zaino_db = Self {
            env: Arc::clone(&self.env),
            headers_db: self.headers_db,
            transactions_db: self.transactions_db,
            spent_db: self.spent_db,
            best_chain_db: self.best_chain_db,
            roots_db: self.roots_db,
            metadata_db: self.metadata_db,
            db_handler: None,
            status: self.status.clone(),
            config: self.config.clone(),
        };

        let db_handler = tokio::spawn(async move {
            // validate database, clean trailing transactions
        });

        self.db_handler = Some(db_handler);
        Ok(())
    }

    // clean_trailing()
    // cleans trailing transactions.

    // write_block(block)

    // delete_block(hash)

    // get_block_header_data(hash_or_height)

    // get_block_transactions(hash_or_height)

    // get_block_spends(hash_or_height)

    // get_roots(start_index, end_index)

    // get_chain_index(hash_or_height)

    // get_chain_block(hash_or_height)

    // get_compact_block(hash_or_height)

    /// Opens an lmdb database if present else creates a new one.
    fn open_or_create_db(
        env: &Environment,
        name: &str,
        flags: DatabaseFlags,
    ) -> Result<Database, FinalisedStateError> {
        match env.open_db(Some(name)) {
            Ok(db) => Ok(db),
            Err(lmdb::Error::NotFound) => env
                .create_db(Some(name), flags)
                .map_err(FinalisedStateError::LmdbError),
            Err(e) => Err(FinalisedStateError::LmdbError(e)),
        }
    }
}

/// DCBOR serialisation schema tags.
///
/// All structs in this module should be included in this enum.
///
/// Items should use the range 500 - 509 (510 - 599 are reserved for chain structs).
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbSchemaTag {
    StoredEntry = 500,
    DbMetadata = 501,
    DbVersion = 502,
}

impl DbSchemaTag {
    pub fn tag(self) -> dcbor::Tag {
        dcbor::Tag::with_value(self as u64)
    }
}

/// Wrapper around a CBOR-tagged item and its BLAKE2b checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    /// The inner tagged item being stored.
    pub item: T,
    /// The BLAKE2b-256 checksum of the canonical dCBOR encoding of `item`.
    pub checksum: [u8; 32],
}

impl<T> StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    /// Construct a new `StoredEntry` from an item, computing its checksum.
    pub fn new(item: T) -> Self {
        let encoded = item.tagged_cbor().to_cbor_data();
        let checksum = Self::compute_blake2b_256(&encoded);
        Self { item, checksum }
    }

    /// Verifies the checksum matches the current item's encoding.
    pub fn verify(&self) -> bool {
        let encoded = self.item.tagged_cbor().to_cbor_data();
        self.checksum == Self::compute_blake2b_256(&encoded)
    }

    /// Serializes the entry into canonical dCBOR.
    pub fn serialize(&self) -> Vec<u8> {
        self.tagged_cbor().to_cbor_data()
    }

    /// Deserializes a `StoredEntry<T>` from canonical dCBOR bytes.
    pub fn deserialize(bytes: &[u8]) -> dcbor::Result<Self> {
        let cbor = CBOR::try_from_data(bytes)?;
        Self::from_tagged_cbor(cbor)
    }

    /// Computes a BLAKE2b-256 checksum.
    fn compute_blake2b_256(data: &[u8]) -> [u8; 32] {
        let mut hasher = Blake2bVar::new(32).expect("Failed to create hasher");
        hasher.update(data);
        let mut output = [0u8; 32];
        hasher
            .finalize_variable(&mut output)
            .expect("Failed to finalize hash");
        output
    }
}

impl<T> CBORTagged for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    fn cbor_tags() -> Vec<Tag> {
        vec![DbSchemaTag::StoredEntry.tag()]
    }
}

impl<T: CBORTaggedEncodable> CBORTaggedEncodable for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    fn untagged_cbor(&self) -> CBOR {
        CBOR::from(vec![
            self.item.tagged_cbor(),
            CBOR::from(&self.checksum[..]),
        ])
    }
}

impl<T: CBORTaggedDecodable> CBORTaggedDecodable for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    fn from_untagged_cbor(cbor: CBOR) -> dcbor::Result<Self> {
        match cbor.into_case() {
            CBORCase::Array(arr) if arr.len() == 2 => {
                let item = T::from_tagged_cbor(arr[0].clone())?;
                let bytes: Vec<u8> = arr[1].clone().try_into()?;
                let checksum: [u8; 32] = bytes.try_into().map_err(|_| dcbor::Error::WrongType)?;
                Ok(Self { item, checksum })
            }
            _ => Err(dcbor::Error::WrongType),
        }
    }
}

impl<T: CBORTaggedDecodable> TryFrom<CBOR> for StoredEntry<T>
where
    T: CBORTaggedEncodable + CBORTaggedDecodable + CBORTagged,
{
    type Error = dcbor::Error;

    fn try_from(cbor: CBOR) -> Result<Self, Self::Error> {
        Self::from_tagged_cbor(cbor)
    }
}

/// Top-level database metadata entry, storing the current schema version.
///
/// Stored under the fixed key `"metadata"` in the LMDB metadata database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbMetadata {
    /// Encodes the version and schema hash.
    pub version: DbVersion,
}

impl CBORTagged for DbMetadata {
    fn cbor_tags() -> Vec<dcbor::Tag> {
        vec![DbSchemaTag::DbMetadata.tag()]
    }
}

impl CBORTaggedEncodable for DbMetadata {
    fn untagged_cbor(&self) -> CBOR {
        self.version.tagged_cbor()
    }
}

impl CBORTaggedDecodable for DbMetadata {
    fn from_untagged_cbor(cbor: CBOR) -> dcbor::Result<Self> {
        Ok(Self {
            version: DbVersion::from_tagged_cbor(cbor)?,
        })
    }
}

impl TryFrom<CBOR> for DbMetadata {
    type Error = dcbor::Error;
    fn try_from(cbor: CBOR) -> dcbor::Result<Self> {
        Self::from_tagged_cbor(cbor)
    }
}

/// Database schema version information.
///
/// This is used for schema migration safety and compatibility checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbVersion {
    /// Monotonically increasing version number (e.g., 1, 2, ...)
    pub version: u32,
    /// BLAKE2b-256 hash of the schema definition (includes struct layout, types, etc.)
    pub schema_hash: [u8; 32],
}

impl CBORTagged for DbVersion {
    fn cbor_tags() -> Vec<dcbor::Tag> {
        vec![DbSchemaTag::DbVersion.tag()]
    }
}

impl CBORTaggedEncodable for DbVersion {
    fn untagged_cbor(&self) -> CBOR {
        CBOR::from(vec![
            CBOR::from(self.version),
            CBOR::from(&self.schema_hash[..]),
        ])
    }
}

impl CBORTaggedDecodable for DbVersion {
    fn from_untagged_cbor(cbor: CBOR) -> dcbor::Result<Self> {
        match cbor.into_case() {
            CBORCase::Array(arr) if arr.len() == 2 => {
                let version = arr[0].clone().try_into()?;
                let hash_vec: Vec<u8> = arr[1].clone().try_into()?;
                let schema_hash: [u8; 32] =
                    hash_vec.try_into().map_err(|_| dcbor::Error::WrongType)?;
                Ok(Self {
                    version,
                    schema_hash,
                })
            }
            _ => Err(dcbor::Error::WrongType),
        }
    }
}

impl TryFrom<CBOR> for DbVersion {
    type Error = dcbor::Error;
    fn try_from(cbor: CBOR) -> dcbor::Result<Self> {
        Self::from_tagged_cbor(cbor)
    }
}
