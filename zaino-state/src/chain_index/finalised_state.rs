//! Holds the Finalised portion of the chain index on disk.

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, AtomicStatus, BlockData, BlockIndex,
    ChainBlock, Hash, Height, Index, ShardRoot, SpentList, SpentOutpoint, StatusType, TxData,
    TxList,
};

use zebra_chain::parameters::NetworkKind;

use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use dcbor::{CBORCase, CBORTagged, CBORTaggedDecodable, CBORTaggedEncodable, Tag, CBOR};
use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction as _, WriteFlags,
};
use std::{fs, sync::Arc};
use tracing::info;
use zebra_state::HashOrHeight;

use super::types::BlockHeaderData;

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
        let roots_db = Self::open_or_create_db(&env, "roots", DatabaseFlags::INTEGER_KEY)?;
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

        // TODO: check schema / version, perform conversions (metadata db)

        zaino_db.spawn_handler().await?;

        Ok(zaino_db)
    }

    /// Spawns a task to handle background validation and cleanup.
    async fn spawn_handler(&mut self) -> Result<(), FinalisedStateError> {
        let _zaino_db = Self {
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
            // TODO: validate database, clean trailing transactions
        });

        self.db_handler = Some(db_handler);
        Ok(())
    }

    /// Clears stale reader slots by opening and closing a read transaction.
    pub fn clean_trailing(&self) -> Result<(), FinalisedStateError> {
        let txn = self.env.begin_ro_txn()?;
        drop(txn);
        Ok(())
    }

    /// Writes a given [`ChainBlock`] to ZainoDB.
    pub fn write_block(&self, block: ChainBlock) -> Result<(), FinalisedStateError> {
        let block_hash_bytes: [u8; 32] = (*block.index().hash()).into();
        let block_height_bytes = block
            .index()
            .height()
            .ok_or(FinalisedStateError::Custom(
                "finalised state received non finalised block".to_string(),
            ))?
            .to_ne_bytes();

        let header_entry = StoredEntry::new(BlockHeaderData {
            index: *block.index(),
            data: *block.data(),
        });
        let tx_entry = StoredEntry::new(block.tx_list());
        let spent_entry = StoredEntry::new(block.spent_list());

        let mut txn = self.env.begin_rw_txn()?;

        txn.put(
            self.headers_db,
            &block_hash_bytes,
            &header_entry.serialize(),
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.transactions_db,
            &block_hash_bytes,
            &tx_entry.serialize(),
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.spent_db,
            &block_hash_bytes,
            &spent_entry.serialize(),
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.best_chain_db,
            &block_height_bytes,
            &block_hash_bytes,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.commit()?;
        Ok(())
    }

    /// Inserts a `ShardRoot` at its numeric `Index`.
    ///
    /// * The key is the 4-byte native-endian encoding of `index`.
    /// * The value is a `StoredEntry<ShardRoot>` (tagged-CBOR + checksum).
    ///
    /// Returns an error if an entry already exists for that index.
    pub fn write_root(&self, index: Index, root: ShardRoot) -> Result<(), FinalisedStateError> {
        let key = index.to_ne_bytes();
        let entry = StoredEntry::new(root);
        let mut txn = self.env.begin_rw_txn()?;

        match txn.put(
            self.roots_db,
            &key,
            &entry.serialize(),
            WriteFlags::NO_OVERWRITE,
        ) {
            Ok(()) => {
                txn.commit()?;
                Ok(())
            }
            Err(lmdb::Error::KeyExist) => Err(FinalisedStateError::Custom(format!(
                "shard-root for index {} already present",
                index.0
            ))),
            Err(e) => Err(FinalisedStateError::LmdbError(e)),
        }
    }

    /// Deletes a block identified by hash *or* height from every finalised table.  
    pub fn delete_block(&self, id: HashOrHeight) -> Result<(), FinalisedStateError> {
        // Resolve the HashOrHeight and find corresponding value from DB.
        let (hash, height_opt) = match id {
            HashOrHeight::Height(height) => {
                let height_bytes = Height::try_from(height.0)
                    .expect("zebra height always fits")
                    .to_ne_bytes();

                // Look up hash in best_chain_db
                let hash = {
                    let ro = self.env.begin_ro_txn()?;
                    let hash_bytes = ro.get(self.best_chain_db, &height_bytes).map_err(|e| {
                        if e == lmdb::Error::NotFound {
                            FinalisedStateError::Custom("height not found in best chain".into())
                        } else {
                            FinalisedStateError::LmdbError(e)
                        }
                    })?;
                    let check_hash_bytes: [u8; 32] = hash_bytes
                        .try_into()
                        .expect("LMDB returned correct length value");
                    Hash::from(check_hash_bytes)
                };
                (
                    hash,
                    Some(
                        Height::try_from(height.0)
                            .expect("blocks in the finalised state always have a height"),
                    ),
                )
            }
            HashOrHeight::Hash(z_hash) => {
                let hash: Hash = z_hash.into();
                let hash_key: [u8; 32] = hash.into();

                // Clone header bytes out of the RO txn and fetch height.
                let header_vec: Vec<u8> = {
                    let ro = self.env.begin_ro_txn()?;
                    let bytes = ro.get(self.headers_db, &hash_key).map_err(|e| {
                        if e == lmdb::Error::NotFound {
                            FinalisedStateError::Custom("block not found".into())
                        } else {
                            FinalisedStateError::LmdbError(e)
                        }
                    })?;
                    bytes.to_vec()
                };

                let stored: StoredEntry<BlockHeaderData> = StoredEntry::deserialize(&header_vec)
                    .map_err(|e| {
                        FinalisedStateError::Custom(format!("corrupt header CBOR: {e}"))
                    })?;
                (hash, stored.item.index.height())
            }
        };

        let hash_key: [u8; 32] = hash.into();
        let height_key_opt = height_opt.map(|h| h.to_ne_bytes());

        // Delete block in single transaction.
        let mut txn = self.env.begin_rw_txn()?;

        for db in [self.headers_db, self.transactions_db, self.spent_db] {
            match txn.del(db, &hash_key, None) {
                Ok(()) | Err(lmdb::Error::NotFound) => {}
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            }
        }

        if let Some(hk) = height_key_opt {
            match txn.del(self.best_chain_db, &hk, None) {
                Ok(()) | Err(lmdb::Error::NotFound) => {}
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            }
        }

        txn.commit()?;
        Ok(())
    }

    /// Removes the `ShardRoot` stored at the specified `Index`.
    ///
    /// Returns an error if no entry exists at that index.
    pub fn delete_root(&self, index: Index) -> Result<(), FinalisedStateError> {
        let key = index.to_ne_bytes();
        let mut txn = self.env.begin_rw_txn()?;

        match txn.del(self.roots_db, &key, None) {
            Ok(()) => {
                txn.commit()?;
                Ok(())
            }
            Err(lmdb::Error::NotFound) => Err(FinalisedStateError::Custom(format!(
                "no shard-root at index {}",
                index.0
            ))),
            Err(e) => Err(FinalisedStateError::LmdbError(e)),
        }
    }

    /// Returns block header and chain indexing data for the block.
    pub fn get_block_header_data(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<BlockHeaderData, FinalisedStateError> {
        let hash_key: [u8; 32] = self.resolve_hash_or_height(hash_or_height)?.into();

        let hdr_vec: Vec<u8> = {
            let ro = self.env.begin_ro_txn()?;
            ro.get(self.headers_db, &hash_key)?.to_vec()
        };
        let stored: StoredEntry<BlockHeaderData> = StoredEntry::deserialize(&hdr_vec)
            .map_err(|e| FinalisedStateError::Custom(format!("corrupt header CBOR: {e}")))?;
        Ok(stored.item)
    }

    /// Returns transaction data for the block.
    pub fn get_block_transactions(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<TxList, FinalisedStateError> {
        let hash_key: [u8; 32] = self.resolve_hash_or_height(hash_or_height)?.into();

        let tx_vec: Vec<u8> = {
            let ro = self.env.begin_ro_txn()?;
            ro.get(self.transactions_db, &hash_key)?.to_vec()
        };
        let stored: StoredEntry<TxList> = StoredEntry::deserialize(&tx_vec)
            .map_err(|e| FinalisedStateError::Custom(format!("corrupt tx CBOR: {e}")))?;
        Ok(stored.item)
    }

    // Returns a single [`TxData`] from the block, identified by its
    /// zero-based position within the block’s compact-transaction list.
    pub fn get_transaction(
        &self,
        hash_or_height: HashOrHeight,
        tx_index: u32,
    ) -> Result<TxData, FinalisedStateError> {
        let list = self.get_block_transactions(hash_or_height)?;
        let idx = tx_index as usize;

        list.0.get(idx).cloned().ok_or_else(|| {
            FinalisedStateError::Custom(format!("transaction index {tx_index} out of range"))
        })
    }

    /// Returns spend data for the block.
    pub fn get_block_spends(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<SpentList, FinalisedStateError> {
        let hash_key: [u8; 32] = self.resolve_hash_or_height(hash_or_height)?.into();

        let sp_vec: Vec<u8> = {
            let ro = self.env.begin_ro_txn()?;
            ro.get(self.spent_db, &hash_key)?.to_vec()
        };
        let stored: StoredEntry<SpentList> = StoredEntry::deserialize(&sp_vec)
            .map_err(|e| FinalisedStateError::Custom(format!("corrupt spends CBOR: {e}")))?;
        Ok(stored.item)
    }

    /// Returns a single `SpentOutpoint` from the block, identified by its
    /// zero-based position inside the stored spends list.
    pub fn get_spend(
        &self,
        hash_or_height: HashOrHeight,
        spend_index: u32,
    ) -> Result<SpentOutpoint, FinalisedStateError> {
        let list = self.get_block_spends(hash_or_height)?;
        let idx = spend_index as usize;

        list.0.get(idx).cloned().ok_or_else(|| {
            FinalisedStateError::Custom(format!("spend index {spend_index} out of range"))
        })
    }

    /// Returns every `ShardRoot` whose index satisfies `start_index ≤ idx < end_index`.
    /// If `start_index >= end_index` an empty vector is returned.
    pub fn get_shard_roots(
        &self,
        start_index: u32,
        end_index: u32,
    ) -> Result<Vec<ShardRoot>, FinalisedStateError> {
        if start_index > end_index {
            return Ok(Vec::new());
        }

        // first key in native-endian order
        let first_key = Index(start_index).to_ne_bytes();

        let ro_txn = self.env.begin_ro_txn()?;
        let mut cursor = ro_txn.open_ro_cursor(self.roots_db)?;

        let mut roots = Vec::new();

        // `iter_from` → Iterator<Item = (&[u8], &[u8])>
        for (key_bytes, val_bytes) in cursor.iter_from::<&[u8]>(&first_key) {
            let idx =
                u32::from_ne_bytes(key_bytes.try_into().expect("INTEGER_KEY keys are 4 bytes"));
            if idx > end_index {
                break;
            }

            let stored: StoredEntry<ShardRoot> = StoredEntry::deserialize(val_bytes)
                .map_err(|e| FinalisedStateError::Custom(format!("corrupt root CBOR: {e}")))?;
            roots.push(stored.item);
        }

        Ok(roots)
    }

    /// Returns chain indexing data for the block.
    pub fn get_chain_index(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<BlockIndex, FinalisedStateError> {
        Ok(self.get_block_header_data(hash_or_height)?.index)
    }

    /// Returns header data for the block.
    pub fn get_block_header(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<BlockData, FinalisedStateError> {
        Ok(self.get_block_header_data(hash_or_height)?.data)
    }

    /// Returns the **entire** [`ChainBlock`] (header/index + compact txs +
    /// spent outpoints) identified by `hash_or_height`.
    pub fn get_chain_block(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<ChainBlock, FinalisedStateError> {
        let header_data = self.get_block_header_data(hash_or_height)?;

        let tx_list = self.get_block_transactions(hash_or_height)?;

        let spent_list = self.get_block_spends(hash_or_height)?;

        Ok(ChainBlock::new(
            header_data.index,
            header_data.data,
            tx_list.0,
            spent_list.0,
        ))
    }

    /// Returns a CompactBlock identified by `hash_or_height`.
    pub fn get_compact_block(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<zaino_proto::proto::compact_formats::CompactBlock, FinalisedStateError> {
        Ok(self.get_chain_block(hash_or_height)?.to_compact_block())
    }

    /// Resolve a `HashOrHeight` to the block hash stored on disk.
    ///
    /// * Hash  ➜  returned unchanged (zero cost).  
    /// * Height ➜  native-endian lookup in `best_chain_db`.
    fn resolve_hash_or_height(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<Hash, FinalisedStateError> {
        match hash_or_height {
            // Fast path: we already have the hash.
            HashOrHeight::Hash(z_hash) => Ok(z_hash.into()),

            // Height lookup path.
            HashOrHeight::Height(z_height) => {
                let height = Height::try_from(z_height.0)
                    .map_err(|_| FinalisedStateError::Custom("height out of range".into()))?;
                let hkey = height.to_ne_bytes();

                let hash_vec: Vec<u8> = {
                    let ro = self.env.begin_ro_txn()?;
                    ro.get(self.best_chain_db, &hkey)
                        .map_err(|e| {
                            if e == lmdb::Error::NotFound {
                                FinalisedStateError::Custom("height not found in best chain".into())
                            } else {
                                FinalisedStateError::LmdbError(e)
                            }
                        })?
                        .to_vec() // clone to break lifetime
                };
                let arr: [u8; 32] = hash_vec
                    .try_into()
                    .expect("best_chain value must be 32 bytes");
                Ok(Hash::from(arr))
            }
        }
    }

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

// TODO: Define v1 schema const
