//! Holds the Finalised portion of the chain index on disk.

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, AtomicStatus, ChainBlock,
    CommitmentTreeData, Hash, Height, Index, OrchardTxList, SaplingTxList, ShardRoot, StatusType,
    TransparentTxList, TxidList,
};

use dashmap::DashSet;
use tokio::time::interval;
use zebra_chain::parameters::NetworkKind;

use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
use core2::io::{self, Read, Write};
use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction as _, WriteFlags,
};
use std::{
    fs,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::{error, info, warn};
use zebra_state::HashOrHeight;

use super::{
    encoding::{
        read_fixed_le, read_u32_le, version, write_fixed_le, write_u32_le, CompactSize,
        FixedEncodedLen, ZainoVersionedSerialise,
    },
    types::BlockHeaderData,
};

// ───────────────────────── Schema v1 constants ─────────────────────────

/// Full V1 schema text file. WARNING: THIS IS WRONG!
// 1. Bring the *exact* ASCII description of the on-disk layout into the binary
//    at compile-time.  The path is relative to this source file.
pub const DB_SCHEMA_V1_TEXT: &str = include_str!("db_schema_v1.txt");

/*
2. Compute the checksum once, outside the code:

       $ cd zaino-state/src/chain_index
       $ b2sum -l 256 db_schema_v1.txt
       bf9ac729a4b8a41d63698547e64072742a6967518cceaa59c5bc827ce146fe93  db_schema_v1.txt

   Optional helper if you don’t have `b2sum`:

       $ python - <<'PY'
       > import hashlib, pathlib, binascii
       > data = pathlib.Path("db_schema_v1.txt").read_bytes()
       > print(hashlib.blake2b(data, digest_size=32).hexdigest())
       > PY

3. Turn those 64 hex digits into a Rust `[u8; 32]` literal:

       echo bf9ac729a4b8a41d63698547e64072742a6967518cceaa59c5bc827ce146fe93 \
       | sed 's/../0x&, /g' | fold -s -w48

*/

/// Database V1 schema hash, used for version validation. WARNING: THIS IS WRONG!
pub const DB_SCHEMA_V1_HASH: [u8; 32] = [
    0xbf, 0x9a, 0xc7, 0x29, 0xa4, 0xb8, 0xa4, 0x1d, 0x63, 0x69, 0x85, 0x47, 0xe6, 0x40, 0x72, 0x74,
    0x2a, 0x69, 0x67, 0x51, 0x8c, 0xce, 0xaa, 0x59, 0xc5, 0xbc, 0x82, 0x7c, 0xe1, 0x46, 0xfe, 0x93,
];

/// Database V1 vesrion data.
pub const DB_SCHEMA_V1: DbVersion = DbVersion {
    version: 1,
    schema_hash: DB_SCHEMA_V1_HASH,
};

/// Zaino’s Finalised state.
/// Implements a persistent LMDB-backed chain index for fast read access and verified data.
pub struct ZainoDB {
    /// Shared LMDB environment.
    env: Arc<Environment>,

    /// Block headers: Height -> StoredEntry<BlockHeaderData>
    ///
    /// Stored per-block, in order.
    headers: Database,
    /// Txids: Height -> StoredEntry<TxidList>
    ///
    /// Stored per-block, in order.
    txids: Database,
    /// Transparent: Height -> StoredEntry<Vec<TransparentTxList>>
    ///
    /// Stored per-block, in order.
    transparent: Database,
    /// Sapling: Height -> StoredEntry<Vec<TxData>>
    ///
    /// Stored per-block, in order.
    sapling: Database,
    /// Orchard: Height -> StoredEntry<Vec<TxData>>
    ///
    /// Stored per-block, in order.
    orchard: Database,
    /// Block commitment tree data: Height -> StoredEntry<Vec<CommitmentTreeData>>
    ///
    /// Stored per-block, in order.
    commitment_tree_data: Database,
    /// Heights: Hash -> Height
    ///
    /// Used for hash based fetch of the best chain (and random access).
    heights: Database,
    /// Spent outpoints: Outpoint -> StoredEntry<Vec<TxIndex>>
    ///
    /// TODO: Add doc!
    spent: Database,
    /// Transparent address history: AddrScript -> StoredEntry<AddrEventBytes>
    ///
    /// TODO: add doc!
    addrhist: Database,
    /// Subtree roots: Index -> StoredEntry<ShardRoot>
    shard_roots: Database,
    /// Metadata: singleton entry "metadata" -> StoredEntry<DbMetadata>
    metadata: Database,

    /// Contiguous **water-mark**: every height ≤ `validated_tip` is known-good.
    ///
    /// Wrapped in an `Arc` so the background validator and any foreground tasks
    /// all see (and update) the **same** atomic.
    validated_tip: Arc<AtomicU32>,
    /// Heights **above** the tip that have also been validated.
    ///
    /// Whenever the next consecutive height is inserted we pop it
    /// out of this set and bump `validated_tip`, so the map never
    /// grows beyond the number of “holes” in the sequence.
    validated_set: DashSet<u32>,

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

        // Prepare database details and path.
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

        // Check system rescources to set max db reeaders, clamped between 256 and 1024.
        let cpu_cnt = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let max_readers = u32::try_from((cpu_cnt * 4).clamp(256, 1024))
            .expect("max_readers was clamped to fit in u32");

        // Open LMDB environment and set environmental details.
        let env = Environment::new()
            .set_max_dbs(12)
            .set_map_size(db_size_bytes)
            .set_max_readers(max_readers)
            .set_flags(EnvironmentFlags::NO_TLS | EnvironmentFlags::NO_READAHEAD)
            .open(&db_path)?;

        // Open individual LMDB DBs.
        let headers = Self::open_or_create_db(&env, "headers", DatabaseFlags::INTEGER_KEY)?;
        let txids = Self::open_or_create_db(&env, "txids", DatabaseFlags::INTEGER_KEY)?;
        let transparent = Self::open_or_create_db(&env, "transparent", DatabaseFlags::INTEGER_KEY)?;
        let sapling = Self::open_or_create_db(&env, "sapling", DatabaseFlags::INTEGER_KEY)?;
        let orchard = Self::open_or_create_db(&env, "orchard", DatabaseFlags::INTEGER_KEY)?;
        let commitment_tree_data =
            Self::open_or_create_db(&env, "commitment_tree_data", DatabaseFlags::INTEGER_KEY)?;
        let hashes = Self::open_or_create_db(&env, "hashes", DatabaseFlags::empty())?;
        let spent = Self::open_or_create_db(&env, "spent", DatabaseFlags::empty())?;
        let addrhist = Self::open_or_create_db(
            &env,
            "addrhist",
            DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED,
        )?;
        let shard_roots = Self::open_or_create_db(&env, "shard_roots", DatabaseFlags::INTEGER_KEY)?;
        let metadata = Self::open_or_create_db(&env, "metadata", DatabaseFlags::empty())?;

        // Create ZainoDB
        let mut zaino_db = Self {
            env: Arc::new(env),
            headers,
            txids,
            transparent,
            sapling,
            orchard,
            commitment_tree_data,
            heights: hashes,
            spent,
            addrhist,
            shard_roots,
            metadata,
            validated_tip: Arc::new(AtomicU32::new(0)),
            validated_set: DashSet::new(),
            db_handler: None,
            status: AtomicStatus::new(StatusType::Spawning.into()),
            config: config.clone(),
        };

        // Validate (or initialise) the metadata entry before we touch any tables.
        zaino_db.check_schema_version()?;

        // Spawn handler task to perform background validation and trailing tx cleanup.
        zaino_db.spawn_handler().await?;

        Ok(zaino_db)
    }

    /// Spawns the background validator / maintenance task.
    ///
    /// *   **Startup** – runs a full‐DB validation pass (`initial_root_scan` →
    ///     `initial_block_scan`).
    /// *   **Steady-state** – every 5 s tries to validate the next block that
    ///     appeared after the current `validated_tip`.  
    ///     Every 60 s it also calls `clean_trailing()` to purge stale reader slots.
    async fn spawn_handler(&mut self) -> Result<(), FinalisedStateError> {
        // Clone everything the task needs so we can move it into the async block.
        let mut zaino_db = Self {
            env: Arc::clone(&self.env),
            headers: self.headers,
            txids: self.txids,
            transparent: self.transparent,
            sapling: self.sapling,
            orchard: self.orchard,
            commitment_tree_data: self.commitment_tree_data,
            heights: self.heights,
            spent: self.spent,
            addrhist: self.addrhist,
            shard_roots: self.shard_roots,
            metadata: self.metadata,
            validated_tip: Arc::clone(&self.validated_tip),
            validated_set: self.validated_set.clone(),
            db_handler: None, // not used inside the task
            status: self.status.clone(),
            config: self.config.clone(),
        };

        let handle = tokio::spawn({
            let zaino_db = zaino_db;
            async move {
                // ────────────────────────── initial validation ─────────────────────────
                // TODO: Run this in background!
                if let Err(e) = zaino_db.initial_root_scan() {
                    error!("initial root scan failed: {e}");
                    zaino_db.status.store(StatusType::CriticalError.into());
                    // TODO: Handle corrupt db better!
                    return;
                }
                if let Err(e) = zaino_db.initial_block_scan() {
                    error!("initial block scan failed: {e}");
                    zaino_db.status.store(StatusType::CriticalError.into());
                    // TODO: Handle corrupt db better!
                    return;
                }
                info!(
                    "initial validation complete – tip={}",
                    zaino_db.validated_tip.load(Ordering::Relaxed)
                );

                // ────────────────────────── steady-state loop ──────────────────────────
                let mut maintenance = interval(Duration::from_secs(60));

                loop {
                    // ---------- try to validate the next consecutive block -------------
                    let next_h = zaino_db.validated_tip.load(Ordering::Acquire) + 1;
                    let next_height = match Height::try_from(next_h) {
                        Ok(h) => h,
                        Err(_) => {
                            warn!("height overflow – validated_tip too large");
                            zaino_db.zaino_db_handler_sleep(&mut maintenance).await;
                            continue;
                        }
                    };

                    // Fetch hash of `next_h` from Heights.
                    let hkey = match next_height.to_bytes() {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            warn!("Failed to serialize height {}: {}", next_height, e);
                            zaino_db.zaino_db_handler_sleep(&mut maintenance).await;
                            continue;
                        }
                    };

                    let hash_opt = (|| -> Option<Hash> {
                        let ro = zaino_db.env.begin_ro_txn().ok()?;
                        let bytes = ro.get(zaino_db.headers, &hkey).ok()?;
                        let entry = StoredEntryVar::<BlockHeaderData>::deserialize(bytes).ok()?;
                        Some(*entry.inner().index().hash())
                    })();

                    if let Some(hash) = hash_opt {
                        if let Err(e) = zaino_db.validate_block(next_height, hash) {
                            warn!("{e}");
                        }
                        // Immediately loop – maybe the chain has more blocks ready.
                        continue;
                    }

                    zaino_db.zaino_db_handler_sleep(&mut maintenance).await;
                }
            }
        });

        self.db_handler = Some(handle);
        Ok(())
    }

    /// Helper method to wait for the next loop iteration or perform maintenance.
    async fn zaino_db_handler_sleep(&self, maintenance: &mut tokio::time::Interval) {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            _ = maintenance.tick() => {
                if let Err(e) = self.clean_trailing() {
                    warn!("clean_trailing failed: {}", e);
                }
            }
        }
    }

    /// Validate every stored `ShardRoot` (cheap – single checksum each).
    fn initial_root_scan(&self) -> Result<(), FinalisedStateError> {
        let ro_txn = self.env.begin_ro_txn()?;
        let mut cursor = ro_txn.open_ro_cursor(self.shard_roots)?;

        for (key_bytes, val_bytes) in cursor.iter() {
            // 1) Deserialize the StoredEntryFixed<ShardRoot> from the raw bytes
            let entry = StoredEntryFixed::<ShardRoot>::from_bytes(val_bytes).map_err(|e| {
                FinalisedStateError::Custom(format!("corrupt shard-root entry: {e}"))
            })?;

            // 2) Verify the checksum against the *same* key bytes
            if !entry.verify(key_bytes) {
                return Err(FinalisedStateError::Custom(
                    "shard-root checksum mismatch".into(),
                ));
            }
        }

        Ok(())
    }

    /// Scan the whole chain once at start-up and validate every block.
    fn initial_block_scan(&self) -> Result<(), FinalisedStateError> {
        let ro = self.env.begin_ro_txn()?;
        let mut cursor = ro.open_ro_cursor(self.heights)?;

        for (hash_bytes, height_entry_bytes) in cursor.iter() {
            let hash = Hash::from_bytes(hash_bytes)?;
            let height = *StoredEntryFixed::<Height>::from_bytes(height_entry_bytes)
                .map_err(|e| FinalisedStateError::Custom(format!("corrupt height entry: {e}")))?
                .inner();

            if let Err(e) = self.validate_block(height, hash) {
                return Err(e);
            }
        }
        Ok(())
    }

    // TODO: Add transaction index scan!

    /// Clears stale reader slots by opening and closing a read transaction.
    pub fn clean_trailing(&self) -> Result<(), FinalisedStateError> {
        let txn = self.env.begin_ro_txn()?;
        drop(txn);
        Ok(())
    }

    /// Writes a given (finalised) [`ChainBlock`] to ZainoDB.
    pub fn write_block(&self, block: ChainBlock) -> Result<(), FinalisedStateError> {
        let block_hash = block.index().hash();
        let block_hash_bytes = block_hash.to_bytes()?;
        let block_height = block.index().height().ok_or(FinalisedStateError::Custom(
            "finalised state received non finalised block".to_string(),
        ))?;
        let block_height_bytes = block_height.to_bytes()?;

        // Build DBHeight
        let height_entry = StoredEntryFixed::new(
            &block_hash_bytes,
            block.index().height().ok_or(FinalisedStateError::Custom(
                "finalised state received non finalised block".to_string(),
            ))?,
        );

        // Build header
        let header_entry = StoredEntryVar::new(
            &block_height_bytes,
            BlockHeaderData::new(*block.index(), *block.data()),
        );

        // Build commitment tree data
        let commitment_tree_entry =
            StoredEntryFixed::new(&block_height_bytes, *block.commitment_tree_data());

        // Build transaction indexes
        let tx_len = block.transactions().len();
        let mut txids = Vec::with_capacity(tx_len);
        let mut transparent = Vec::with_capacity(tx_len);
        let mut sapling = Vec::with_capacity(tx_len);
        let mut orchard = Vec::with_capacity(tx_len);

        for tx in block.transactions() {
            txids.push(Hash::from(*tx.txid()));

            let t = if tx.transparent().inputs().is_empty() && tx.transparent().outputs().is_empty()
            {
                None
            } else {
                Some(tx.transparent().clone())
            };
            transparent.push(t);

            let s = if tx.sapling().value().is_none() {
                None
            } else {
                Some(tx.sapling().clone())
            };
            sapling.push(s);

            let o = if tx.orchard().value().is_none() {
                None
            } else {
                Some(tx.orchard().clone())
            };
            orchard.push(o);

            // TODO: Build transaction indexes!
        }

        let txid_entry = StoredEntryVar::new(&block_height_bytes, TxidList::new(txids));
        let transparent_entry =
            StoredEntryVar::new(&block_height_bytes, TransparentTxList::new(transparent));
        let sapling_entry = StoredEntryVar::new(&block_height_bytes, SaplingTxList::new(sapling));
        let orchard_entry = StoredEntryVar::new(&block_height_bytes, OrchardTxList::new(orchard));

        let mut txn = self.env.begin_rw_txn()?;

        // Write to ZainoDB
        txn.put(
            self.headers,
            &block_height_bytes,
            &header_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.txids,
            &block_height_bytes,
            &txid_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.transparent,
            &block_height_bytes,
            &transparent_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.sapling,
            &block_height_bytes,
            &sapling_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.orchard,
            &block_height_bytes,
            &orchard_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.commitment_tree_data,
            &block_height_bytes,
            &commitment_tree_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        txn.put(
            self.heights,
            &block_hash_bytes,
            &height_entry.to_bytes()?,
            WriteFlags::NO_OVERWRITE,
        )?;

        // TODO: write transaction indexes!

        txn.commit()?;

        self.validate_block(block_height, *block_hash)?;

        Ok(())
    }

    /// Inserts a `ShardRoot` at its numeric `Index`.
    ///
    /// * The key is the 4-byte native-endian encoding of `index`, proceeded by a 1 byte version tag.
    /// * The value is a `StoredEntryFixed<ShardRoot>`.
    ///
    /// Returns an error if an entry already exists for that index.
    pub fn write_root(&self, index: Index, root: ShardRoot) -> Result<(), FinalisedStateError> {
        // 1) Build key (tag + BE index)
        let key = index
            .to_bytes()
            .map_err(|e| FinalisedStateError::Custom(format!("index key serialize: {e}")))?;

        // 2) Wrap in StoredEntryFixed (versioned body + checksum), then to_bytes()
        let entry = StoredEntryFixed::new(&key, root);
        let val = entry
            .to_bytes()
            .map_err(|e| FinalisedStateError::Custom(format!("shard-root serialize: {e}")))?;

        // 3) Insert under NO_OVERWRITE
        let mut txn = self.env.begin_rw_txn()?;
        match txn.put(self.shard_roots, &key, &val, WriteFlags::NO_OVERWRITE) {
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
        // Resolve the HashOrHeight and find corresponding database hash and height key.
        let (hash_bytes, height_bytes) = match id {
            HashOrHeight::Height(height) => {
                let height_bytes = Height::try_from(height.0)
                    .expect("zebra height always fits")
                    .to_bytes()?;

                // Look up hash in hashes db.
                let hash = {
                    let ro = self.env.begin_ro_txn()?;
                    let hash_bytes = ro.get(self.heights, &height_bytes).map_err(|e| {
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
                let hash_bytes = hash.to_bytes()?;
                (hash_bytes, height_bytes)
            }
            HashOrHeight::Hash(z_hash) => {
                let hash_bytes = Hash::from(z_hash).to_bytes()?;

                // Clone header bytes out of the RO txn and fetch height.
                let header_vec: Vec<u8> = {
                    let ro = self.env.begin_ro_txn()?;
                    let header_bytes = ro.get(self.headers, &hash_bytes).map_err(|e| {
                        if e == lmdb::Error::NotFound {
                            FinalisedStateError::Custom("block not found".into())
                        } else {
                            FinalisedStateError::LmdbError(e)
                        }
                    })?;
                    header_bytes.to_vec()
                };

                let stored: StoredEntryVar<BlockHeaderData> =
                    StoredEntryVar::from_bytes(&header_vec).map_err(|e| {
                        FinalisedStateError::Custom(format!("corrupt header CBOR: {e}"))
                    })?;
                let height_bytes = Height::try_from(
                    stored
                        .item
                        .index()
                        .height()
                        .expect("db always stores a height"),
                )
                .expect("zebra height always fits")
                .to_bytes()?;

                (hash_bytes, height_bytes)
            }
        };

        // Delete block in single transaction.
        let mut txn = self.env.begin_rw_txn()?;

        for &db in &[
            self.headers,
            self.txids,
            self.transparent,
            self.sapling,
            self.orchard,
            self.commitment_tree_data,
        ] {
            match txn.del(db, &height_bytes, None) {
                Ok(()) | Err(lmdb::Error::NotFound) => {}
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            }
        }

        match txn.del(self.heights, &hash_bytes, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => {}
            Err(e) => return Err(FinalisedStateError::LmdbError(e)),
        }

        // TODO: Delete transactions indexes!

        txn.commit()?;
        Ok(())
    }

    /// Removes the `ShardRoot` stored at the specified `Index`.
    ///
    /// Returns an error if no entry exists at that index.
    pub fn delete_root(&self, index: Index) -> Result<(), FinalisedStateError> {
        // Reconstruct exactly the same key
        let key = index
            .to_bytes()
            .map_err(|e| FinalisedStateError::Custom(format!("index key serialize: {e}")))?;

        let mut txn = self.env.begin_rw_txn()?;
        match txn.del(self.shard_roots, &key, None) {
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

    // /// Returns block header and chain indexing data for the block.
    // pub fn get_block_header_data(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<BlockHeaderData, FinalisedStateError> {
    //     let hash_key: [u8; 32] = self
    //         .resolve_validated_hash_or_height(hash_or_height)?
    //         .into();

    //     let hdr_vec: Vec<u8> = {
    //         let ro = self.env.begin_ro_txn()?;
    //         ro.get(self.headers_db, &hash_key)?.to_vec()
    //     };
    //     let stored: StoredEntry<BlockHeaderData> = StoredEntry::deserialize(&hdr_vec)
    //         .map_err(|e| FinalisedStateError::Custom(format!("corrupt header CBOR: {e}")))?;
    //     Ok(stored.item)
    // }

    // /// Returns transaction data for the block.
    // pub fn get_block_transactions(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<TxList, FinalisedStateError> {
    //     let hash_key: [u8; 32] = self
    //         .resolve_validated_hash_or_height(hash_or_height)?
    //         .into();

    //     let tx_vec: Vec<u8> = {
    //         let ro = self.env.begin_ro_txn()?;
    //         ro.get(self.transactions_db, &hash_key)?.to_vec()
    //     };
    //     let stored: StoredEntry<TxList> = StoredEntry::deserialize(&tx_vec)
    //         .map_err(|e| FinalisedStateError::Custom(format!("corrupt tx CBOR: {e}")))?;
    //     Ok(stored.item)
    // }

    // // Returns a single [`TxData`] from the block, identified by its
    // /// zero-based position within the block’s compact-transaction list.
    // pub fn get_transaction(
    //     &self,
    //     hash_or_height: HashOrHeight,
    //     tx_index: u32,
    // ) -> Result<TxData, FinalisedStateError> {
    //     let list = self.get_block_transactions(hash_or_height)?;
    //     let idx = tx_index as usize;

    //     list.0.get(idx).cloned().ok_or_else(|| {
    //         FinalisedStateError::Custom(format!("transaction index {tx_index} out of range"))
    //     })
    // }

    // /// Returns spend data for the block.
    // pub fn get_block_spends(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<SpentList, FinalisedStateError> {
    //     let hash_key: [u8; 32] = self
    //         .resolve_validated_hash_or_height(hash_or_height)?
    //         .into();

    //     let sp_vec: Vec<u8> = {
    //         let ro = self.env.begin_ro_txn()?;
    //         ro.get(self.spent_db, &hash_key)?.to_vec()
    //     };
    //     let stored: StoredEntry<SpentList> = StoredEntry::deserialize(&sp_vec)
    //         .map_err(|e| FinalisedStateError::Custom(format!("corrupt spends CBOR: {e}")))?;
    //     Ok(stored.item)
    // }

    // /// Returns a single `SpentOutpoint` from the block, identified by its
    // /// zero-based position inside the stored spends list.
    // pub fn get_spend(
    //     &self,
    //     hash_or_height: HashOrHeight,
    //     spend_index: u32,
    // ) -> Result<SpentOutpoint, FinalisedStateError> {
    //     let list = self.get_block_spends(hash_or_height)?;
    //     let idx = spend_index as usize;

    //     list.0.get(idx).cloned().ok_or_else(|| {
    //         FinalisedStateError::Custom(format!("spend index {spend_index} out of range"))
    //     })
    // }

    // /// Returns every `ShardRoot` whose index satisfies `start_index ≤ idx < end_index`.
    // /// If `start_index >= end_index` an empty vector is returned.
    // pub fn get_shard_roots(
    //     &self,
    //     start_index: u32,
    //     end_index: u32,
    // ) -> Result<Vec<ShardRoot>, FinalisedStateError> {
    //     if start_index > end_index {
    //         return Ok(Vec::new());
    //     }

    //     // first key in native-endian order
    //     let first_key = Index(start_index).to_ne_bytes();

    //     let ro_txn = self.env.begin_ro_txn()?;
    //     let mut cursor = ro_txn.open_ro_cursor(self.roots_db)?;

    //     let mut roots = Vec::new();

    //     // `iter_from` → Iterator<Item = (&[u8], &[u8])>
    //     for (key_bytes, val_bytes) in cursor.iter_from::<&[u8]>(&first_key) {
    //         let idx =
    //             u32::from_ne_bytes(key_bytes.try_into().expect("INTEGER_KEY keys are 4 bytes"));
    //         if idx > end_index {
    //             break;
    //         }

    //         let stored: StoredEntry<ShardRoot> = StoredEntry::deserialize(val_bytes)
    //             .map_err(|e| FinalisedStateError::Custom(format!("corrupt root CBOR: {e}")))?;
    //         roots.push(stored.item);
    //     }

    //     Ok(roots)
    // }

    // /// Returns chain indexing data for the block.
    // pub fn get_chain_index(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<BlockIndex, FinalisedStateError> {
    //     Ok(self.get_block_header_data(hash_or_height)?.index)
    // }

    // /// Returns header data for the block.
    // pub fn get_block_header(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<BlockData, FinalisedStateError> {
    //     Ok(self.get_block_header_data(hash_or_height)?.data)
    // }

    // /// Returns the **entire** [`ChainBlock`] (header/index + compact txs +
    // /// spent outpoints) identified by `hash_or_height`.
    // pub fn get_chain_block(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<ChainBlock, FinalisedStateError> {
    //     let header_data = self.get_block_header_data(hash_or_height)?;

    //     let tx_list = self.get_block_transactions(hash_or_height)?;

    //     let spent_list = self.get_block_spends(hash_or_height)?;

    //     Ok(ChainBlock::new(
    //         header_data.index,
    //         header_data.data,
    //         tx_list.0,
    //         spent_list.0,
    //     ))
    // }

    // /// Returns a CompactBlock identified by `hash_or_height`.
    // pub fn get_compact_block(
    //     &self,
    //     hash_or_height: HashOrHeight,
    // ) -> Result<zaino_proto::proto::compact_formats::CompactBlock, FinalisedStateError> {
    //     Ok(self.get_chain_block(hash_or_height)?.to_compact_block())
    // }

    // /// Convenience getter so callers (RPC, tests, CLI) can inspect the
    // /// on-disk schema version and hash.
    // pub fn get_db_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
    //     let ro = self.env.begin_ro_txn()?;
    //     let raw = ro.get(self.metadata_db, b"metadata")?;
    //     let stored: StoredEntry<DbMetadata> = StoredEntry::deserialize(raw)
    //         .map_err(|e| FinalisedStateError::Custom(format!("corrupt metadata CBOR: {e}")))?;
    //     if !stored.verify() {
    //         return Err(FinalisedStateError::Custom(
    //             "metadata checksum mismatch".into(),
    //         ));
    //     }
    //     Ok(stored.item)
    // }

    /// Create a read-only facade backed by *this* live database.
    pub fn to_reader(&self) -> DbReader<'_> {
        DbReader { inner: self }
    }

    /// Return `true` if *height* is already known-good.
    ///
    /// O(1) look-ups: we check the tip first (fast) and only hit the DashSet
    /// when `h > tip`.
    fn is_validated(&self, h: u32) -> bool {
        let tip = self.validated_tip.load(Ordering::Acquire);
        h <= tip || self.validated_set.contains(&h)
    }

    /// Mark *height* as validated and coalesce contiguous ranges.
    ///
    /// 1. Insert it into the DashSet (if it was a “hole”).
    /// 2. While `validated_tip + 1` is now present, pop it and advance the tip.
    fn mark_validated(&self, h: u32) {
        let mut next = h;
        loop {
            let tip = self.validated_tip.load(Ordering::Acquire);

            // Fast-path: extend the tip directly?
            if next == tip + 1 {
                // Try to claim the new tip.
                if self
                    .validated_tip
                    .compare_exchange(tip, next, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    // Successfully advanced; now look for further consecutive heights
                    // already in the DashSet.
                    next += 1;
                    while self.validated_set.remove(&next).is_some() {
                        self.validated_tip.store(next, Ordering::Release);
                        next += 1;
                    }
                    break;
                }
                // CAS failed: someone else updated the tip – retry loop.
            } else if next > tip {
                // Out-of-order hole: just remember it and exit.
                self.validated_set.insert(next);
                break;
            } else {
                // Already below tip – nothing to do.
                break;
            }
        }
    }

    /// Lightweight per-block validation.
    ///
    /// *Confirms the checksum* in each of the three per-block tables.  
    /// TODO: Add Merkle / ZIP-243 checks.
    fn validate_block(&self, height: Height, hash: Hash) -> Result<(), FinalisedStateError> {
        if self.is_validated(height.into()) {
            return Ok(());
        }

        let height_key = height
            .to_bytes()
            .map_err(|e| FinalisedStateError::Custom(format!("height serialize: {e}")))?;
        let hash_key = hash
            .to_bytes()
            .map_err(|e| FinalisedStateError::Custom(format!("hash serialize: {e}")))?;

        let ro = self.env.begin_ro_txn()?;

        // Helper to fabricate the error.
        let fail = |reason: &str| FinalisedStateError::InvalidBlock {
            height: height.into(),
            hash,
            reason: reason.to_owned(),
        };

        // *** header ***
        {
            let raw = ro
                .get(self.headers, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<BlockHeaderData>::from_bytes(raw)
                .map_err(|e| fail(&format!("header corrupt CBOR: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("header checksum mismatch"));
            }
        }

        // *** txids ***
        {
            let raw = ro
                .get(self.txids, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<TxidList>::from_bytes(raw)
                .map_err(|e| fail(&format!("txids corrupt CBOR: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("txids checksum mismatch"));
            }
        }

        // *** transparent ***
        {
            let raw = ro
                .get(self.transparent, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<TransparentTxList>::from_bytes(raw)
                .map_err(|e| fail(&format!("transparent corrupt CBOR: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("transparent checksum mismatch"));
            }
        }

        // *** sapling ***
        {
            let raw = ro
                .get(self.sapling, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<SaplingTxList>::from_bytes(raw)
                .map_err(|e| fail(&format!("sapling corrupt CBOR: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("sapling checksum mismatch"));
            }
        }

        // *** orchard ***
        {
            let raw = ro
                .get(self.orchard, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<OrchardTxList>::from_bytes(raw)
                .map_err(|e| fail(&format!("orchard corrupt CBOR: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("orchard checksum mismatch"));
            }
        }

        // *** commitment_tree_data (fixed) ***
        {
            let raw = ro
                .get(self.commitment_tree_data, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryFixed::<CommitmentTreeData>::from_bytes(raw)
                .map_err(|e| fail(&format!("commitment_tree corrupt bytes: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("commitment_tree checksum mismatch"));
            }
        }

        // *** hash→height mapping ***
        {
            let raw = ro
                .get(self.heights, &hash_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryFixed::<Height>::from_bytes(raw)
                .map_err(|e| fail(&format!("hash -> height corrupt bytes: {e}")))?;
            if !entry.verify(&hash_key) {
                return Err(fail("hash -> height checksum mismatch"));
            }
            if entry.item != height {
                return Err(fail("hash -> height mapping mismatch"));
            }
        }

        // TODO: Add transaction index validation!

        self.mark_validated(height.into());
        Ok(())
    }

    /// Same as `resolve_hash_or_height`, **but guarantees the block is validated**.
    ///
    /// * If the block hasn’t been validated yet we do it on-demand
    /// * On success the block hright is returned; on any failure you get a
    ///   `FinalisedStateError`.
    fn resolve_validated_hash_or_height(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<Height, FinalisedStateError> {
        let (height, hash) = match hash_or_height {
            // Height lookup path.
            HashOrHeight::Height(z_height) => {
                let height = Height::try_from(z_height.0)
                    .map_err(|_| FinalisedStateError::Custom("height out of range".into()))?;

                // Check if height is below validated tip,
                // this avoids hash lookups for height based fetch under the valdated tip.
                if height.0 <= self.validated_tip.load(Ordering::Acquire) {
                    return Ok(height);
                }

                let hkey = height.to_bytes()?;

                let hash = {
                    let ro = self.env.begin_ro_txn()?;
                    let bytes = ro.get(self.headers, &hkey).map_err(|e| {
                        if e == lmdb::Error::NotFound {
                            FinalisedStateError::Custom("height not found in best chain".into())
                        } else {
                            FinalisedStateError::LmdbError(e)
                        }
                    })?;

                    *StoredEntryVar::<BlockHeaderData>::deserialize(bytes)?
                        .inner()
                        .index()
                        .hash()
                };
                (height, hash)
            }

            // Hash lookup path.
            HashOrHeight::Hash(z_hash) => {
                let height = self.resolve_hash_or_height(hash_or_height)?;
                (height, Hash::from(z_hash))
            }
        };

        self.validate_block(height, hash)?;
        Ok(height)
    }

    /// Resolve a `HashOrHeight` to the block hash stored on disk.
    ///
    /// * Height  ->  returned unchanged (zero cost).  
    /// * Hash ->  lookup in `hashes` db.
    fn resolve_hash_or_height(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<Height, FinalisedStateError> {
        match hash_or_height {
            // Fast path: we already have the hash.
            HashOrHeight::Height(z_height) => Ok(Height::try_from(z_height.0)
                .map_err(|_| FinalisedStateError::Custom("height out of range".into()))?),

            // Height lookup path.
            HashOrHeight::Hash(z_hash) => {
                let hash = Hash::try_from(z_hash.0)
                    .map_err(|_| FinalisedStateError::Custom("incorrect hash".into()))?;
                let hkey = hash.to_bytes()?;

                let height: Height = {
                    let ro = self.env.begin_ro_txn()?;
                    let bytes = ro.get(self.heights, &hkey).map_err(|e| {
                        if e == lmdb::Error::NotFound {
                            FinalisedStateError::Custom("height not found in best chain".into())
                        } else {
                            FinalisedStateError::LmdbError(e)
                        }
                    })?;

                    *StoredEntryFixed::<Height>::deserialize(bytes)?.inner()
                };

                Ok(height)
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

    /// Ensure the `metadata` table contains **exactly** our `DB_SCHEMA_V1`.
    ///
    /// * Brand-new DB → insert the entry.
    /// * Existing DB  → verify checksum, version, and schema hash.
    fn check_schema_version(&self) -> Result<(), FinalisedStateError> {
        // We only need a mutable LMDB txn; `self` itself isn’t mutated.
        let mut txn = self.env.begin_rw_txn()?;

        match txn.get(self.metadata, b"metadata") {
            // *** Existing DB ***
            Ok(raw_bytes) => {
                let stored: StoredEntryFixed<DbMetadata> = StoredEntryFixed::from_bytes(raw_bytes)
                    .map_err(|e| {
                        FinalisedStateError::Custom(format!("corrupt metadata CBOR: {e}"))
                    })?;
                if !stored.verify(b"metadata") {
                    return Err(FinalisedStateError::Custom(
                        "metadata checksum mismatch – DB corruption suspected".into(),
                    ));
                }

                let meta = stored.item;

                if meta.version.version != DB_SCHEMA_V1.version {
                    return Err(FinalisedStateError::Custom(format!(
                        "unsupported schema version {} (expected v{})",
                        meta.version.version, DB_SCHEMA_V1.version
                    )));
                }
                if meta.version.schema_hash != DB_SCHEMA_V1.schema_hash {
                    return Err(FinalisedStateError::Custom(
                        "schema hash mismatch – db_schema_v1.txt edited without bumping version"
                            .into(),
                    ));
                }
            }

            // *** Fresh DB (key not found) ***
            Err(lmdb::Error::NotFound) => {
                let entry = StoredEntryFixed::new(
                    b"metadata",
                    DbMetadata {
                        version: DB_SCHEMA_V1,
                    },
                );
                txn.put(
                    self.metadata,
                    b"metadata",
                    &entry.to_bytes()?,
                    WriteFlags::NO_OVERWRITE,
                )?;
            }

            // ── Any other LMDB error ──────────────────────────────────────
            Err(e) => return Err(FinalisedStateError::LmdbError(e)),
        }

        txn.commit()?;
        Ok(())
    }
}

/// Immutable view onto an already-running [`ZainoDB`].
///
/// * Carries a plain reference with the same lifetime as the parent DB,
///   therefore:
///   * absolutely no cloning / ARC-ing of LMDB handles,
///   * compile-time guarantee that the reader cannot mutate state.
pub struct DbReader<'a> {
    inner: &'a ZainoDB,
}

// impl<'a> DbReader<'a> {
//     /// Returns block header and chain indexing data for the block.
//     pub fn get_block_header_data(
//         &self,
//         id: HashOrHeight,
//     ) -> Result<BlockHeaderData, FinalisedStateError> {
//         self.inner.get_block_header_data(id)
//     }

//     /// Returns transaction data for the block.
//     pub fn get_block_transactions(&self, id: HashOrHeight) -> Result<TxList, FinalisedStateError> {
//         self.inner.get_block_transactions(id)
//     }

//     // Returns a single [`TxData`] from the block, identified by its
//     /// zero-based position within the block’s compact-transaction list.
//     pub fn get_transaction(
//         &self,
//         id: HashOrHeight,
//         idx: u32,
//     ) -> Result<TxData, FinalisedStateError> {
//         self.inner.get_transaction(id, idx)
//     }

//     /// Returns spend data for the block.
//     pub fn get_block_spends(&self, id: HashOrHeight) -> Result<SpentList, FinalisedStateError> {
//         self.inner.get_block_spends(id)
//     }

//     pub fn get_spend(
//         &self,
//         id: HashOrHeight,
//         idx: u32,
//     ) -> Result<SpentOutpoint, FinalisedStateError> {
//         self.inner.get_spend(id, idx)
//     }

//     /// Returns a single `SpentOutpoint` from the block, identified by its
//     /// zero-based position inside the stored spends list.
//     pub fn get_shard_roots(
//         &self,
//         start: u32,
//         end: u32,
//     ) -> Result<Vec<ShardRoot>, FinalisedStateError> {
//         self.inner.get_shard_roots(start, end)
//     }

//     /// Returns chain indexing data for the block.
//     pub fn get_chain_index(&self, id: HashOrHeight) -> Result<BlockIndex, FinalisedStateError> {
//         self.inner.get_chain_index(id)
//     }

//     /// Returns header data for the block.
//     pub fn get_block_header(&self, id: HashOrHeight) -> Result<BlockData, FinalisedStateError> {
//         self.inner.get_block_header(id)
//     }

//     /// Returns the **entire** [`ChainBlock`] (header/index + compact txs +
//     /// spent outpoints) identified by `hash_or_height`.
//     pub fn get_chain_block(&self, id: HashOrHeight) -> Result<ChainBlock, FinalisedStateError> {
//         self.inner.get_chain_block(id)
//     }

//     /// Returns a CompactBlock identified by `hash_or_height`.
//     pub fn get_compact_block(
//         &self,
//         id: HashOrHeight,
//     ) -> Result<zaino_proto::proto::compact_formats::CompactBlock, FinalisedStateError> {
//         self.inner.get_compact_block(id)
//     }

//     /// Convenience getter so callers (RPC, tests, CLI) can inspect the
//     /// on-disk schema version and hash.
//     pub fn get_db_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
//         self.inner.get_db_metadata()
//     }
// }

// *** DB validation and varification ***

/// A fixed length database entry.
/// This is an important distinction for correct usage of DUP_SORT and DUP_FIXED
/// LMDB database flags.
///
/// Encoded Format:
///
/// ┌─────── byte 0 ───────┬───── byte 1 ─────┬───── T::raw_len() bytes ──────┬─── 32 bytes ────┐
/// │ StoredEntry version  │  Record version  │             Body              │ B2B256 hash     │
/// └──────────────────────┴──────────────────┴───────────────────────────────┴─────────────────┘
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntryFixed<T: ZainoVersionedSerialise + FixedEncodedLen> {
    pub item: T,
    pub checksum: [u8; 32],
}

impl<T: ZainoVersionedSerialise + FixedEncodedLen> StoredEntryFixed<T> {
    /// Create a new entry, hashing `key || encoded_item`.
    pub fn new<K: AsRef<[u8]>>(key: K, item: T) -> Self {
        let body = {
            let mut v = Vec::with_capacity(T::VERSIONED_LEN);
            item.serialize(&mut v).unwrap();
            v
        };
        let checksum = Self::blake2b256(&[key.as_ref(), &body].concat());
        Self { item, checksum }
    }

    /// Verify checksum given the DB key.
    /// Returns `true` if `self.checksum == blake2b256(key || item.serialize())`.
    pub fn verify<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let body = {
            let mut v = Vec::with_capacity(T::VERSIONED_LEN);
            self.item.serialize(&mut v).unwrap();
            v
        };
        let candidate = Self::blake2b256(&[key.as_ref(), &body].concat());
        candidate == self.checksum
    }

    /// Returns a reference to the inner item.
    pub fn inner(&self) -> &T {
        &self.item
    }

    /// Computes a BLAKE2b-256 checksum.
    fn blake2b256(data: &[u8]) -> [u8; 32] {
        let mut hasher = Blake2bVar::new(32).expect("Failed to create hasher");
        hasher.update(data);
        let mut output = [0u8; 32];
        hasher
            .finalize_variable(&mut output)
            .expect("Failed to finalize hash");
        output
    }
}

impl<T: ZainoVersionedSerialise + FixedEncodedLen> ZainoVersionedSerialise for StoredEntryFixed<T> {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.item.serialize(&mut *w)?;
        write_fixed_le::<32, _>(&mut *w, &self.checksum)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut body = vec![0u8; T::VERSIONED_LEN];
        r.read_exact(&mut body)?;
        let item = T::deserialize(&body[..])?;

        let checksum = read_fixed_le::<32, _>(r)?;
        Ok(Self { item, checksum })
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_latest(r)
    }
}

/// Variable-length database value.
/// Layout (little-endian unless noted):
///
/// ┌────── byte 0 ───────┬─────── CompactSize(len) ─────┬──── 1 byte ────┬── len - 1 bytes ───┬─ 32 bytes ─┐
/// │ StoredEntry version │ (length of item.serialize()) │ Record version │        Body        │    Hash    │
/// └─────────────────────┴──────────────────────────────┴────────────────┴────────────────────┴────────────┘
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntryVar<T: ZainoVersionedSerialise> {
    pub item: T,
    pub checksum: [u8; 32],
}

impl<T: ZainoVersionedSerialise> StoredEntryVar<T> {
    /// Create a new entry, hashing `encoded_key || encoded_item`.
    pub fn new<K: AsRef<[u8]>>(key: K, item: T) -> Self {
        let body = {
            let mut v = Vec::new();
            item.serialize(&mut v).unwrap();
            v
        };
        let checksum = Self::blake2b256(&[key.as_ref(), &body].concat());
        Self { item, checksum }
    }

    /// Verify checksum given the DB key.
    /// Returns `true` if `self.checksum == blake2b256(key || item.serialize())`.
    pub fn verify<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let mut body = Vec::new();
        self.item.serialize(&mut body).unwrap();
        let candidate = Self::blake2b256(&[key.as_ref(), &body].concat());
        candidate == self.checksum
    }

    /// Returns a reference to the inner item.
    pub fn inner(&self) -> &T {
        &self.item
    }

    /// Computes a BLAKE2b-256 checksum.
    fn blake2b256(data: &[u8]) -> [u8; 32] {
        let mut hasher = Blake2bVar::new(32).expect("Failed to create hasher");
        hasher.update(data);
        let mut output = [0u8; 32];
        hasher
            .finalize_variable(&mut output)
            .expect("Failed to finalize hash");
        output
    }
}

impl<T: ZainoVersionedSerialise> ZainoVersionedSerialise for StoredEntryVar<T> {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut body = Vec::new();
        self.item.serialize(&mut body)?;

        CompactSize::write(&mut *w, body.len())?;
        w.write_all(&body)?;
        write_fixed_le::<32, _>(&mut *w, &self.checksum)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        let len = CompactSize::read(&mut *r)? as usize;

        let mut body = vec![0u8; len];
        r.read_exact(&mut body)?;
        let item = T::deserialize(&body[..])?;

        let checksum = read_fixed_le::<32, _>(r)?;
        Ok(Self { item, checksum })
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_latest(r)
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

impl ZainoVersionedSerialise for DbMetadata {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.version.serialize(&mut *w)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        let version = DbVersion::deserialize(&mut *r)?;
        Ok(DbMetadata { version })
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_latest(r)
    }
}

/* DbMetadata: its body is one *versioned* DbVersion (36 + 1 tag) = 37 */
impl FixedEncodedLen for DbMetadata {
    const ENCODED_LEN: usize = DbVersion::VERSIONED_LEN;
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

impl ZainoVersionedSerialise for DbVersion {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        write_u32_le(&mut *w, self.version)?;
        write_fixed_le::<32, _>(&mut *w, &self.schema_hash)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        let version = read_u32_le(&mut *r)?;
        let schema_hash = read_fixed_le::<32, _>(&mut *r)?;
        Ok(DbVersion {
            version,
            schema_hash,
        })
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_latest(r)
    }
}

/* DbVersion: body = 4-byte u32 + 32-byte hash - 36 bytes */
impl FixedEncodedLen for DbVersion {
    const ENCODED_LEN: usize = 4 + 32;
}
