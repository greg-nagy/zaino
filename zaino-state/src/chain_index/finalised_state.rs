//! Holds the Finalised portion of the chain index on disk.

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, AtomicStatus, BlockData, BlockIndex,
    ChainBlock, Hash, Height, Index, ShardRoot, SpentList, SpentOutpoint, StatusType, TxData,
    TxList,
};

use dashmap::DashSet;
use tokio::time::{interval, sleep};
use zebra_chain::parameters::NetworkKind;

use blake2::{
    digest::{Update, VariableOutput},
    Blake2bVar,
};
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
use tracing::{debug, info, warn};
use zebra_state::HashOrHeight;

use super::types::BlockHeaderData;

// ───────────────────────── Schema v1 constants ─────────────────────────

/// Full V1 schema text file.
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

/// Database V1 schema hash, used for version validation.
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
        let zaino_db = Arc::new(Self {
            env: Arc::clone(&self.env),
            headers_db: self.headers_db,
            transactions_db: self.transactions_db,
            spent_db: self.spent_db,
            best_chain_db: self.best_chain_db,
            roots_db: self.roots_db,
            metadata_db: self.metadata_db,
            validated_tip: Arc::clone(&self.validated_tip),
            validated_set: self.validated_set.clone(),
            db_handler: None, // not used inside the task
            status: self.status.clone(),
            config: self.config.clone(),
        });

        let handle = tokio::spawn({
            let zaino_db = Arc::clone(&zaino_db);
            async move {
                // ────────────────────────── initial validation ─────────────────────────
                if let Err(e) = zaino_db.initial_root_scan() {
                    warn!("initial root scan failed: {e}");
                    return;
                }
                if let Err(e) = zaino_db.initial_block_scan() {
                    warn!("initial block scan failed: {e}");
                    return;
                }
                debug!(
                    "initial validation complete – tip={}",
                    zaino_db.validated_tip.load(Ordering::Relaxed)
                );

                // ────────────────────────── steady-state loop ──────────────────────────
                let mut maintenance = interval(Duration::from_secs(60));

                loop {
                    // ---------- try to validate the next consecutive block -------------
                    let next_h = zaino_db.validated_tip.load(Ordering::Acquire) + 1;
                    let height = match Height::try_from(next_h) {
                        Ok(h) => h,
                        Err(_) => {
                            warn!("height overflow – validated_tip too large");
                            break;
                        }
                    };

                    // Fetch hash of `next_h` from best_chain (Option-returning helper).
                    let hash_opt = (|| -> Option<Hash> {
                        let ro = zaino_db.env.begin_ro_txn().ok()?;
                        let key = height.to_ne_bytes();
                        let val = ro.get(zaino_db.best_chain_db, &key).ok()?;
                        let arr: [u8; 32] = val.try_into().ok()?;
                        Some(Hash::from(arr))
                    })();

                    if let Some(hash) = hash_opt {
                        if let Err(e) = zaino_db.validate_block(height, hash) {
                            // Already includes “…failed validation” wording.
                            warn!("{e}");
                        }
                        // Immediately loop – maybe the chain has more blocks ready.
                        continue;
                    }

                    // ---------- nothing new yet → wait / maintenance -------------------
                    tokio::select! {
                        // short nap so we don’t spin-hot
                        _ = sleep(Duration::from_secs(5)) => {},

                        // fire every 60 s regardless of block arrivals
                        _ = maintenance.tick() => {
                            if let Err(e) = zaino_db.clean_trailing() {
                                warn!("clean_trailing failed: {e}");
                            }
                        }
                    }
                }
            }
        });

        self.db_handler = Some(handle);
        Ok(())
    }

    /// Validate every stored `ShardRoot` (cheap – single checksum each).
    fn initial_root_scan(&self) -> Result<(), FinalisedStateError> {
        let ro = self.env.begin_ro_txn()?;
        let mut cursor = ro.open_ro_cursor(self.roots_db)?;

        for (_key, val_bytes) in cursor.iter() {
            let entry = StoredEntry::<ShardRoot>::deserialize(val_bytes)
                .map_err(|e| FinalisedStateError::Custom(format!("corrupt root CBOR: {e}")))?;
            if !entry.verify() {
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
        let mut cursor = ro.open_ro_cursor(self.best_chain_db)?;

        for (h_bytes, hash_bytes) in cursor.iter() {
            let height_u32 = u32::from_ne_bytes(h_bytes.try_into().expect("height key is 4 bytes"));
            let height =
                Height::try_from(height_u32).expect("height in best_chain is always inside range");

            let hash_arr: [u8; 32] = hash_bytes.try_into().expect("hash value is 32 bytes");
            let hash = Hash::from(hash_arr);

            // Will short-circuit if already done
            if let Err(e) = self.validate_block(height, hash) {
                return Err(e);
            }
        }
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
        let hash_key: [u8; 32] = self
            .resolve_validated_hash_or_height(hash_or_height)?
            .into();

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
        let hash_key: [u8; 32] = self
            .resolve_validated_hash_or_height(hash_or_height)?
            .into();

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
        let hash_key: [u8; 32] = self
            .resolve_validated_hash_or_height(hash_or_height)?
            .into();

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

    /// Convenience getter so callers (RPC, tests, CLI) can inspect the
    /// on-disk schema version and hash.
    pub fn get_db_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        let ro = self.env.begin_ro_txn()?;
        let raw = ro.get(self.metadata_db, b"metadata")?;
        let stored: StoredEntry<DbMetadata> = StoredEntry::deserialize(raw)
            .map_err(|e| FinalisedStateError::Custom(format!("corrupt metadata CBOR: {e}")))?;
        if !stored.verify() {
            return Err(FinalisedStateError::Custom(
                "metadata checksum mismatch".into(),
            ));
        }
        Ok(stored.item)
    }

    /// Create a read-only facade backed by *this* live database.
    pub fn to_reader(&self) -> DbReader<'_> {
        DbReader { inner: self }
    }

    /// Return `true` if *height* is already known-good.
    ///
    /// O(1) look-ups: we check the tip first (fast) and only hit the DashSet
    /// when `h > tip`.
    fn is_validated(&self, h: u32) -> bool {
        let tip = self.validated_tip.load(Ordering::Relaxed);
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

        let key: [u8; 32] = hash.into();
        let ro = self.env.begin_ro_txn()?;

        // Helper to fabricate the error.
        let fail = |reason: &str| FinalisedStateError::InvalidBlock {
            height: height.into(),
            hash,
            reason: reason.to_owned(),
        };

        // -------- header -----------------------------------------------------
        {
            let raw = ro.get(self.headers_db, &key)?;
            let entry = StoredEntry::<BlockHeaderData>::deserialize(raw)
                .map_err(|e| fail(&format!("corrupt header CBOR: {e}")))?;
            if !entry.verify() {
                return Err(fail("header checksum mismatch"));
            }
        }

        // -------- transactions ----------------------------------------------
        {
            let raw = ro.get(self.transactions_db, &key)?;
            let entry = StoredEntry::<TxList>::deserialize(raw)
                .map_err(|e| fail(&format!("corrupt tx CBOR: {e}")))?;
            if !entry.verify() {
                return Err(fail("tx checksum mismatch"));
            }
        }

        // -------- spent list -------------------------------------------------
        {
            let raw = ro.get(self.spent_db, &key)?;
            let entry = StoredEntry::<SpentList>::deserialize(raw)
                .map_err(|e| fail(&format!("corrupt spends CBOR: {e}")))?;
            if !entry.verify() {
                return Err(fail("spent checksum mismatch"));
            }
        }

        self.mark_validated(height.into());
        Ok(())
    }

    /// Same as `resolve_hash_or_height`, **but guarantees the block is validated**.
    ///
    /// * If the block hasn’t been validated yet we do it on-demand (cheap: one
    ///   read-only LMDB txn and three checksum calculations).
    /// * On success the block hash is returned; on any failure you get a
    ///   `FinalisedStateError`.
    fn resolve_validated_hash_or_height(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<Hash, FinalisedStateError> {
        // ---------- resolve to (height, hash) ------------------------------
        let (height, hash) = match hash_or_height {
            // ---- caller gave us a hash ------------------------------------
            HashOrHeight::Hash(hash) => {
                // Convert into our local `Hash` new-type.
                let hash: Hash = hash.into();
                let hash_bytes: [u8; 32] = hash.into();

                // We still need the *height* (to update water-mark).
                let ro = self.env.begin_ro_txn()?;
                let raw_bytes = ro.get(self.headers_db, &hash_bytes)?;
                let header_entry =
                    StoredEntry::<BlockHeaderData>::deserialize(raw_bytes).map_err(|e| {
                        FinalisedStateError::Custom(format!("corrupt header CBOR: {e}"))
                    })?;

                let h = header_entry.item.index.height().ok_or_else(|| {
                    FinalisedStateError::Custom("header without height (shouldn't happen)".into())
                })?;

                (h, hash)
            }

            // ---- caller gave us a height ----------------------------------
            HashOrHeight::Height(z_height) => {
                let my_hash = self.resolve_hash_or_height(hash_or_height)?;
                (
                    Height::try_from(z_height.0).expect("already checked range"),
                    my_hash,
                )
            }
        };

        // ---------- ensure the block is validated --------------------------
        self.validate_block(height, hash)?;
        Ok(hash)
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

    /// Ensure the `metadata` table contains **exactly** our `DB_SCHEMA_V1`.
    ///
    /// * Brand-new DB → insert the entry.
    /// * Existing DB  → verify checksum, version, and schema hash.
    fn check_schema_version(&self) -> Result<(), FinalisedStateError> {
        // We only need a mutable LMDB txn; `self` itself isn’t mutated.
        let mut txn = self.env.begin_rw_txn()?;

        match txn.get(self.metadata_db, b"metadata") {
            // ── Existing DB ────────────────────────────────────────────────
            Ok(raw_bytes) => {
                let stored: StoredEntry<DbMetadata> =
                    StoredEntry::deserialize(raw_bytes).map_err(|e| {
                        FinalisedStateError::Custom(format!("corrupt metadata CBOR: {e}"))
                    })?;
                if !stored.verify() {
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

            // ── Fresh DB (key not found) ──────────────────────────────────
            Err(lmdb::Error::NotFound) => {
                let entry = StoredEntry::new(DbMetadata {
                    version: DB_SCHEMA_V1,
                });
                txn.put(
                    self.metadata_db,
                    b"metadata",
                    &entry.serialize(),
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

impl<'a> DbReader<'a> {
    /// Returns block header and chain indexing data for the block.
    pub fn get_block_header_data(
        &self,
        id: HashOrHeight,
    ) -> Result<BlockHeaderData, FinalisedStateError> {
        self.inner.get_block_header_data(id)
    }

    /// Returns transaction data for the block.
    pub fn get_block_transactions(&self, id: HashOrHeight) -> Result<TxList, FinalisedStateError> {
        self.inner.get_block_transactions(id)
    }

    // Returns a single [`TxData`] from the block, identified by its
    /// zero-based position within the block’s compact-transaction list.
    pub fn get_transaction(
        &self,
        id: HashOrHeight,
        idx: u32,
    ) -> Result<TxData, FinalisedStateError> {
        self.inner.get_transaction(id, idx)
    }

    /// Returns spend data for the block.
    pub fn get_block_spends(&self, id: HashOrHeight) -> Result<SpentList, FinalisedStateError> {
        self.inner.get_block_spends(id)
    }

    pub fn get_spend(
        &self,
        id: HashOrHeight,
        idx: u32,
    ) -> Result<SpentOutpoint, FinalisedStateError> {
        self.inner.get_spend(id, idx)
    }

    /// Returns a single `SpentOutpoint` from the block, identified by its
    /// zero-based position inside the stored spends list.
    pub fn get_shard_roots(
        &self,
        start: u32,
        end: u32,
    ) -> Result<Vec<ShardRoot>, FinalisedStateError> {
        self.inner.get_shard_roots(start, end)
    }

    /// Returns chain indexing data for the block.
    pub fn get_chain_index(&self, id: HashOrHeight) -> Result<BlockIndex, FinalisedStateError> {
        self.inner.get_chain_index(id)
    }

    /// Returns header data for the block.
    pub fn get_block_header(&self, id: HashOrHeight) -> Result<BlockData, FinalisedStateError> {
        self.inner.get_block_header(id)
    }

    /// Returns the **entire** [`ChainBlock`] (header/index + compact txs +
    /// spent outpoints) identified by `hash_or_height`.
    pub fn get_chain_block(&self, id: HashOrHeight) -> Result<ChainBlock, FinalisedStateError> {
        self.inner.get_chain_block(id)
    }

    /// Returns a CompactBlock identified by `hash_or_height`.
    pub fn get_compact_block(
        &self,
        id: HashOrHeight,
    ) -> Result<zaino_proto::proto::compact_formats::CompactBlock, FinalisedStateError> {
        self.inner.get_compact_block(id)
    }

    /// Convenience getter so callers (RPC, tests, CLI) can inspect the
    /// on-disk schema version and hash.
    pub fn get_db_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        self.inner.get_db_metadata()
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

/// Top-level database metadata entry, storing the current schema version.
///
/// Stored under the fixed key `"metadata"` in the LMDB metadata database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbMetadata {
    /// Encodes the version and schema hash.
    pub version: DbVersion,
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
