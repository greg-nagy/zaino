//! Holds the Finalised portion of the chain index on disk.

use crate::{
    config::BlockCacheConfig, error::FinalisedStateError, AddrHistRecord, AddrScript, AtomicStatus,
    ChainBlock, CommitmentTreeData, Hash, Height, Index, OrchardTxList, Outpoint, SaplingTxList,
    ShardRoot, StatusType, TransparentTxList, TxInCompact, TxIndex, TxOutCompact, TxidList,
};

use dashmap::DashSet;
use sha2::{Digest, Sha256};
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
    collections::{hash_map::Entry, HashMap, HashSet},
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
    types::{AddrEventBytes, BlockHeaderData},
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
    /// Heights: Hash -> SotredEntry<Height>
    ///
    /// Used for hash based fetch of the best chain (and random access).
    heights: Database,
    /// Spent outpoints: Outpoint -> StoredEntry<Vec<TxIndex>>
    ///
    /// Used to check spent status of given outpoints, retuning spending tx.
    spent: Database,
    /// Transparent address history: AddrScript -> StoredEntry<AddrEventBytes>
    ///
    /// Used to search all transparent address indexes (txids, utxos, balances, deltas)
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

    // *** Internal Control Methods ***

    /// Spawns the background validator / maintenance task.
    ///
    /// *   **Startup** – runs a full‐DB validation pass (`initial_root_scan` →
    ///     `initial_block_scan`).
    /// *   **Steady-state** – every 5 s tries to validate the next block that
    ///     appeared after the current `validated_tip`.  
    ///     Every 60 s it also calls `clean_trailing()` to purge stale reader slots.
    async fn spawn_handler(&mut self) -> Result<(), FinalisedStateError> {
        // Clone everything the task needs so we can move it into the async block.
        let zaino_db = Self {
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
                let (r1, r2, r3, r4) = tokio::join!(
                    async { zaino_db.initial_root_scan() },
                    async { zaino_db.initial_spent_scan() },
                    async { zaino_db.initial_addrhist_scan() },
                    async { zaino_db.initial_block_scan() },
                );

                for (desc, result) in [
                    ("root scan", r1),
                    ("spent scan", r2),
                    ("addrhist scan", r3),
                    ("block scan", r4),
                ] {
                    if let Err(e) = result {
                        error!("initial {desc} failed: {e}");
                        zaino_db.status.store(StatusType::CriticalError.into());
                        // TODO: Handle error better?
                        return;
                    }
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

    /// Validate every stored `TxIndex` (cheap – single checksum each).
    pub fn initial_spent_scan(&self) -> Result<(), FinalisedStateError> {
        let ro = self.env.begin_ro_txn()?;
        let mut cursor = ro.open_ro_cursor(self.spent)?;

        for (key_bytes, val_bytes) in cursor.iter() {
            // 1) Deserialize the StoredEntryFixed<TxIndex> from the raw bytes
            let entry = StoredEntryFixed::<TxIndex>::from_bytes(val_bytes)
                .map_err(|e| FinalisedStateError::Custom(format!("corrupt spent entry: {e}")))?;

            // 2) Verify the checksum against the *same* key bytes
            if !entry.verify(key_bytes) {
                return Err(FinalisedStateError::Custom(
                    "spent record checksum mismatch".into(),
                ));
            }
        }

        Ok(())
    }

    /// Validate every stored `AddrEventBytes` (cheap – single checksum each).
    pub fn initial_addrhist_scan(&self) -> Result<(), FinalisedStateError> {
        let ro = self.env.begin_ro_txn()?;
        let mut cursor = ro.open_ro_cursor(self.addrhist)?;

        for (addr_bytes, record_bytes) in cursor.iter() {
            // 1) Deserialize the StoredEntryFixed<AddrEventBytes> from the raw bytes
            let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(record_bytes)
                .map_err(|e| FinalisedStateError::Custom(format!("corrupt addrhist entry: {e}")))?;

            // 2) Verify the checksum against the *same* key bytes
            if !entry.verify(addr_bytes) {
                return Err(FinalisedStateError::Custom(
                    "spent record checksum mismatch".into(),
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
            // 1) Deserialize the StoredEntryFixed<Height> from the raw bytes
            let height = *StoredEntryFixed::<Height>::from_bytes(height_entry_bytes)
                .map_err(|e| FinalisedStateError::Custom(format!("corrupt height entry: {e}")))?
                .inner();

            // 3) Validate block.
            if let Err(e) = self.validate_block(height, hash) {
                return Err(e);
            }
        }
        Ok(())
    }

    /// Clears stale reader slots by opening and closing a read transaction.
    fn clean_trailing(&self) -> Result<(), FinalisedStateError> {
        let txn = self.env.begin_ro_txn()?;
        drop(txn);
        Ok(())
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

    // *** DB write / delete methods ***
    // These should only ever be used in a single DB control task.

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

        let mut spent_map: HashMap<Outpoint, TxIndex> = HashMap::new();
        let mut addrhist_map: HashMap<AddrScript, Vec<AddrHistRecord>> = HashMap::new();

        for (tx_pos, tx) in block.transactions().iter().enumerate() {
            txids.push(Hash::from(*tx.txid()));

            // Transparent transactions
            let transparent_data =
                if tx.transparent().inputs().is_empty() && tx.transparent().outputs().is_empty() {
                    None
                } else {
                    Some(tx.transparent().clone())
                };
            transparent.push(transparent_data);

            // Sapling transactions
            let sapling_data = if tx.sapling().value().is_none() {
                None
            } else {
                Some(tx.sapling().clone())
            };
            sapling.push(sapling_data);

            // Orchard transactions
            let orchard_data = if tx.orchard().value().is_none() {
                None
            } else {
                Some(tx.orchard().clone())
            };
            orchard.push(orchard_data);

            // Transaction index
            let tx_index = TxIndex::new(block_height.into(), tx_pos as u16);

            // Transparent Inputs: Build Spent Outpoints Index and Address History
            for input in tx.transparent().inputs() {
                let prev_outpoint = Outpoint::new(*input.prevout_txid(), input.prevout_index());
                spent_map.insert(prev_outpoint, tx_index);

                if let Ok(prev_output) = self.get_previous_output(prev_outpoint) {
                    let addr_script = AddrScript::new(*prev_output.script_hash());
                    let hist_record = AddrHistRecord::new(
                        tx_index,
                        input.prevout_index() as u16,
                        prev_output.value(),
                        AddrHistRecord::FLAG_IS_INPUT,
                    );
                    match addrhist_map.entry(addr_script) {
                        Entry::Occupied(mut entry) => entry.get_mut().push(hist_record),
                        Entry::Vacant(entry) => {
                            entry.insert(vec![hist_record]);
                        }
                    }
                }
            }

            // Transparent Outputs: Build Address History
            for (output_index, output) in tx.transparent().outputs().iter().enumerate() {
                let addr_script = AddrScript::new(*output.script_hash());
                let hist_record = AddrHistRecord::new(
                    tx_index,
                    output_index as u16,
                    output.value(),
                    AddrHistRecord::FLAG_MINED,
                );
                match addrhist_map.entry(addr_script) {
                    Entry::Occupied(mut entry) => entry.get_mut().push(hist_record),
                    Entry::Vacant(entry) => {
                        entry.insert(vec![hist_record]);
                    }
                }
            }
        }

        let txid_entry = StoredEntryVar::new(&block_height_bytes, TxidList::new(txids));
        let transparent_entry =
            StoredEntryVar::new(&block_height_bytes, TransparentTxList::new(transparent));
        let sapling_entry = StoredEntryVar::new(&block_height_bytes, SaplingTxList::new(sapling));
        let orchard_entry = StoredEntryVar::new(&block_height_bytes, OrchardTxList::new(orchard));

        // Write to ZainoDB
        let mut txn = self.env.begin_rw_txn()?;

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

        for (outpoint, tx_index) in spent_map {
            let outpoint_bytes = &outpoint.to_bytes()?;
            let tx_index_entry_bytes =
                StoredEntryFixed::new(&outpoint_bytes, tx_index).to_bytes()?;
            txn.put(
                self.spent,
                &outpoint_bytes,
                &tx_index_entry_bytes,
                WriteFlags::NO_OVERWRITE,
            )?;
        }

        for (addr_script, records) in addrhist_map {
            let addr_bytes = &addr_script.to_bytes()?;
            for record in records {
                let packed_record = AddrEventBytes::from_record(&record).map_err(|e| {
                    FinalisedStateError::Custom(format!("AddrEventBytes pack error: {e:?}"))
                })?;
                let record_entry_bytes =
                    StoredEntryFixed::new(&addr_bytes, packed_record).to_bytes()?;
                txn.put(
                    self.addrhist,
                    &addr_bytes,
                    &record_entry_bytes,
                    WriteFlags::empty(),
                )?;
                if record.is_input() {
                    // mark corresponding output as spent
                    let _updated = self.mark_addr_hist_record_spent_txn(
                        &mut txn,
                        &addr_script,
                        record.tx_index(),
                    )?;
                }
            }
        }

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

        // Reconstruct transaction index state to remove
        let block_height = Height::from_bytes(&height_bytes)?;
        let ro = self.env.begin_ro_txn()?;
        let transparent_bytes = self.block_transparent_raw_txn(&ro, &height_bytes)?;
        let transparent_entry = StoredEntryVar::<TransparentTxList>::from_bytes(&transparent_bytes)
            .map_err(|e| {
                FinalisedStateError::Custom(format!("transparent CBOR decode error: {e:?}"))
            })?;
        let transparent = transparent_entry.inner().tx();

        let mut addrhist_map: HashMap<AddrScript, Vec<AddrHistRecord>> = HashMap::new();
        let mut spent_outpoints: Vec<Outpoint> = Vec::new();

        for (tx_index, tx_opt) in transparent.iter().enumerate() {
            let tx_index = TxIndex::new(block_height.0, tx_index as u16);
            if let Some(tx) = tx_opt {
                for input in tx.inputs() {
                    let prev_outpoint = Outpoint::new(*input.prevout_txid(), input.prevout_index());
                    spent_outpoints.push(prev_outpoint);

                    if let Ok(prev_output) = self.get_previous_output(prev_outpoint) {
                        let addr_script = AddrScript::new(*prev_output.script_hash());
                        let hist_record = AddrHistRecord::new(
                            tx_index,
                            input.prevout_index() as u16,
                            prev_output.value(),
                            AddrHistRecord::FLAG_IS_INPUT,
                        );
                        match addrhist_map.entry(addr_script) {
                            Entry::Occupied(mut entry) => entry.get_mut().push(hist_record),
                            Entry::Vacant(entry) => {
                                entry.insert(vec![hist_record]);
                            }
                        }
                    }
                }
                for (vout, output) in tx.outputs().iter().enumerate() {
                    let addr_script = AddrScript::new(*output.script_hash());
                    let hist_record = AddrHistRecord::new(
                        tx_index,
                        vout as u16,
                        output.value(),
                        AddrHistRecord::FLAG_MINED,
                    );
                    match addrhist_map.entry(addr_script) {
                        Entry::Occupied(mut entry) => entry.get_mut().push(hist_record),
                        Entry::Vacant(entry) => {
                            entry.insert(vec![hist_record]);
                        }
                    }
                }
            }
        }

        // Delete data from db.
        let mut txn = self.env.begin_rw_txn()?;

        // Delete spent data
        for outpoint in &spent_outpoints {
            let outpoint_bytes = &outpoint.to_bytes()?;
            match txn.del(self.spent, outpoint_bytes, None) {
                Ok(()) | Err(lmdb::Error::NotFound) => {}
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            }
        }

        // Delete addrhist data and mark old outputs spent in this block as unspent
        for (addr_script, records) in &addrhist_map {
            let addr_bytes = &addr_script.to_bytes()?;
            for record in records {
                let packed_record = AddrEventBytes::from_record(record).map_err(|e| {
                    FinalisedStateError::Custom(format!("AddrEventBytes pack error: {e:?}"))
                })?;
                let record_entry_bytes =
                    StoredEntryFixed::new(addr_bytes, packed_record).to_bytes()?;
                match txn.del(self.addrhist, addr_bytes, Some(&record_entry_bytes)) {
                    Ok(()) | Err(lmdb::Error::NotFound) => {}
                    Err(e) => return Err(FinalisedStateError::LmdbError(e)),
                }
                if record.is_input() {
                    let _updated = self.mark_addr_hist_record_unspent_txn(
                        &mut txn,
                        addr_script,
                        record.tx_index(),
                    )?;
                }
            }
        }

        // Delete block data
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

    // **** Internal LMDB transaction methods ****

    /// Heights: Hash -> StoredEntryFixed<Height>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_height_from_hash_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        hash_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let value = txn.get(self.heights, hash_bytes)?;
        Ok(value.to_vec())
    }

    /// Headers: Height -> StoredEntryVar<BlockHeaderData>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_header_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        height_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let bytes = txn.get(self.headers, height_bytes)?;
        Ok(bytes.to_vec())
    }

    /// Headers: Height -> StoredEntry<BlockHeaderData>
    ///
    /// Returns a Vec of serialized entries, for a byte range [start_height, end_height].
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn block_range_header_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        start_height_bytes: &Vec<u8>,
        end_height_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.headers)?;
        for (k, v) in cursor.iter_from(&start_height_bytes[..]) {
            if k > &end_height_bytes[..] {
                break;
            }
            results.push(v.to_vec());
        }
        Ok(results)
    }

    /// Txids: Height -> StoredEntryVar<TxidList>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_txids_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        height_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let bytes = txn.get(self.txids, height_bytes)?;
        Ok(bytes.to_vec())
    }

    /// Txids: Height -> StoredEntryVar<TxidList>
    ///
    /// Returns a Vec of serialized entries, for a byte range [start_height, end_height].
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn block_range_txids_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        start_height_bytes: &Vec<u8>,
        end_height_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.txids)?;
        for (k, v) in cursor.iter_from(&start_height_bytes[..]) {
            if k > &end_height_bytes[..] {
                break;
            }
            results.push(v.to_vec());
        }
        Ok(results)
    }

    /// Txids: TxIndex -> Txid (raw bytes)
    ///
    /// Returns the serialized 32-byte txid for the given TxIndex.
    ///
    /// This is an optimized version that avoids deserializing the entire TxidList.
    fn txindex_to_txid_bytes_txn(
        &self,
        txn: &lmdb::RoTransaction,
        tx_index: TxIndex,
    ) -> Result<[u8; 32], FinalisedStateError> {
        use std::io::Cursor;

        let height = Height::try_from(tx_index.block_index())
            .map_err(|e| FinalisedStateError::Custom(e.to_string()))?;
        let height_bytes = height.to_bytes()?;

        let raw = txn.get(self.txids, &height_bytes)?;
        let mut cursor = Cursor::new(raw);

        // Skip [0] StoredEntry version
        cursor.set_position(1);

        // Read CompactSize: length of serialized body
        let _body_len = CompactSize::read(&mut cursor)
            .map_err(|e| FinalisedStateError::Custom(format!("compact size read error: {e}")))?;

        // Read [1] Record version (skip 1 byte)
        cursor.set_position(cursor.position() + 1);

        // Read CompactSize: number of txids
        let list_len = CompactSize::read(&mut cursor)
            .map_err(|e| FinalisedStateError::Custom(format!("txid list len error: {e}")))?;

        // Bounds check TODO: FIX THIS!
        let idx = tx_index.tx_index() as usize;
        if idx >= list_len as usize {
            return Err(FinalisedStateError::Custom(
                "tx_index out of range in txid list".to_string(),
            ));
        }

        // Each txid is 32 bytes, so skip to the desired one
        cursor.set_position(cursor.position() + (idx as u64) * 32);

        let mut txid_bytes = [0u8; 32];
        cursor
            .read_exact(&mut txid_bytes)
            .map_err(|e| FinalisedStateError::Custom(format!("txid read error: {e}")))?;

        Ok(txid_bytes)
    }

    /// Transparent: Height -> StoredEntryVar<TransparentTxList>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_transparent_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        height_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let bytes = txn.get(self.transparent, height_bytes)?;
        Ok(bytes.to_vec())
    }

    /// Transparent: Height -> StoredEntryVar<TransparentTxList>
    ///
    /// Returns a Vec of serialized entries, for a byte range [start_height, end_height].
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn block_range_transparent_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        start_height_bytes: &Vec<u8>,
        end_height_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.transparent)?;
        for (k, v) in cursor.iter_from(&start_height_bytes[..]) {
            if k > &end_height_bytes[..] {
                break;
            }
            results.push(v.to_vec());
        }
        Ok(results)
    }

    /// Sapling: Height -> StoredEntryVar<SaplingTxList>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_sapling_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        height_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let bytes = txn.get(self.sapling, height_bytes)?;
        Ok(bytes.to_vec())
    }

    /// Sapling: Height -> StoredEntryVar<SaplingTxList>
    ///
    /// Returns a Vec of serialized entries, for a byte range [start_height, end_height].
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn block_range_sapling_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        start_height_bytes: &Vec<u8>,
        end_height_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.sapling)?;
        for (k, v) in cursor.iter_from(&start_height_bytes[..]) {
            if k > &end_height_bytes[..] {
                break;
            }
            results.push(v.to_vec());
        }
        Ok(results)
    }

    /// Orchard: Height -> StoredEntryVar<OrchardTxList>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_orchard_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        height_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let bytes = txn.get(self.orchard, height_bytes)?;
        Ok(bytes.to_vec())
    }

    /// Orchard: Height -> StoredEntryVar<OrchardTxList>
    ///
    /// Returns a Vec of serialized entries, for a byte range [start_height, end_height].
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn block_range_orchard_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        start_height_bytes: &Vec<u8>,
        end_height_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.orchard)?;
        for (k, v) in cursor.iter_from(&start_height_bytes[..]) {
            if k > &end_height_bytes[..] {
                break;
            }
            results.push(v.to_vec());
        }
        Ok(results)
    }

    /// Commitment tree data: Height -> StoredEntryFixed<Vec<CommitmentTreeData>>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn block_commitment_tree_data_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        height_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let bytes = txn.get(self.commitment_tree_data, height_bytes)?;
        Ok(bytes.to_vec())
    }

    /// Commitment tree data: Height -> StoredEntryFixed<Vec<CommitmentTreeData>>
    ///
    /// Returns a Vec of serialized entries, for a byte range [start_height, end_height].
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn block_range_commitment_tree_data_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        start_height_bytes: &Vec<u8>,
        end_height_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.commitment_tree_data)?;
        for (k, v) in cursor.iter_from(&start_height_bytes[..]) {
            if k > &end_height_bytes[..] {
                break;
            }
            results.push(v.to_vec());
        }
        Ok(results)
    }

    /// Spent: Outpoint -> StoredEntryFixed<TxIndex>
    ///
    /// Returns the raw bytes for the stored TxIndex of a spent output.
    ///
    /// This method takes an *open* read-only LMDB transaction.
    /// so multiple calls can be chained internally.
    fn spent_tx_index_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        outpoint_bytes: &Vec<u8>,
    ) -> Result<Vec<u8>, FinalisedStateError> {
        let value = txn.get(self.spent, &outpoint_bytes)?;
        Ok(value.to_vec())
    }

    /// AddrHist: AddrScript -> StoredEntryFixed<AddrEventBytes> (DUP_SORT + DUP_FIXED)
    ///
    /// Returns a cursor over all records in the db.
    ///
    /// The caller is responsible for finding the correct key / record.
    ///
    /// The caller must close the cursor before committing the transaction.
    fn addr_hist_cursor<'txn>(
        &self,
        txn: &'txn lmdb::RoTransaction,
    ) -> Result<lmdb::RoCursor<'txn>, FinalisedStateError> {
        let cursor = txn.open_ro_cursor(self.addrhist)?;
        Ok(cursor)
    }

    /// Returns all raw AddrHist records for a given AddrScript.
    ///
    /// Returns a Vec of serialized entries, for given addr_script.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Uses cursor search for efficient range lookup.
    fn addr_hist_records_by_addr_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        addr_script_bytes: &Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut cursor = txn.open_ro_cursor(self.addrhist)?;
        let mut records = Vec::new();

        for (key, val) in cursor.iter_dup_of(&addr_script_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                break;
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                break;
            }
            records.push(val.to_vec());
        }
        Ok(records)
    }

    /// Returns all raw AddrHist records for a given AddrScript and TxIndex.
    ///
    /// Returns a Vec of serialized entries, for given addr_script and ix_index.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    ///
    /// Efficiently filters by matching block + tx index bytes in-place.
    fn addr_hist_records_by_addr_and_index_raw_txn(
        &self,
        txn: &lmdb::RoTransaction,
        addr_script_bytes: &Vec<u8>,
        tx_index: TxIndex,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let mut cursor = txn.open_ro_cursor(self.addrhist)?;
        let mut results = Vec::new();

        for (key, val) in cursor.iter_dup_of(&addr_script_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }

            // Check tx_index match without deserializing
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..5] height
            // - [6..7] tx_index
            // - [8..9] vout
            // - [10] flags
            // - [11..18] value
            // - [19..50] checksum

            let block_index = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
            let tx_idx = u16::from_be_bytes([val[6], val[7]]);

            if block_index == tx_index.block_index() && tx_idx == tx_index.tx_index() {
                results.push(val.to_vec());
            }
        }

        Ok(results)
    }

    /// Returns every distinct `TxIndex` touching `addr_script` in the height
    /// range `[start_height, end_height]` (inclusive), **deduplicated**.
    ///
    /// Ordering: returned `Vec` is sorted by `(height, tx_index)`.
    fn addr_hist_tx_indices_by_addr_and_range_txn(
        &self,
        txn: &lmdb::RoTransaction,
        addr_script_bytes: &Vec<u8>,
        start_height: Height,
        end_height: Height,
    ) -> Result<Vec<TxIndex>, FinalisedStateError> {
        let mut cursor = txn.open_ro_cursor(self.addrhist)?;
        let mut set: HashSet<TxIndex> = HashSet::new();

        for (key, val) in cursor.iter_dup_of(&addr_script_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN
                || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
            {
                continue;
            }

            // Parse the tx_index out of val:
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..5] height
            // - [6..7] tx_index
            // - [8..9] vout
            // - [10] flags
            // - [11..18] value
            // - [19..50] checksum

            let h = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
            if h < start_height.0 || h > end_height.0 {
                continue;
            }

            let tx_idx = u16::from_be_bytes([val[6], val[7]]);
            set.insert(TxIndex::new(h, tx_idx));
        }

        let mut vec: Vec<_> = set.into_iter().collect();
        vec.sort_by_key(|txi| (txi.block_index(), txi.tx_index()));
        Ok(vec)
    }

    /// Returns every *unspent mined output* (UTXO) for `addr_script` in the
    /// height range `[start_height, end_height]`.
    ///
    /// A record qualifies when:
    ///   * `FLAG_MINED` is set
    ///   * `FLAG_SPENT` is **not** set
    fn addr_hist_utxos_by_addr_and_range_txn(
        &self,
        txn: &lmdb::RoTransaction,
        addr_script_bytes: &Vec<u8>,
        start_height: Height,
        end_height: Height,
    ) -> Result<Vec<(TxIndex, u16, u64)>, FinalisedStateError> {
        let mut cursor = txn.open_ro_cursor(self.addrhist)?;
        let mut utxos = Vec::new();

        for (key, val) in cursor.iter_dup_of(&addr_script_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN
                || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
            {
                continue;
            }

            // Parse the tx_index out of val:
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..5] height
            // - [6..7] tx_index
            // - [8..9] vout
            // - [10] flags
            // - [11..18] value
            // - [19..50] checksum

            let height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
            if height < start_height.0 || height > end_height.0 {
                continue;
            }

            let flags = val[10];
            if (flags & AddrEventBytes::FLAG_MINED == 0)
                || (flags & AddrEventBytes::FLAG_SPENT != 0)
            {
                continue;
            }

            let tx_idx = u16::from_be_bytes([val[6], val[7]]);
            let vout = u16::from_be_bytes([val[8], val[9]]);
            let value = u64::from_le_bytes([
                val[11], val[12], val[13], val[14], val[15], val[16], val[17], val[18],
            ]);

            utxos.push((TxIndex::new(height, tx_idx), vout, value));
        }

        Ok(utxos)
    }

    /// Computes the net transparent balance change for `addr_script` across
    /// `[start_height, end_height]`, using `i128` to avoid overflow.
    ///
    /// + Add `value` when `FLAG_MINED`
    /// + Sub `value` when `FLAG_IS_INPUT`
    fn addr_hist_balance_by_addr_and_range_txn(
        &self,
        txn: &lmdb::RoTransaction,
        addr_script_bytes: &Vec<u8>,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        let mut cursor = txn.open_ro_cursor(self.addrhist)?;
        let mut balance: i64 = 0;

        for (key, val) in cursor.iter_dup_of(&addr_script_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN
                || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
            {
                continue;
            }

            // Parse the tx_index out of val:
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..5] height
            // - [6..7] tx_index
            // - [8..9] vout
            // - [10] flags
            // - [11..18] value
            // - [19..50] checksum

            let height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
            if height < start_height.0 || height > end_height.0 {
                continue;
            }

            let flags = val[10];
            let value = u64::from_le_bytes([
                val[11], val[12], val[13], val[14], val[15], val[16], val[17], val[18],
            ]) as i64;

            if flags & AddrEventBytes::FLAG_IS_INPUT != 0 {
                balance -= value;
            } else if flags & AddrEventBytes::FLAG_MINED != 0 {
                balance += value;
            }
        }

        Ok(balance)
    }

    /// Mark a specific AddrHistRecord as spent in the addrhist DB.
    /// Looks up a record by script and tx_index, sets FLAG_SPENT, and updates it in place.
    ///
    /// Returns Ok(true) if a record was updated, Ok(false) if not found, or Err on DB error.
    ///
    /// This method takes an *open* read/write LMDB transaction
    /// so multiple calls can be chained internally.
    pub fn mark_addr_hist_record_spent_txn(
        &self,
        txn: &mut lmdb::RwTransaction,
        addr_script: &AddrScript,
        tx_index: TxIndex,
    ) -> Result<bool, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let mut cur = txn.open_rw_cursor(self.addrhist)?;

        for (key, val) in cur.iter_dup_of(&addr_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }
            let mut hist_record = [0u8; StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN];
            hist_record.copy_from_slice(val);

            // Parse the tx_index out of arr:
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..5] height
            // - [6..7] tx_index
            // - [8..9] vout
            // - [10] flags
            // - [11..18] value
            // - [19..50] checksum

            let block_index = u32::from_be_bytes([
                hist_record[2],
                hist_record[3],
                hist_record[4],
                hist_record[5],
            ]);
            let t_idx = u16::from_be_bytes([hist_record[6], hist_record[7]]);

            if block_index == tx_index.block_index() && t_idx == tx_index.tx_index() {
                if hist_record[10] & AddrHistRecord::FLAG_SPENT != 0 {
                    continue;
                }
                // Mark as spent (set the flag)
                hist_record[10] |= AddrHistRecord::FLAG_SPENT;

                // Update checksum
                let checksum = StoredEntryFixed::<AddrEventBytes>::blake2b256(
                    &[&addr_bytes, &hist_record[1..19]].concat(),
                );
                hist_record[19..51].copy_from_slice(&checksum);

                // Overwrite in place
                cur.put(&addr_bytes, &hist_record, WriteFlags::CURRENT)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Mark a specific AddrHistRecord as unspent in the addrhist DB.
    /// Looks up a record by script and tx_index, sets FLAG_SPENT, and updates it in place.
    ///
    /// Returns Ok(true) if a record was updated, Ok(false) if not found, or Err on DB error.
    ///
    /// This method takes an *open* read/write LMDB transaction
    /// so multiple calls can be chained internally.
    pub fn mark_addr_hist_record_unspent_txn(
        &self,
        txn: &mut lmdb::RwTransaction,
        addr_script: &AddrScript,
        tx_index: TxIndex,
    ) -> Result<bool, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let mut cur = txn.open_rw_cursor(self.addrhist)?;

        for (key, val) in cur.iter_dup_of(&addr_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }
            let mut hist_record = [0u8; StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN];
            hist_record.copy_from_slice(val);

            // Parse the tx_index out of arr:
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..5] height
            // - [6..7] tx_index
            // - [8..9] vout
            // - [10] flags
            // - [11..18] value
            // - [19..50] checksum

            let block_index = u32::from_be_bytes([
                hist_record[2],
                hist_record[3],
                hist_record[4],
                hist_record[5],
            ]);
            let t_idx = u16::from_be_bytes([hist_record[6], hist_record[7]]);

            if block_index == tx_index.block_index() && t_idx == tx_index.tx_index() {
                if hist_record[10] & AddrHistRecord::FLAG_SPENT != 0 {
                    continue;
                }
                // Mark as unspent (unset the flag)
                hist_record[10] &= !AddrHistRecord::FLAG_SPENT;

                // Update checksum
                let checksum = StoredEntryFixed::<AddrEventBytes>::blake2b256(
                    &[&addr_bytes, &hist_record[1..19]].concat(),
                );
                hist_record[19..51].copy_from_slice(&checksum);

                // Overwrite in place
                cur.put(&addr_bytes, &hist_record, WriteFlags::CURRENT)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Metadata: singleton entry "metadata" -> StoredEntryFixed<DbMetadata>
    ///
    /// Returns slice of data holding serialised entry.
    ///
    /// This method takes an *open* read only LMDB transaction
    /// so multiple calls can be chained internally.
    fn metadata_raw_txn(&self, txn: &lmdb::RoTransaction) -> Result<Vec<u8>, FinalisedStateError> {
        let value = txn
            .get(self.metadata, b"metadata")
            .map_err(FinalisedStateError::LmdbError)?;
        Ok(value.to_vec())
    }

    // *** Internal DB methods ***

    /// Fetches the previous transparent output for the given outpoint.
    /// Returns `TxOutCompact` or an explicit error if not found or invalid.
    ///
    /// Used to build addrhist records.
    fn get_previous_output(&self, outpoint: Outpoint) -> Result<TxOutCompact, FinalisedStateError> {
        // Find the tx’s location in the chain
        let prev_txid = Hash::from(*outpoint.prev_txid());
        let tx_index = self
            .find_txid_index(&prev_txid)?
            .ok_or_else(|| FinalisedStateError::Custom("Previous txid not found".into()))?;

        // Fetch the output from the transparent db.
        self.get_txout_by_index_and_outpoint(tx_index, &outpoint)?
            .ok_or_else(|| {
                FinalisedStateError::Custom("Previous output not found at given index".into())
            })
    }

    /// Finds a TxIndex [block_height, tx_index] from a given txid.
    /// Used for Txid based lookup in transaction DBs.
    fn find_txid_index(&self, txid: &Hash) -> Result<Option<TxIndex>, FinalisedStateError> {
        let ro = self.env.begin_ro_txn()?;
        let mut cursor = ro.open_ro_cursor(self.txids)?;

        let target: [u8; 32] = (*txid).into();

        for (height_bytes, stored_bytes) in cursor.iter() {
            if let Some(tx_idx) = Self::find_txid_position_in_stored_txidlist(&target, stored_bytes)
            {
                let height = Height::from_bytes(height_bytes)?;
                return Ok(Some(TxIndex::new(height.0, tx_idx as u16)));
            }
        }
        Ok(None)
    }

    /// Efficiently scans a raw `StoredEntryVar<TxidList>` buffer to locate the index
    /// of a given transaction ID without full deserialization.
    ///
    /// The format is:
    /// - 1 byte: StoredEntryVar version
    /// - CompactSize: length of the item
    /// - 1 byte: TxidList version
    /// - CompactSize: number of the item
    /// - N x (1 byte + 32 bytes): tagged Hash items
    /// - 32 bytes: checksum
    ///
    /// # Arguments
    /// - `target_txid`: A `[u8; 32]` representing the transaction ID to match.
    /// - `stored`: Raw LMDB byte slice from a `StoredEntryVar<TxidList>`.
    ///
    /// # Returns
    /// - `Some(index)` if a matching txid is found
    /// - `None` if the format is invalid or no match
    fn find_txid_position_in_stored_txidlist(
        target_txid: &[u8; 32],
        stored: &[u8],
    ) -> Option<usize> {
        const CHECKSUM_LEN: usize = 32;

        // Check is at least sotred version + compactsize + checksum
        // else return none.
        if stored.len() < Hash::VERSION_TAG_LEN + 8 + CHECKSUM_LEN {
            return None;
        }

        let mut cursor = &stored[Hash::VERSION_TAG_LEN..];
        let item_len = CompactSize::read(&mut cursor).ok()? as usize;
        if cursor.len() < item_len + CHECKSUM_LEN {
            return None;
        }

        let (_record_version, mut remaining) = cursor.split_first()?;
        let vec_len = CompactSize::read(&mut remaining).ok()? as usize;

        for idx in 0..vec_len {
            // Each entry is 1-byte tag + 32-byte hash
            let (_tag, rest) = remaining.split_first()?;
            let hash_bytes: &[u8; 32] = rest.get(..32)?.try_into().ok()?;
            if hash_bytes == target_txid {
                return Some(idx);
            }
            remaining = &rest[32..];
        }

        None
    }

    /// Fetches the specific TxOutCompact for a given TxIndex and Outpoint.
    /// Looks up the block, scans only the necessary bytes, and returns the output if found.
    ///
    /// # Arguments
    /// - `tx_index`: identifies the block and tx position
    /// - `outpoint`: contains the output index to look up
    ///
    /// # Returns
    /// - `Ok(Some(TxOutCompact))` if present, `Ok(None)` if missing, `Err` on DB error
    fn get_txout_by_index_and_outpoint(
        &self,
        tx_index: TxIndex,
        outpoint: &Outpoint,
    ) -> Result<Option<TxOutCompact>, FinalisedStateError> {
        let block_height = tx_index.block_index();
        let tx_pos = tx_index.tx_index() as usize;
        let out_pos = outpoint.prev_index() as usize;

        let ro = self.env.begin_ro_txn()?;
        let height_key = Height(block_height).to_bytes()?;
        let stored_bytes = ro.get(self.transparent, &height_key)?;

        Ok(Self::find_txout_in_stored_transparenttxlist(
            stored_bytes,
            tx_pos,
            out_pos,
        ))
    }

    /// Efficiently scans a raw `StoredEntryVar<TransparentTxList>` buffer to locate the
    /// specific output at [tx_idx, output_idx] without full deserialization.
    ///
    /// # Arguments
    /// - `stored`: the raw LMDB byte buffer
    /// - `target_tx_idx`: index in the tx list
    /// - `target_output_idx`: index in the outputs of that tx
    ///
    /// # Returns
    /// - `Some(TxOutCompact)` if found and present, otherwise `None`
    fn find_txout_in_stored_transparenttxlist(
        stored: &[u8],
        target_tx_idx: usize,
        target_output_idx: usize,
    ) -> Option<TxOutCompact> {
        const CHECKSUM_LEN: usize = 32;

        if stored.len() < Hash::VERSION_TAG_LEN + 8 + CHECKSUM_LEN {
            return None;
        }

        let mut cursor = &stored[Hash::VERSION_TAG_LEN..];
        let item_len = CompactSize::read(&mut cursor).ok()? as usize;
        if cursor.len() < item_len + CHECKSUM_LEN {
            return None;
        }

        let (_record_version, mut remaining) = cursor.split_first()?;
        let vec_len = CompactSize::read(&mut remaining).ok()? as usize;

        for i in 0..vec_len {
            let (option_tag, rest) = remaining.split_first()?;
            remaining = rest;

            if *option_tag == 0 {
                // None: nothing to skip, go to next
                if i == target_tx_idx {
                    return None;
                }
            } else if *option_tag == 1 {
                let vin_len = CompactSize::read(&mut remaining).ok()? as usize;
                let vin_bytes = TxInCompact::VERSIONED_LEN * vin_len;
                if remaining.len() < vin_bytes {
                    return None;
                }
                remaining = &remaining[vin_bytes..];

                let vout_len = CompactSize::read(&mut remaining).ok()? as usize;
                let vout_bytes = TxOutCompact::VERSIONED_LEN * vout_len;
                if remaining.len() < vout_bytes {
                    return None;
                }

                if i == target_tx_idx {
                    if target_output_idx >= vout_len {
                        return None;
                    }
                    let offset = TxOutCompact::VERSIONED_LEN * target_output_idx;
                    let out_bytes = &remaining[offset..offset + TxOutCompact::VERSIONED_LEN];
                    let (_record_version, output_bytes) = out_bytes.split_first()?;
                    let tx_out = TxOutCompact::from_bytes(output_bytes).ok()?;
                    return Some(tx_out);
                }
                remaining = &remaining[vout_bytes..];
            } else {
                // Non-canonical Option tag
                return None;
            }
        }
        None
    }

    // *** Public fetcher methods - Used by DbReader ***

    /// Fetch the block height in the main chain for a given block hash.
    pub fn get_block_height_by_hash(&self, hash: Hash) -> Result<Height, FinalisedStateError> {
        let height = self.resolve_validated_hash_or_height(HashOrHeight::Hash(hash.into()))?;
        Ok(height)
    }

    /// Fetch the height range for the given block hashes.
    pub fn get_block_range_by_hash(
        &self,
        start_hash: Hash,
        end_hash: Hash,
    ) -> Result<(Height, Height), FinalisedStateError> {
        let start_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Hash(start_hash.into()))?;
        let end_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Hash(end_hash.into()))?;

        let (validated_start, validated_end) =
            self.validate_block_range(start_height, end_height)?;

        Ok((validated_start, validated_end))
    }

    /// Fetch block header data by height.
    pub fn get_block_header_data(
        &self,
        height: Height,
    ) -> Result<BlockHeaderData, FinalisedStateError> {
        let validated_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Height(height.into()))?;
        let height_bytes = validated_height.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw = self.block_header_raw_txn(&txn, &height_bytes)?;
        let entry = StoredEntryVar::from_bytes(&raw)
            .map_err(|e| FinalisedStateError::Custom(format!("header decode error: {e}")))?;

        Ok(*entry.inner())
    }

    /// Fetches block headers for the given height range.
    ///
    /// Uses cursor based fetch.
    pub fn get_block_range_headers(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<BlockHeaderData>, FinalisedStateError> {
        self.validate_block_range(start, end)?;
        let start_bytes = start.to_bytes()?;
        let end_bytes = end.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw_entries = self.block_range_header_raw_txn(&txn, &start_bytes, &end_bytes)?;

        raw_entries
            .into_iter()
            .map(|bytes| {
                StoredEntryVar::<BlockHeaderData>::from_bytes(&bytes)
                    .map(|e| *e.inner())
                    .map_err(|e| FinalisedStateError::Custom(format!("header decode error: {e}")))
            })
            .collect()
    }

    /// Fetch block txids by height.
    pub fn get_block_txids(&self, height: Height) -> Result<TxidList, FinalisedStateError> {
        let validated_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Height(height.into()))?;
        let height_bytes = validated_height.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw = self.block_txids_raw_txn(&txn, &height_bytes)?;
        let entry: StoredEntryVar<TxidList> = StoredEntryVar::from_bytes(&raw)
            .map_err(|e| FinalisedStateError::Custom(format!("txids decode error: {e}")))?;

        Ok(entry.inner().clone())
    }

    /// Fetches block txids for the given height range.
    ///
    /// Uses cursor based fetch.
    pub fn get_block_range_txids(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<TxidList>, FinalisedStateError> {
        self.validate_block_range(start, end)?;
        let start_bytes = start.to_bytes()?;
        let end_bytes = end.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw_entries = self.block_range_txids_raw_txn(&txn, &start_bytes, &end_bytes)?;

        raw_entries
            .into_iter()
            .map(|bytes| {
                StoredEntryVar::<TxidList>::from_bytes(&bytes)
                    .map(|e| e.inner().clone())
                    .map_err(|e| FinalisedStateError::Custom(format!("txids decode error: {e}")))
            })
            .collect()
    }

    /// Fetch block transparent transaction data by height.
    pub fn get_block_transparent(
        &self,
        height: Height,
    ) -> Result<TransparentTxList, FinalisedStateError> {
        let validated_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Height(height.into()))?;
        let height_bytes = validated_height.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw = self.block_transparent_raw_txn(&txn, &height_bytes)?;
        let entry: StoredEntryVar<TransparentTxList> = StoredEntryVar::from_bytes(&raw)
            .map_err(|e| FinalisedStateError::Custom(format!("transparent decode error: {e}")))?;

        Ok(entry.inner().clone())
    }

    /// Fetches block transparent tx data for the given height range.
    ///
    /// Uses cursor based fetch.
    pub fn get_block_range_transparent(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<TransparentTxList>, FinalisedStateError> {
        self.validate_block_range(start, end)?;
        let start_bytes = start.to_bytes()?;
        let end_bytes = end.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw_entries = self.block_range_transparent_raw_txn(&txn, &start_bytes, &end_bytes)?;

        raw_entries
            .into_iter()
            .map(|bytes| {
                StoredEntryVar::<TransparentTxList>::from_bytes(&bytes)
                    .map(|e| e.inner().clone())
                    .map_err(|e| {
                        FinalisedStateError::Custom(format!("transparent decode error: {e}"))
                    })
            })
            .collect()
    }

    /// Fetch block sapling transaction data by height.
    pub fn get_block_sapling(&self, height: Height) -> Result<SaplingTxList, FinalisedStateError> {
        let validated_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Height(height.into()))?;
        let height_bytes = validated_height.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw = self.block_sapling_raw_txn(&txn, &height_bytes)?;
        let entry: StoredEntryVar<SaplingTxList> = StoredEntryVar::from_bytes(&raw)
            .map_err(|e| FinalisedStateError::Custom(format!("sapling decode error: {e}")))?;

        Ok(entry.inner().clone())
    }

    /// Fetches block sapling tx data for the given height range.
    ///
    /// Uses cursor based fetch.
    pub fn get_block_range_sapling(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<SaplingTxList>, FinalisedStateError> {
        self.validate_block_range(start, end)?;
        let start_bytes = start.to_bytes()?;
        let end_bytes = end.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw_entries = self.block_range_sapling_raw_txn(&txn, &start_bytes, &end_bytes)?;

        raw_entries
            .into_iter()
            .map(|bytes| {
                StoredEntryVar::<SaplingTxList>::from_bytes(&bytes)
                    .map(|e| e.inner().clone())
                    .map_err(|e| FinalisedStateError::Custom(format!("sapling decode error: {e}")))
            })
            .collect()
    }

    /// Fetch block orchard transaction data by height.
    pub fn get_block_orchard(&self, height: Height) -> Result<OrchardTxList, FinalisedStateError> {
        let validated_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Height(height.into()))?;
        let height_bytes = validated_height.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw = self.block_orchard_raw_txn(&txn, &height_bytes)?;
        let entry: StoredEntryVar<OrchardTxList> = StoredEntryVar::from_bytes(&raw)
            .map_err(|e| FinalisedStateError::Custom(format!("orchard decode error: {e}")))?;

        Ok(entry.inner().clone())
    }

    /// Fetches block orchard tx data for the given height range.
    ///
    /// Uses cursor based fetch.
    pub fn get_block_range_orchard(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<OrchardTxList>, FinalisedStateError> {
        self.validate_block_range(start, end)?;
        let start_bytes = start.to_bytes()?;
        let end_bytes = end.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw_entries = self.block_range_orchard_raw_txn(&txn, &start_bytes, &end_bytes)?;

        raw_entries
            .into_iter()
            .map(|bytes| {
                StoredEntryVar::<OrchardTxList>::from_bytes(&bytes)
                    .map(|e| e.inner().clone())
                    .map_err(|e| FinalisedStateError::Custom(format!("orchard decode error: {e}")))
            })
            .collect()
    }

    /// Fetch block commitment tree data by height.
    pub fn get_block_commitment_tree_data(
        &self,
        height: Height,
    ) -> Result<CommitmentTreeData, FinalisedStateError> {
        let validated_height =
            self.resolve_validated_hash_or_height(HashOrHeight::Height(height.into()))?;
        let height_bytes = validated_height.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw = self.block_commitment_tree_data_raw_txn(&txn, &height_bytes)?;
        let entry = StoredEntryFixed::from_bytes(&raw).map_err(|e| {
            FinalisedStateError::Custom(format!("commitment_tree decode error: {e}"))
        })?;

        Ok(entry.item)
    }

    /// Fetches block commitment tree data for the given height range.
    ///
    /// Uses cursor based fetch.
    pub fn get_block_range_commitment_tree_data(
        &self,
        start: Height,
        end: Height,
    ) -> Result<Vec<CommitmentTreeData>, FinalisedStateError> {
        self.validate_block_range(start, end)?;
        let start_bytes = start.to_bytes()?;
        let end_bytes = end.to_bytes()?;

        let txn = self.env.begin_ro_txn()?;
        let raw_entries =
            self.block_range_commitment_tree_data_raw_txn(&txn, &start_bytes, &end_bytes)?;

        raw_entries
            .into_iter()
            .map(|bytes| {
                StoredEntryFixed::<CommitmentTreeData>::from_bytes(&bytes)
                    .map(|e| e.item.clone())
                    .map_err(|e| {
                        FinalisedStateError::Custom(format!("commitment_tree decode error: {e}"))
                    })
            })
            .collect()
    }

    /// Fetch the `TxIndex` that spent a given outpoint, if any.
    ///
    /// Returns:
    /// - `Ok(Some(TxIndex))` if the outpoint is spent.
    /// - `Ok(None)` if no entry exists (not spent or not known).
    /// - `Err(...)` on deserialization or DB error.
    pub fn prev_output_index(
        &self,
        outpoint: Outpoint,
    ) -> Result<Option<TxIndex>, FinalisedStateError> {
        let key = outpoint.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        match txn.get(self.spent, &key) {
            Ok(bytes) => {
                let entry = StoredEntryFixed::<TxIndex>::from_bytes(bytes).map_err(|e| {
                    FinalisedStateError::Custom(format!("spent entry decode error: {e}"))
                })?;
                Ok(Some(entry.item))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(FinalisedStateError::LmdbError(e)),
        }
    }

    /// Fetch the `TxIndex` entries for a batch of outpoints.
    ///
    /// For each input:
    /// - Returns `Some(TxIndex)` if spent,
    /// - `None` if not found,
    /// - or returns `Err` immediately if any DB or decode error occurs.
    pub fn prev_outputs_index(
        &self,
        outpoints: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxIndex>>, FinalisedStateError> {
        let txn = self.env.begin_ro_txn()?;

        outpoints
            .into_iter()
            .map(|outpoint| {
                let key = outpoint.to_bytes()?;
                match txn.get(self.spent, &key) {
                    Ok(bytes) => {
                        let entry =
                            StoredEntryFixed::<TxIndex>::from_bytes(bytes).map_err(|e| {
                                FinalisedStateError::Custom(format!(
                                    "spent entry decode error for {outpoint:?}: {e}"
                                ))
                            })?;
                        Ok(Some(entry.item))
                    }
                    Err(lmdb::Error::NotFound) => Ok(None),
                    Err(e) => Err(FinalisedStateError::LmdbError(e)),
                }
            })
            .collect()
    }

    /// Fetch all address history records for a given transparent address.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more valid records exist,
    /// - `Ok(None)` if no records exist (not an error),
    /// - `Err(...)` if any decoding or DB error occurs.
    pub fn addr_records(
        &self,
        addr_script: AddrScript,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        let raw_records = match self.addr_hist_records_by_addr_raw_txn(&txn, &addr_bytes) {
            Ok(records) => records,
            Err(FinalisedStateError::LmdbError(lmdb::Error::NotFound)) => return Ok(None),
            Err(e) => return Err(e),
        };
        if raw_records.is_empty() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(raw_records.len());
        for val in raw_records {
            let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(&val)
                .map_err(|e| FinalisedStateError::Custom(format!("addrhist decode error: {e}")))?;
            records.push(entry.item);
        }

        Ok(Some(records))
    }

    /// Fetch all address history records for a given address and TxIndex.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more matching records are found at that index,
    /// - `Ok(None)` if no matching records exist (not an error),
    /// - `Err(...)` on decode or DB failure.
    pub fn addr_and_index_records(
        &self,
        addr_script: AddrScript,
        tx_index: TxIndex,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        let raw_records =
            match self.addr_hist_records_by_addr_and_index_raw_txn(&txn, &addr_bytes, tx_index) {
                Ok(records) => records,
                Err(FinalisedStateError::LmdbError(lmdb::Error::NotFound)) => return Ok(None),
                Err(e) => return Err(e),
            };
        if raw_records.is_empty() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(raw_records.len());

        for val in raw_records {
            let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(&val)
                .map_err(|e| FinalisedStateError::Custom(format!("addrhist decode error: {e}")))?;
            records.push(entry.item);
        }

        Ok(Some(records))
    }

    /// Fetch all distinct `TxIndex` values for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more matching records are found,
    /// - `Ok(None)` if no matches found (not an error),
    /// - `Err(...)` on decode or DB failure.
    pub fn addr_tx_indexes_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<TxIndex>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        let indices = self.addr_hist_tx_indices_by_addr_and_range_txn(
            &txn,
            &addr_bytes,
            start_height,
            end_height,
        )?;

        if indices.is_empty() {
            Ok(None)
        } else {
            Ok(Some(indices))
        }
    }

    /// Fetch all UTXOs (unspent mined outputs) for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Each entry is `(TxIndex, vout, value)`.
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more UTXOs are found,
    /// - `Ok(None)` if none found (not an error),
    /// - `Err(...)` on decode or DB failure.
    pub fn addr_utxos_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<(TxIndex, u16, u64)>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        let utxos = self.addr_hist_utxos_by_addr_and_range_txn(
            &txn,
            &addr_bytes,
            start_height,
            end_height,
        )?;

        if utxos.is_empty() {
            Ok(None)
        } else {
            Ok(Some(utxos))
        }
    }

    /// Computes the transparent balance change for `addr_script` over the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Includes:
    /// - `+value` for mined outputs
    /// - `−value` for spent inputs
    ///
    /// Returns the signed net value as `i64`, or error on failure.
    pub fn addr_balance_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        self.addr_hist_balance_by_addr_and_range_txn(&txn, &addr_bytes, start_height, end_height)
    }

    /// Fetch database metadata.
    pub fn get_metadata(&self) -> Result<DbMetadata, FinalisedStateError> {
        let txn = self.env.begin_ro_txn()?;
        let raw = self.metadata_raw_txn(&txn)?;
        let entry = StoredEntryFixed::from_bytes(&raw)
            .map_err(|e| FinalisedStateError::Custom(format!("metadata decode error: {e}")))?;

        Ok(entry.item)
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

    // *** DbReader creation ***

    /// Create a read-only facade backed by *this* live database.
    pub fn to_reader(&self) -> DbReader<'_> {
        DbReader { inner: self }
    }

    // *** Internal DB validation / varification ***

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
        let header_entry = {
            let raw = ro
                .get(self.headers, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<BlockHeaderData>::from_bytes(raw)
                .map_err(|e| fail(&format!("header corrupt data: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("header checksum mismatch"));
            }
            entry
        };

        // *** txids ***
        let txid_list_entry = {
            let raw = ro
                .get(self.txids, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<TxidList>::from_bytes(raw)
                .map_err(|e| fail(&format!("txids corrupt data: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("txids checksum mismatch"));
            }
            entry
        };

        // *** transparent ***
        let transparent_tx_list = {
            let raw = ro.get(self.transparent, &height_key)?;
            let entry = StoredEntryVar::<TransparentTxList>::from_bytes(raw)
                .map_err(|e| fail(&format!("transparent corrupt data: {e}")))?;
            if !entry.verify(&height_key) {
                return Err(fail("transparent checksum mismatch"));
            }
            entry
        };

        // *** sapling ***
        {
            let raw = ro
                .get(self.sapling, &height_key)
                .map_err(|e| FinalisedStateError::LmdbError(e))?;
            let entry = StoredEntryVar::<SaplingTxList>::from_bytes(raw)
                .map_err(|e| fail(&format!("sapling corrupt data: {e}")))?;
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
                .map_err(|e| fail(&format!("orchard corrupt data: {e}")))?;
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

        // *** Parent block hash validation (chain continuity) ***
        let parent_block_hash = {
            let parent_block_height = Height::try_from(height.0.saturating_sub(1))
                .map_err(|e| fail(&format!("invalid parent height: {e}")))?;
            let parent_block_height_key = parent_block_height
                .to_bytes()
                .map_err(|e| fail(&format!("parent height serialize: {e}")))?;
            let raw = ro
                .get(self.headers, &parent_block_height_key)
                .map_err(FinalisedStateError::LmdbError)?;
            let entry = StoredEntryVar::<BlockHeaderData>::from_bytes(raw)
                .map_err(|e| fail(&format!("parent header corrupt data: {e}")))?;

            *entry.inner().index().parent_hash()
        };

        if parent_block_hash != hash {
            return Err(fail("parent hash mismatch"));
        }

        // *** Merkle root / Txid validation ***
        // let txids = txid_list_entry.inner().tx();
        let txids: Vec<[u8; 32]> = txid_list_entry.inner().tx().iter().map(|h| h.0).collect();
        let header_merkle_root = header_entry.inner().data().merkle_root();

        let check_root = Self::calculate_block_merkle_root(&txids);

        if &check_root != header_merkle_root {
            return Err(fail("merkle root mismatch"));
        }

        // *** spent + addrhist validation ***
        let tx_list = transparent_tx_list.inner().tx();

        for (tx_index, tx_opt) in tx_list.iter().enumerate() {
            let tx_index = tx_index as u16;
            let txid_index = TxIndex::new(height.0, tx_index);

            let Some(tx) = tx_opt else { continue };

            // Inputs: check spent + addrhist input record
            for input in tx.inputs() {
                // Check spent record
                let outpoint = Outpoint::new(*input.prevout_txid(), input.prevout_index());
                let outpoint_bytes = outpoint.to_bytes()?;
                let val = ro.get(self.spent, &outpoint_bytes).map_err(|_| {
                    fail(&format!("missing spent index for outpoint {:?}", outpoint))
                })?;
                let entry = StoredEntryFixed::<TxIndex>::from_bytes(val)
                    .map_err(|e| fail(&format!("corrupt spent entry: {e}")))?;
                if !entry.verify(&outpoint_bytes) {
                    return Err(fail("spent entry checksum mismatch"));
                }
                if entry.inner() != &txid_index {
                    return Err(fail("spent entry has wrong TxIndex"));
                }

                // Check addrhist input record
                let prev_output = self.get_previous_output(outpoint)?;
                let addr_bytes = AddrScript::new(*prev_output.script_hash()).to_bytes()?;
                let rec_bytes =
                    self.addr_hist_records_by_addr_and_index_raw_txn(&ro, &addr_bytes, txid_index)?;

                let matched = rec_bytes.iter().any(|val| {
                    // avoid deserialization: check IS_INPUT + correct vout
                    // - [0] StoredEntry tag
                    // - [1] record tag
                    // - [2..5] height
                    // - [6..7] tx_index
                    // - [8..9] vout
                    // - [10] flags
                    // - [11..18] value
                    // - [19..50] checksum

                    let flags = val[10];
                    let vout = u16::from_be_bytes([val[8], val[9]]);
                    (flags & AddrEventBytes::FLAG_IS_INPUT) != 0
                        && vout == input.prevout_index() as u16
                });

                if !matched {
                    return Err(fail("missing addrhist input record"));
                }
            }

            // Outputs: check addrhist mined record
            for (vout, output) in tx.outputs().iter().enumerate() {
                let addr_bytes = AddrScript::new(*output.script_hash()).to_bytes()?;
                let rec_bytes =
                    self.addr_hist_records_by_addr_and_index_raw_txn(&ro, &addr_bytes, txid_index)?;

                let matched = rec_bytes.iter().any(|val| {
                    // avoid deserialization: check IS_MINED + correct vout
                    // - [0] StoredEntry tag
                    // - [1] record tag
                    // - [2..5] height
                    // - [6..7] tx_index
                    // - [8..9] vout
                    // - [10] flags
                    // - [11..18] value
                    // - [19..50] checksum

                    let flags = val[10];
                    let vout_rec = u16::from_be_bytes([val[8], val[9]]);
                    (flags & AddrEventBytes::FLAG_MINED) != 0 && vout_rec as usize == vout
                });

                if !matched {
                    return Err(fail("missing addrhist mined output record"));
                }
            }
        }

        self.mark_validated(height.into());
        Ok(())
    }

    /// Double‑SHA‑256 (SHA256d) as used by Bitcoin/Zcash headers and Merkle nodes.
    fn sha256d(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        Digest::update(&mut hasher, data); // first pass
        let first = hasher.finalize_reset();
        Digest::update(&mut hasher, &first); // second pass
        let second = hasher.finalize();

        let mut out = [0u8; 32];
        out.copy_from_slice(&second);
        out
    }

    /// Compute the Merkle root of a non‑empty slice of 32‑byte transaction IDs.
    /// `txids` must be in block order and already in internal (little‑endian) byte order.
    pub fn calculate_block_merkle_root(txids: &[[u8; 32]]) -> [u8; 32] {
        assert!(
            !txids.is_empty(),
            "block must contain at least the coinbase"
        );
        let mut layer: Vec<[u8; 32]> = txids.to_vec();

        // Iterate until we have reduced to one hash.
        while layer.len() > 1 {
            let mut next = Vec::with_capacity((layer.len() + 1) / 2);

            // Combine pairs (duplicate the last when the count is odd).
            for chunk in layer.chunks(2) {
                let left = &chunk[0];
                let right = if chunk.len() == 2 {
                    &chunk[1]
                } else {
                    &chunk[0]
                };

                // Concatenate left‖right and hash twice.
                let mut buf = [0u8; 64];
                buf[..32].copy_from_slice(left);
                buf[32..].copy_from_slice(right);
                next.push(Self::sha256d(&buf));
            }

            layer = next;
        }

        layer[0]
    }

    /// Validate a contiguous range of block heights `[start, end]` inclusive.
    ///
    /// Optimized to skip blocks already known to be validated.  
    /// Returns the full requested `(start, end)` range on success.
    fn validate_block_range(
        &self,
        start: Height,
        end: Height,
    ) -> Result<(Height, Height), FinalisedStateError> {
        if end.0 < start.0 {
            return Err(FinalisedStateError::Custom(
                "invalid block range: end < start".to_string(),
            ));
        }

        let tip = self.validated_tip.load(Ordering::Acquire);
        let mut h = std::cmp::max(start.0, tip);

        if h > end.0 {
            return Ok((start, end));
        }

        while h <= end.0 {
            if self.is_validated(h) {
                h += 1;
                continue;
            }

            let height = Height(h);
            let height_bytes = height.to_bytes()?;
            let ro = self.env.begin_ro_txn()?;
            let bytes = ro.get(self.headers, &height_bytes).map_err(|e| {
                if e == lmdb::Error::NotFound {
                    FinalisedStateError::Custom("height not found in best chain".into())
                } else {
                    FinalisedStateError::LmdbError(e)
                }
            })?;

            let hash = *StoredEntryVar::<BlockHeaderData>::deserialize(bytes)?
                .inner()
                .index()
                .hash();

            self.validate_block(height, hash)?;
            h += 1;
        }

        Ok((start, end))
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

    /// Resolve a `HashOrHeight` to the block height stored on disk.
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

// *** ZainoDB Reader ***

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

impl<T: ZainoVersionedSerialise + FixedEncodedLen> FixedEncodedLen for StoredEntryFixed<T> {
    const ENCODED_LEN: usize = T::VERSIONED_LEN + 32;
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
