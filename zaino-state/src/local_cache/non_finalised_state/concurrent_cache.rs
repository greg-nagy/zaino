use std::{collections::HashMap, mem, sync::Arc};

use tokio::sync::{mpsc, RwLock};
use zaino_fetch::jsonrpsee::response::ChainWork;
use zebra_chain::{block::Height, transaction::Hash};

/// Holds the block cache
struct ConcurrentBlockCache {
    staged: RwLock<mpsc::UnboundedReceiver<ChainBlock>>,
    staging_sender: mpsc::UnboundedSender<ChainBlock>,
    /// This lock should not be exposed to consumers. Rather,
    /// clone the Arc and offer that. This means we can overwrite the arc
    /// without interfering with readers, who will hold a stale copy
    current: RwLock<Arc<BlockCacheSnapshot>>,
}

pub(crate) struct BlockCacheSnapshot {
    blocks: HashMap<Hash, ChainBlock>,
    // Do we need height here?
    best_tip: (Hash, Height),
}

/// This is the core of the concurrent block cache.
impl ConcurrentBlockCache {
    pub async fn update(&self, finalized_height: Height) -> Result<(), ()> {
        let mut new = HashMap::new();
        let mut staged = self.staged.write().await;
        loop {
            match staged.try_recv() {
                Ok(chain_block) => {
                    new.insert(chain_block.index.hash, chain_block);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(()),
            }
        }
        // at this point, we've collected everything in the staging area
        // we can drop the stage lock, and more blocks can be staged while we finish setting current
        mem::drop(staged);
        let snapshot = self.get_snapshot().await;
        new.extend(
            snapshot
                .blocks
                .iter()
                .map(|(hash, block)| (hash.clone(), block.clone())),
        );
        let (newly_finalzed, blocks): (HashMap<_, _>, HashMap<_, _>) =
            new.into_iter()
                .partition(|(_hash, block)| match block.index.height {
                    Some(height) => height <= finalized_height,
                    None => false,
                });
        let best_tip = blocks.iter().fold(snapshot.best_tip, |acc, (hash, block)| {
            match block.index.height {
                Some(height) if height > acc.1 => (hash.clone(), height),
                _ => acc,
            }
        });
        // Need to get best hash at some point in this process
        *self.current.write().await = Arc::new(BlockCacheSnapshot { blocks, best_tip });

        Ok(())
    }

    pub async fn get_snapshot(&self) -> Arc<BlockCacheSnapshot> {
        self.current.read().await.clone()
    }
}

/// Compact block data.
#[derive(Clone)]
struct ChainBlock {
    /// Chain index data.
    index: BlockIndex,
    /// Block header and metadata.
    data: BlockData,
    /// Compact transactions.
    tx: Vec<TxData>,
}
/// Block chain-index data.
#[derive(Clone)]
struct BlockIndex {
    /// Block hash
    hash: Hash,
    /// Prev / parent block hash.
    parent_hash: Hash,
    /// Cumulative proof-of-work of the chain up to this block.
    chainwork: ChainWork,
    /// Block height (if in best chain, else None).
    height: Option<Height>,
}

/// Essential block header fields required for quick indexing and client-serving.
#[derive(Clone)]
struct BlockData {
    /// Version number of the block format (protocol upgrades).
    version: u32,
    /// Unix timestamp of when the block was mined (seconds since epoch).
    time: i64,
    /// The size of the full block in bytes (header + transactions).
    size: u64,
    /// Merkle root hash of all transaction IDs in the block (used for quick tx inclusion proofs).
    merkle_root: [u8; 32],
    /// Digest representing the block-commitments Merkle root (commitment to note states).
    commitment_digest: [u8; 32],
    /// Roots of the Sapling and Orchard note-commitment trees (for shielded tx verification).
    commitment_tree_roots: CommitmentTreeRoots,
    /// Number of leaves in Sapling/Orchard note commitment trees at this block.
    commitment_tree_size: CommitmentTreeSizes,
    /// Compact difficulty target used for proof-of-work and difficulty calculation.
    bits: u32,
    /// Orchard anchor (Orchard note commitment tree root) at this block,
    /// required to build Orchard CompactBlocks and verify Orchard shielded transactions.
    orchard_anchor: [u8; 32],
}

/// Commitment tree roots for shielded transactions, enabling shielded wallet synchronization.
#[derive(Clone)]
struct CommitmentTreeRoots {
    /// Sapling note-commitment tree root (anchor) at this block.
    sapling: [u8; 32],
    /// Orchard note-commitment tree root at this block.
    orchard: [u8; 32],
}

/// Sizes of commitment trees, indicating total number of shielded notes created.
#[derive(Clone)]
struct CommitmentTreeSizes {
    /// Total notes in Sapling commitment tree.
    sapling: u32,
    /// Total notes in Orchard commitment tree.
    orchard: u32,
}

/// Compact indexed representation of a transaction within a block, supporting quick queries.
#[derive(Clone)]
struct TxData {
    /// The index (position) of this transaction within its block (0-based).
    index: u64,
    /// Unique identifier (hash) of the transaction, used for lookup and indexing.
    txid: [u8; 32],
    /// The fee (in zatoshis) paid by this transaction.
    fee: u32,
    /// Compact representation of transparent inputs/outputs in the transaction.
    transparent: TransparentCompactTx,
    /// Compact representation of Sapling shielded data.
    sapling: SaplingCompactTx,
    /// Compact representation of Orchard actions (shielded pool transactions).
    orchard: Vec<CompactOrchardAction>,
}

/// Compact transaction inputs and outputs for transparent (unshielded) transactions.
#[derive(Clone)]
struct TransparentCompactTx {
    /// Transaction inputs (spent outputs from previous transactions).
    vin: Vec<TxInCompact>,
    /// Transaction outputs (newly created UTXOs).
    vout: Vec<TxOutCompact>,
}

/// A compact reference to a previously created transparent UTXO being spent.
#[derive(Clone)]
struct TxInCompact {
    /// Transaction ID of the output being spent.
    prevout_txid: [u8; 32],
    /// Index (position) of the output in the previous transaction being spent.
    prevout_index: u32,
}

/// Identifies the type of transparent transaction output script.
enum ScriptType {
    /// Standard pay-to-public-key-hash (P2PKH) address (`t1...`).
    P2PKH = 0x00,
    /// Standard pay-to-script-hash (P2SH) address (`t3...`).
    P2SH = 0x01,
    /// Non-standard output script (rare).
    NonStandard = 0xFF,
}

/// Compact representation of a transparent output, optimized for indexing and efficient querying.
#[derive(Clone)]
struct TxOutCompact {
    /// Amount of ZEC sent to this output (in zatoshis).
    value: u64,
    /// 20-byte hash representation of the script or address this output pays to.
    script_hash: [u8; 20],
    /// Type indicator for the output's script/address type, enabling efficient address reconstruction.
    script_type: u8,
}

/// Compact representation of Sapling shielded transaction data for wallet scanning.
#[derive(Clone)]
struct SaplingCompactTx {
    /// Shielded spends (notes being consumed).
    spend: Vec<CompactSaplingSpend>,
    /// Shielded outputs (new notes created).
    output: Vec<CompactSaplingOutput>,
    /// Anchor (Sapling note commitment tree root) referenced by this transaction.
    anchor: [u8; 32],
}

/// Compact representation of a Sapling shielded spend (consuming a previous shielded note).
#[derive(Clone)]
struct CompactSaplingSpend {
    /// Nullifier of the Sapling note being spent, prevents double spends.
    nf: [u8; 32],
}

/// Compact representation of a newly created Sapling shielded note output.
#[derive(Clone)]
struct CompactSaplingOutput {
    /// Commitment of the newly created shielded note.
    cmu: [u8; 32],
    /// Ephemeral public key used by receivers to detect/decrypt the note.
    ephemeral_key: [u8; 32],
    /// Encrypted note ciphertext (minimal required portion).
    ciphertext: [u8; 35],
}

/// Compact representation of Orchard shielded action (note spend or output).
#[derive(Clone)]
struct CompactOrchardAction {
    /// Nullifier preventing double spends of the Orchard note.
    nullifier: [u8; 32],
    /// Commitment of the new Orchard note created.
    cmx: [u8; 32],
    /// Ephemeral public key for detecting and decrypting Orchard notes.
    ephemeral_key: [u8; 32],
    /// Encrypted ciphertext of the Orchard note (minimal required portion).
    ciphertext: [u8; 52],
}

/// Represents explicitly recorded spent transparent outputs within a block, facilitating fast indexing and rollbacks.
struct SpentOutpoint {
    /// Transaction ID of the output that was spent.
    txid: [u8; 32],
    /// The index of the output within the previous transaction being spent.
    vout: u32,
}
