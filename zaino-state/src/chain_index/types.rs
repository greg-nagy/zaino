//! Holds chain and block structs used internally by the ChainIndex.
//!
//! Held here to ensure serialisation consistency for ZainoDB.

use hex::{FromHex, ToHex};
use primitive_types::U256;
use std::fmt;

/// Represents the indexing data of a single compact Zcash block used internally by Zaino.
/// Provides efficient indexing for blockchain state queries and updates.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainBlock {
    /// Metadata and indexing information for this block.
    index: BlockIndex,
    /// Essential header and metadata information for the block.
    data: BlockData,
    /// Compact representations of transactions in this block.
    tx: Vec<TxData>,
    /// Explicitly recorded UTXOs spent in this block, speeding up reorgs and delta indexing.
    spent_outpoints: Vec<SpentOutpoint>,
}

impl ChainBlock {
    /// Creates a new `ChainBlock`.
    pub fn new(
        index: BlockIndex,
        data: BlockData,
        tx: Vec<TxData>,
        spent_outpoints: Vec<SpentOutpoint>,
    ) -> Self {
        Self {
            index,
            data,
            tx,
            spent_outpoints,
        }
    }

    /// Returns a reference to the block index metadata.
    pub fn index(&self) -> &BlockIndex {
        &self.index
    }

    /// Returns a reference to the header and auxiliary block data.
    pub fn data(&self) -> &BlockData {
        &self.data
    }

    /// Returns a reference to the compact transactions in this block.
    pub fn transactions(&self) -> &[TxData] {
        &self.tx
    }

    /// Returns a reference to the spent transparent outpoints.
    pub fn spent_outpoints(&self) -> &[SpentOutpoint] {
        &self.spent_outpoints
    }

    /// Returns the block hash.
    pub fn hash(&self) -> &Hash {
        self.index.hash()
    }

    /// Returns the block height if available.
    pub fn height(&self) -> Option<Height> {
        self.index.height()
    }

    /// Returns true if this block is part of the best chain.
    pub fn is_on_best_chain(&self) -> bool {
        self.index.is_on_best_chain()
    }

    /// Returns the cumulative chainwork.
    pub fn chainwork(&self) -> &ChainWork {
        self.index.chainwork()
    }

    /// Returns the raw work value (targeted work contribution).
    pub fn work(&self) -> U256 {
        self.data.work()
    }
}

/// Metadata about the block used to identify and navigate the blockchain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockIndex {
    /// The hash identifying this block uniquely.
    hash: Hash,
    /// The hash of this block's parent block (previous block in chain).
    parent_hash: Hash,
    /// The cumulative proof-of-work of the blockchain up to this block, used for chain selection.
    chainwork: ChainWork,
    /// The height of this block if it's in the current best chain. None if it's part of a fork.
    height: Option<Height>,
}

impl BlockIndex {
    /// Constructs a new `BlockIndex`.
    pub fn new(
        hash: Hash,
        parent_hash: Hash,
        chainwork: ChainWork,
        height: Option<Height>,
    ) -> Self {
        Self {
            hash,
            parent_hash,
            chainwork,
            height,
        }
    }

    /// Returns the hash of this block.
    pub fn hash(&self) -> &Hash {
        &self.hash
    }

    /// Returns the hash of the parent block.
    pub fn parent_hash(&self) -> &Hash {
        &self.parent_hash
    }

    /// Returns the cumulative chainwork up to this block.
    pub fn chainwork(&self) -> &ChainWork {
        &self.chainwork
    }

    /// Returns the height of this block if it’s part of the best chain.
    pub fn height(&self) -> Option<Height> {
        self.height
    }

    /// Returns true if this block is part of the best chain.
    pub fn is_on_best_chain(&self) -> bool {
        self.height.is_some()
    }
}

/// Block hash (SHA256d hash of the block header).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Return the hash bytes in big-endian byte-order suitable for printing out byte by byte.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut reversed_bytes = self.0;
        reversed_bytes.reverse();
        reversed_bytes
    }

    /// Convert bytes in big-endian byte-order into a [`block::Hash`](crate::block::Hash).
    pub fn from_bytes_in_display_order(bytes_in_display_order: &[u8; 32]) -> Hash {
        let mut internal_byte_order = *bytes_in_display_order;
        internal_byte_order.reverse();

        Hash(internal_byte_order)
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.encode_hex::<String>())
    }
}

impl ToHex for &Hash {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex_upper()
    }
}

impl ToHex for Hash {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex_upper()
    }
}

impl FromHex for Hash {
    type Error = <[u8; 32] as FromHex>::Error;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let hash = <[u8; 32]>::from_hex(hex)?;

        Ok(Self::from_bytes_in_display_order(&hash))
    }
}

impl From<[u8; 32]> for Hash {
    fn from(bytes: [u8; 32]) -> Self {
        Hash(bytes)
    }
}

impl From<Hash> for [u8; 32] {
    fn from(hash: Hash) -> Self {
        hash.0
    }
}

impl From<Hash> for zebra_chain::block::Hash {
    fn from(h: Hash) -> Self {
        zebra_chain::block::Hash(h.0)
    }
}

impl From<zebra_chain::block::Hash> for Hash {
    fn from(h: zebra_chain::block::Hash) -> Self {
        Hash(h.0)
    }
}

impl From<Hash> for zcash_primitives::block::BlockHash {
    fn from(h: Hash) -> Self {
        // Convert to display order (big-endian)
        zcash_primitives::block::BlockHash(h.bytes_in_display_order())
    }
}

impl From<zcash_primitives::block::BlockHash> for Hash {
    fn from(h: zcash_primitives::block::BlockHash) -> Self {
        Hash::from_bytes_in_display_order(&h.0)
    }
}

/// Block height.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Height(u32);

impl TryFrom<u32> for Height {
    type Error = &'static str;

    fn try_from(height: u32) -> Result<Self, Self::Error> {
        // Zebra enforces Height <= 2^31 - 1
        if height <= zebra_chain::block::Height::MAX.0 {
            Ok(Self(height))
        } else {
            Err("height must be ≤ 2^31 - 1")
        }
    }
}

impl From<Height> for u32 {
    fn from(h: Height) -> Self {
        h.0
    }
}

impl std::fmt::Display for Height {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Height {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let h = s.parse::<u32>().map_err(|_| "invalid u32")?;
        Self::try_from(h)
    }
}

impl From<Height> for zebra_chain::block::Height {
    fn from(h: Height) -> Self {
        zebra_chain::block::Height(h.0)
    }
}

impl TryFrom<zebra_chain::block::Height> for Height {
    type Error = &'static str;

    fn try_from(h: zebra_chain::block::Height) -> Result<Self, Self::Error> {
        Height::try_from(h.0)
    }
}

impl From<Height> for zcash_protocol::consensus::BlockHeight {
    fn from(h: Height) -> Self {
        zcash_protocol::consensus::BlockHeight::from(h.0)
    }
}

impl TryFrom<zcash_protocol::consensus::BlockHeight> for Height {
    type Error = &'static str;

    fn try_from(h: zcash_protocol::consensus::BlockHeight) -> Result<Self, Self::Error> {
        Height::try_from(u32::from(h))
    }
}

/// Numerical index of subtree / shard roots.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Index(pub u32);

/// Cumulative proof-of-work of the chain,
/// stored as a **big-endian** 256-bit unsigned integer.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ChainWork([u8; 32]);

impl ChainWork {
    ///Returns ChainWork as a U256.
    pub fn to_u256(&self) -> U256 {
        U256::from_big_endian(&self.0)
    }

    /// Builds a ChainWork from a U256.
    pub fn from_u256(value: U256) -> Self {
        let buf: [u8; 32] = value.to_big_endian();
        ChainWork(buf)
    }

    /// Adds 2 ChainWorks.
    pub fn add(&self, other: &Self) -> Self {
        Self::from_u256(self.to_u256() + other.to_u256())
    }

    /// Subtract one ChainWork from another.
    pub fn sub(&self, other: &Self) -> Self {
        Self::from_u256(self.to_u256() - other.to_u256())
    }

    /// Returns ChainWork bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<U256> for ChainWork {
    fn from(value: U256) -> Self {
        Self::from_u256(value)
    }
}

impl From<ChainWork> for U256 {
    fn from(value: ChainWork) -> Self {
        value.to_u256()
    }
}

impl fmt::Display for ChainWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_u256().fmt(f)
    }
}

/// Essential block header fields required for quick indexing and client-serving.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockData {
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

impl BlockData {
    /// Creates a new  BlockData instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u32,
        time: i64,
        size: u64,
        merkle_root: [u8; 32],
        commitment_digest: [u8; 32],
        commitment_tree_roots: CommitmentTreeRoots,
        commitment_tree_size: CommitmentTreeSizes,
        bits: u32,
        orchard_anchor: [u8; 32],
    ) -> Self {
        Self {
            version,
            time,
            size,
            merkle_root,
            commitment_digest,
            commitment_tree_roots,
            commitment_tree_size,
            bits,
            orchard_anchor,
        }
    }

    /// Returns block Version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns block time.
    pub fn time(&self) -> i64 {
        self.time
    }

    /// Returns block size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns block merkle root.
    pub fn merkle_root(&self) -> &[u8; 32] {
        &self.merkle_root
    }

    /// Returns block commitment digest.
    pub fn commitment_digest(&self) -> &[u8; 32] {
        &self.commitment_digest
    }

    /// Returns commitment tree roots.
    pub fn commitment_tree_roots(&self) -> &CommitmentTreeRoots {
        &self.commitment_tree_roots
    }

    /// Returns commitment tree sizes.
    pub fn commitment_tree_size(&self) -> &CommitmentTreeSizes {
        &self.commitment_tree_size
    }

    /// Returns nbits.
    pub fn bits(&self) -> u32 {
        self.bits
    }

    /// Returns orchard anchor.
    pub fn orchard_anchor(&self) -> &[u8; 32] {
        &self.orchard_anchor
    }

    /// Converts compact bits field into the full target as a 256-bit integer.
    pub fn target(&self) -> U256 {
        Self::compact_to_target_u256(self.bits)
    }

    /// Returns the block work as 2^256 / (target + 1)
    pub fn work(&self) -> U256 {
        let target = self.target();
        if target.is_zero() {
            U256::zero()
        } else {
            (U256::one() << 256) / (target + 1)
        }
    }

    /// Returns difficulty as ratio of the genesis target to this block's target.
    pub fn difficulty(&self) -> f64 {
        let max_target = Self::compact_to_target_u256(0x1d00ffff); // Zcash genesis
        let target = self.target();
        Self::u256_to_f64(max_target) / Self::u256_to_f64(target)
    }

    /// Used to convert bits to target.
    fn compact_to_target_u256(bits: u32) -> U256 {
        let exponent = (bits >> 24) as usize;
        let mantissa = bits & 0x007fffff;

        if exponent <= 3 {
            U256::from(mantissa) >> (8 * (3 - exponent))
        } else {
            U256::from(mantissa) << (8 * (exponent - 3))
        }
    }

    /// Converts a `U256` to `f64` lossily (sufficient for difficulty comparison).
    fn u256_to_f64(value: U256) -> f64 {
        let mut result = 0.0f64;
        for (i, word) in value.0.iter().enumerate() {
            result += (*word as f64) * 2f64.powi(64 * i as i32);
        }
        result
    }
}

/// Commitment tree roots for shielded transactions, enabling shielded wallet synchronization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitmentTreeRoots {
    /// Sapling note-commitment tree root (anchor) at this block.
    sapling: [u8; 32],
    /// Orchard note-commitment tree root at this block.
    orchard: [u8; 32],
}

impl CommitmentTreeRoots {
    /// Reutns a new CommitmentTreeRoots instance.
    pub fn new(sapling: [u8; 32], orchard: [u8; 32]) -> Self {
        Self { sapling, orchard }
    }

    /// Returns sapling commitment tree root.
    pub fn sapling(&self) -> &[u8; 32] {
        &self.sapling
    }

    /// returns orchard commitment tree root.
    pub fn orchard(&self) -> &[u8; 32] {
        &self.orchard
    }
}

/// Sizes of commitment trees, indicating total number of shielded notes created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitmentTreeSizes {
    /// Total notes in Sapling commitment tree.
    sapling: u32,
    /// Total notes in Orchard commitment tree.
    orchard: u32,
}

impl CommitmentTreeSizes {
    /// Creates a new CompactSaplingSizes instance.
    pub fn new(sapling: u32, orchard: u32) -> Self {
        Self { sapling, orchard }
    }

    /// Returns sapling commitment tree size
    pub fn sapling(&self) -> u32 {
        self.sapling
    }

    /// Returns orchard commitment tree size
    pub fn orchard(&self) -> u32 {
        self.orchard
    }
}

/// Compact indexed representation of a transaction within a block, supporting quick queries.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TxData {
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

impl TxData {
    /// Creates a new TxData instance.
    pub fn new(
        index: u64,
        txid: [u8; 32],
        fee: u32,
        transparent: TransparentCompactTx,
        sapling: SaplingCompactTx,
        orchard: Vec<CompactOrchardAction>,
    ) -> Self {
        Self {
            index,
            txid,
            fee,
            transparent,
            sapling,
            orchard,
        }
    }

    /// Returns transactions index within block.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Returns transaction ID.
    pub fn txid(&self) -> &[u8; 32] {
        &self.txid
    }

    /// Returns transaction fee.
    pub fn fee(&self) -> u32 {
        self.fee
    }

    /// Returns compact transparent tx data.
    pub fn transparent(&self) -> &TransparentCompactTx {
        &self.transparent
    }

    /// Returns compact sapling tx data.
    pub fn sapling(&self) -> &SaplingCompactTx {
        &self.sapling
    }

    /// Returns compact orchard tx data.
    pub fn orchard(&self) -> &[CompactOrchardAction] {
        &self.orchard
    }
}

/// Compact transaction inputs and outputs for transparent (unshielded) transactions.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransparentCompactTx {
    /// Transaction inputs (spent outputs from previous transactions).
    vin: Vec<TxInCompact>,
    /// Transaction outputs (newly created UTXOs).
    vout: Vec<TxOutCompact>,
}

impl TransparentCompactTx {
    /// Creates a new TransparentCompactTx instance.
    pub fn new(vin: Vec<TxInCompact>, vout: Vec<TxOutCompact>) -> Self {
        Self { vin, vout }
    }

    /// Returns transparent inputs.
    pub fn inputs(&self) -> &[TxInCompact] {
        &self.vin
    }

    /// Returns transparent outputs.
    pub fn outputs(&self) -> &[TxOutCompact] {
        &self.vout
    }
}

/// A compact reference to a previously created transparent UTXO being spent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TxInCompact {
    /// Transaction ID of the output being spent.
    prevout_txid: [u8; 32],
    /// Index (position) of the output in the previous transaction being spent.
    prevout_index: u32,
}

impl TxInCompact {
    /// Creates a new TxInCompact instance.
    pub fn new(prevout_txid: [u8; 32], prevout_index: u32) -> Self {
        Self {
            prevout_txid,
            prevout_index,
        }
    }

    /// Returns txid of the transaction that holds the output being sent.
    pub fn prevout_txid(&self) -> &[u8; 32] {
        &self.prevout_txid
    }

    /// Returns the index of the output being sent within the transaction.
    pub fn prevout_index(&self) -> u32 {
        self.prevout_index
    }
}

/// Identifies the type of transparent transaction output script.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScriptType {
    /// Standard pay-to-public-key-hash (P2PKH) address (`t1...`).
    P2PKH = 0x00,
    /// Standard pay-to-script-hash (P2SH) address (`t3...`).
    P2SH = 0x01,
    /// Non-standard output script (rare).
    NonStandard = 0xFF,
}

impl TryFrom<u8> for ScriptType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(ScriptType::P2PKH),
            0x01 => Ok(ScriptType::P2SH),
            0xFF => Ok(ScriptType::NonStandard),
            _ => Err(()),
        }
    }
}

impl ScriptType {
    /// Returns ScriptType as a String.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScriptType::P2PKH => "P2PKH",
            ScriptType::P2SH => "P2SH",
            ScriptType::NonStandard => "NonStandard",
        }
    }
}

/// Compact representation of a transparent output, optimized for indexing and efficient querying.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TxOutCompact {
    /// Amount of ZEC sent to this output (in zatoshis).
    value: u64,
    /// 20-byte hash representation of the script or address this output pays to.
    script_hash: [u8; 20],
    /// Type indicator for the output's script/address type, enabling efficient address reconstruction.
    script_type: u8,
}

impl TxOutCompact {
    /// Creates a new TxOutCompact instance.
    pub fn new(value: u64, script_hash: [u8; 20], script_type: u8) -> Option<Self> {
        if ScriptType::try_from(script_type).is_ok() {
            Some(Self {
                value,
                script_hash,
                script_type,
            })
        } else {
            None
        }
    }

    /// Returns the valuse in zatoshi sent in this output.
    pub fn value(&self) -> u64 {
        self.value
    }

    /// Returns script hash.
    pub fn script_hash(&self) -> &[u8; 20] {
        &self.script_hash
    }

    /// Returns script type u8.
    pub fn script_type(&self) -> u8 {
        self.script_type
    }

    /// Returns script type Enum.
    pub fn script_type_enum(&self) -> Option<ScriptType> {
        ScriptType::try_from(self.script_type).ok()
    }
}

/// Compact representation of Sapling shielded transaction data for wallet scanning.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SaplingCompactTx {
    /// Shielded spends (notes being consumed).
    spend: Vec<CompactSaplingSpend>,
    /// Shielded outputs (new notes created).
    output: Vec<CompactSaplingOutput>,
    /// Anchor (Sapling note commitment tree root) referenced by this transaction.
    anchor: [u8; 32],
}

impl SaplingCompactTx {
    /// Creates a new SaplingCompactTx instance.
    pub fn new(
        spend: Vec<CompactSaplingSpend>,
        output: Vec<CompactSaplingOutput>,
        anchor: [u8; 32],
    ) -> Self {
        Self {
            spend,
            output,
            anchor,
        }
    }

    /// Returns sapling spends.
    pub fn spends(&self) -> &[CompactSaplingSpend] {
        &self.spend
    }

    /// Returns sapling outputs
    pub fn outputs(&self) -> &[CompactSaplingOutput] {
        &self.output
    }

    /// Returns sapling anchor.
    pub fn anchor(&self) -> &[u8; 32] {
        &self.anchor
    }
}

/// Compact representation of a Sapling shielded spend (consuming a previous shielded note).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactSaplingSpend {
    /// Nullifier of the Sapling note being spent, prevents double spends.
    nf: [u8; 32],
}

impl CompactSaplingSpend {
    /// Creates a new CompactSaplingSpend instance.
    pub fn new(nf: [u8; 32]) -> Self {
        Self { nf }
    }

    /// Returns sapling nullifier.
    pub fn nullifier(&self) -> &[u8; 32] {
        &self.nf
    }
}

/// Compact representation of a newly created Sapling shielded note output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactSaplingOutput {
    /// Commitment of the newly created shielded note.
    cmu: [u8; 32],
    /// Ephemeral public key used by receivers to detect/decrypt the note.
    ephemeral_key: [u8; 32],
    /// Encrypted note ciphertext (minimal required portion).
    #[serde(with = "serde_arrays::fixed_52")]
    ciphertext: [u8; 52],
}

impl CompactSaplingOutput {
    /// Creates a new CompactSaplingOutput instance.
    pub fn new(cmu: [u8; 32], ephemeral_key: [u8; 32], ciphertext: [u8; 52]) -> Self {
        Self {
            cmu,
            ephemeral_key,
            ciphertext,
        }
    }

    /// Returns cmu.
    pub fn cmu(&self) -> &[u8; 32] {
        &self.cmu
    }

    /// Returns ephemeral key.
    pub fn ephemeral_key(&self) -> &[u8; 32] {
        &self.ephemeral_key
    }

    /// Returns ciphertext.
    pub fn ciphertext(&self) -> &[u8; 52] {
        &self.ciphertext
    }
}

/// Compact representation of Orchard shielded action (note spend or output).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactOrchardAction {
    /// Nullifier preventing double spends of the Orchard note.
    nullifier: [u8; 32],
    /// Commitment of the new Orchard note created.
    cmx: [u8; 32],
    /// Ephemeral public key for detecting and decrypting Orchard notes.
    ephemeral_key: [u8; 32],
    /// Encrypted ciphertext of the Orchard note (minimal required portion).
    #[serde(with = "serde_arrays::fixed_52")]
    ciphertext: [u8; 52],
}

impl CompactOrchardAction {
    /// Creates a new CompactOrchardAction instance.
    pub fn new(
        nullifier: [u8; 32],
        cmx: [u8; 32],
        ephemeral_key: [u8; 32],
        ciphertext: [u8; 52],
    ) -> Self {
        Self {
            nullifier,
            cmx,
            ephemeral_key,
            ciphertext,
        }
    }

    /// Returns orchard nullifier.
    pub fn nullifier(&self) -> &[u8; 32] {
        &self.nullifier
    }

    /// Returns cmx.
    pub fn cmx(&self) -> &[u8; 32] {
        &self.cmx
    }

    /// Returns ephemeral key.
    pub fn ephemeral_key(&self) -> &[u8; 32] {
        &self.ephemeral_key
    }

    /// Returns ciphertext.
    pub fn ciphertext(&self) -> &[u8; 52] {
        &self.ciphertext
    }
}

/// Represents explicitly recorded spent transparent outputs within a block, facilitating fast indexing and rollbacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpentOutpoint {
    /// Transaction ID of the output that was spent.
    txid: [u8; 32],
    /// The index of the output within the previous transaction being spent.
    vout: u32,
}

impl SpentOutpoint {
    /// Creates a new SpentOutpoint instance.
    pub fn new(txid: [u8; 32], vout: u32) -> Self {
        Self { txid, vout }
    }

    /// Returns the Txid of the transaction the outpoint was spent in.
    pub fn txid(&self) -> &[u8; 32] {
        &self.txid
    }

    /// Index of the output within the transaction.
    pub fn vout(&self) -> u32 {
        self.vout
    }
}

/// Root commitment for a state shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardRoot {
    /// Shard commitment tree root (256-bit digest)
    hash: [u8; 32],
    /// Hash of the final block in this shard
    final_block_hash: [u8; 32],
    /// Height of the final block in this shard
    final_block_height: u32,
}

impl ShardRoot {
    /// Creates a new ShardRoot instance.
    pub fn new(hash: [u8; 32], final_block_hash: [u8; 32], final_block_height: u32) -> Self {
        Self {
            hash,
            final_block_hash,
            final_block_height,
        }
    }

    /// Returns commitment tree root.
    pub fn hash(&self) -> &[u8; 32] {
        &self.hash
    }

    /// Returns the hash of the final block in this shard.
    pub fn final_block_hash(&self) -> &[u8; 32] {
        &self.final_block_hash
    }

    /// Returns the Height of the final block in this shard.
    pub fn final_block_height(&self) -> u32 {
        self.final_block_height
    }
}

pub mod serde_arrays {
    use serde::{Deserialize, Deserializer, Serializer};

    pub mod fixed_52 {
        use super::*;
        pub fn serialize<S>(val: &[u8; 52], s: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            s.serialize_bytes(val)
        }

        pub fn deserialize<'de, D>(d: D) -> Result<[u8; 52], D::Error>
        where
            D: Deserializer<'de>,
        {
            let v: &[u8] = Deserialize::deserialize(d)?;
            v.try_into()
                .map_err(|_| serde::de::Error::custom("invalid length for [u8; 52]"))
        }
    }
}
