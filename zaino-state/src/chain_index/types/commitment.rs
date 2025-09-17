//! Commitment tree types and utilities.
//!
//! This module contains types for managing Zcash commitment tree state, including:
//! - Tree sizes with validation
//! - Merkle tree roots for Sapling and Orchard pools  
//! - Combined tree metadata structures
//!
//! Commitment trees track the existence of shielded notes in the Sapling and Orchard
//! shielded pools, enabling efficient zero-knowledge proofs and wallet synchronization.

use core2::io::{self, Read, Write};

use crate::chain_index::encoding::{
    read_fixed_le, version, write_fixed_le, ZainoVersionedSerialise,
};

use super::super::encoding::{
    read_u32_le, write_u32_le, FixedEncodedLen,
};

/// Common behavior for validated commitment tree sizes.
pub trait TreeSize: Copy + Clone + PartialEq + Eq + std::fmt::Debug {
    /// Maximum number of notes this tree type can hold.
    const MAX: u64;
    
    /// The name of the shielded pool this tree belongs to.
    const POOL_NAME: &'static str;
    
    /// Creates a new instance with the given size (must be implemented by each type).
    fn from_raw(size: u64) -> Self;
    
    /// Extracts the raw size value (must be implemented by each type).
    fn value(self) -> u64;
    
    /// Creates a new validated tree size with bounds checking.
    fn new(size: u64) -> Result<Self, TreeSizeError> {
        if size <= Self::MAX {
            Ok(Self::from_raw(size))
        } else {
            Err(TreeSizeError::ExceedsMaximum { 
                size, 
                max: Self::MAX,
                pool: Self::POOL_NAME
            })
        }
    }
    
    /// Returns the raw u64 value (guaranteed to be within valid bounds).
    fn as_u64(self) -> u64 {
        self.value()
    }
    
    /// Creates from u32 (always safe since u32 ⊆ valid range for all pools).
    fn from_u32(size: u32) -> Self {
        Self::from_raw(size as u64)
    }
    
    /// Attempts conversion to u32, handling potential overflow.
    fn as_u32(self) -> Result<u32, TreeSizeError> {
        u32::try_from(self.value()).map_err(|_| TreeSizeError::EdgeCase {
            size: self.value(),
            pool: Self::POOL_NAME,
            explanation: "Tree size exceeds u32::MAX"
        })
    }
    
    /// Checks if this tree size is at its theoretical maximum.
    fn is_at_max(self) -> bool {
        self.value() == Self::MAX
    }
    
    /// Returns remaining capacity before hitting the maximum.
    fn remaining_capacity(self) -> u64 {
        Self::MAX.saturating_sub(self.value())
    }
}

/// Validated Sprout commitment tree size (max 2^29 = 536,870,912 notes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct SproutTreeSize(u64);

impl TreeSize for SproutTreeSize {
    const MAX: u64 = 1u64 << 29;
    const POOL_NAME: &'static str = "Sprout";
    
    fn from_raw(size: u64) -> Self {
        SproutTreeSize(size)
    }
    
    fn value(self) -> u64 {
        self.0
    }
}

impl SproutTreeSize {
    /// Converts to u32 (always safe for Sprout since 2^29 < u32::MAX).
    pub fn as_u32_unchecked(self) -> u32 {
        self.0 as u32
    }
}

/// Validated Sapling commitment tree size (max 2^32 = 4,294,967,296 notes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct SaplingTreeSize(u64);

impl TreeSize for SaplingTreeSize {
    const MAX: u64 = 1u64 << 32;
    const POOL_NAME: &'static str = "Sapling";
    
    fn from_raw(size: u64) -> Self {
        SaplingTreeSize(size)
    }
    
    fn value(self) -> u64 {
        self.0
    }
}

/// Validated Orchard commitment tree size (max 2^32 = 4,294,967,296 notes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct OrchardTreeSize(u64);

impl TreeSize for OrchardTreeSize {
    const MAX: u64 = 1u64 << 32;
    const POOL_NAME: &'static str = "Orchard";
    
    fn from_raw(size: u64) -> Self {
        OrchardTreeSize(size)
    }
    
    fn value(self) -> u64 {
        self.0
    }
}

/// Errors that can occur when working with tree sizes.
#[derive(Debug, thiserror::Error)]
pub enum TreeSizeError {
    /// Tree size exceeds the theoretical maximum for the pool.
    #[error("Tree size {size} exceeds maximum allowed {max} for {pool} pool")]
    ExceedsMaximum { 
        /// The size that was too large.
        size: u64, 
        /// The maximum allowed size.
        max: u64, 
        /// The name of the pool.
        pool: &'static str 
    },
    
    /// Tree size hits an edge case during conversion.
    #[error("Tree size edge case: {explanation} for {pool} pool")]
    EdgeCase { 
        /// The problematic size.
        size: u64, 
        /// The name of the pool.
        pool: &'static str, 
        /// Description of the edge case.
        explanation: &'static str 
    },
}

/// Holds commitment tree metadata (roots and sizes) for a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct CommitmentTreeData {
    roots: CommitmentTreeRoots,
    sizes: CommitmentTreeSizes,
}

impl CommitmentTreeData {
    /// Returns a new CommitmentTreeData instance.
    pub fn new(roots: CommitmentTreeRoots, sizes: CommitmentTreeSizes) -> Self {
        Self { roots, sizes }
    }

    /// Returns the commitment tree roots for the block.
    pub fn roots(&self) -> &CommitmentTreeRoots {
        &self.roots
    }

    /// Returns the commitment tree sizes for the block.
    pub fn sizes(&self) -> &CommitmentTreeSizes {
        &self.sizes
    }
}

impl ZainoVersionedSerialise for CommitmentTreeData {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut w = w;
        self.roots.serialize(&mut w)?; // carries its own tag
        self.sizes.serialize(&mut w)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_v1(r)
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut r = r;
        let roots = CommitmentTreeRoots::deserialize(&mut r)?;
        let sizes = CommitmentTreeSizes::deserialize(&mut r)?;
        Ok(CommitmentTreeData::new(roots, sizes))
    }
}

/// CommitmentTreeData: 74 bytes total
impl FixedEncodedLen for CommitmentTreeData {
    // 1 byte tag + 64 body for roots
    // + 1 byte tag +  8 body for sizes
    const ENCODED_LEN: usize =
        (CommitmentTreeRoots::ENCODED_LEN + 1) + (CommitmentTreeSizes::ENCODED_LEN + 1);
}

/// Commitment tree roots for shielded transactions, enabling shielded wallet synchronization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
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

impl ZainoVersionedSerialise for CommitmentTreeRoots {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut w = w;
        write_fixed_le::<32, _>(&mut w, &self.sapling)?;
        write_fixed_le::<32, _>(&mut w, &self.orchard)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_v1(r)
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut r = r;
        let sapling = read_fixed_le::<32, _>(&mut r)?;
        let orchard = read_fixed_le::<32, _>(&mut r)?;
        Ok(CommitmentTreeRoots::new(sapling, orchard))
    }
}

/// CommitmentTreeRoots: 64 bytes total
impl FixedEncodedLen for CommitmentTreeRoots {
    /// 32 byte hash + 32 byte hash.
    const ENCODED_LEN: usize = 32 + 32;
}

/// Sizes of commitment trees, indicating total number of shielded notes created.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
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

impl ZainoVersionedSerialise for CommitmentTreeSizes {
    const VERSION: u8 = version::V1;

    fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let mut w = w;
        write_u32_le(&mut w, self.sapling)?;
        write_u32_le(&mut w, self.orchard)
    }

    fn decode_latest<R: Read>(r: &mut R) -> io::Result<Self> {
        Self::decode_v1(r)
    }

    fn decode_v1<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut r = r;
        let sapling = read_u32_le(&mut r)?;
        let orchard = read_u32_le(&mut r)?;
        Ok(CommitmentTreeSizes::new(sapling, orchard))
    }
}

/// CommitmentTreeSizes: 8 bytes total
impl FixedEncodedLen for CommitmentTreeSizes {
    /// 4 byte LE int32 + 4 byte LE int32
    const ENCODED_LEN: usize = 4 + 4;
}