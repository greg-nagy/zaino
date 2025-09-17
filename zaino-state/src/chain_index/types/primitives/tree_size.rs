//! Validated commitment tree size types.
//!
//! This module provides type-safe, validated representations of commitment tree sizes
//! for each of Zcash's shielded pools. Tree sizes are bounded by their Merkle tree depth:
//! - Sprout: 2^29 max leaves (536,870,912)
//! - Sapling: 2^32 max leaves (4,294,967,296) 
//! - Orchard: 2^32 max leaves (4,294,967,296)

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