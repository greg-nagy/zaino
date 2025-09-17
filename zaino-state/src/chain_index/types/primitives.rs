//! Foundational primitive types for the chain index.
//!
//! This module contains the basic building blocks that other types depend on,
//! including validated tree sizes, hashes, heights, and chain work values.

pub mod tree_size;

// Re-export tree size types
pub use tree_size::{
    TreeSize, TreeSizeError, 
    SproutTreeSize, SaplingTreeSize, OrchardTreeSize
};