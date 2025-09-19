//! Foundational primitive types for the chain index.
//!
//! This module contains the basic building blocks that other types depend on.
//! Currently contains validated tree sizes, with plans to migrate other primitives
//! from the main types.rs file.
//!
//! ## Current Types
//! - Tree size validation and type safety
//!
//! ## Planned Migrations
//! The following types should be extracted from types.rs:
//! - Block and transaction hashes
//! - Block heights and chain work
//! - Basic indexing primitives

pub mod tree_size;

// Re-export tree size types
pub use tree_size::{OrchardTreeSize, SaplingTreeSize, SproutTreeSize, TreeSize, TreeSizeError};
