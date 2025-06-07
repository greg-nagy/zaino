//! A mempool-fetching, chain-fetching and transaction submission service that uses zcashd's JsonRPC interface.
//!
//! Used as a reference, legacy option, and for backwards compatibility.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod chain;
pub mod jsonrpsee;
