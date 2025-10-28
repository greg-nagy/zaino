//! Types associated with the `getaddressdeltas` RPC request.

use std::convert::Infallible;

use serde::{Deserialize, Serialize};
use zebra_rpc::client::{Input, Output, TransactionObject};

use crate::jsonrpsee::connector::ResponseToError;

/// Request parameters for the `getaddressdeltas` RPC method.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum GetAddressDeltasParams {
    /// Extends the basic address/height range with chaininfo and multiple address support.
    #[serde(rename_all = "camelCase")]
    Filtered {
        /// List of base58check encoded addresses
        addresses: Vec<String>,

        /// Start block height (inclusive)
        #[serde(default)]
        start: u32,

        /// End block height (inclusive)
        #[serde(default)]
        end: u32,

        /// Whether to include chain info in response (defaults to false)
        #[serde(default)]
        chain_info: bool,

        /// Zero-based position of the transaction within its containing block.
        #[serde(rename = "blockindex", skip_serializing_if = "Option::is_none")]
        blockindex: Option<u32>,
    },

    /// Get deltas for a single transparent address
    Address(String),
}

impl GetAddressDeltasParams {
    /// Creates a new [`GetAddressDeltasParams::Filtered`] instance.
    pub fn new_filtered(addresses: Vec<String>, start: u32, end: u32, chain_info: bool) -> Self {
        GetAddressDeltasParams::Filtered {
            addresses,
            start,
            end,
            chain_info,
            blockindex: None,
        }
    }

    /// Creates a new [`GetAddressDeltasParams::Address`] instance.
    pub fn new_address(addr: impl Into<String>) -> Self {
        GetAddressDeltasParams::Address(addr.into())
    }
}

/// Response to a `getaddressdeltas` RPC request.
///
/// This enum supports both simple array responses and extended responses with chain info.
/// The format depends on the `chaininfo` parameter in the request.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum GetAddressDeltasResponse {
    /// Simple array format (chaininfo = false or not specified)
    /// Returns: [AddressDelta, AddressDelta, ...]
    Simple(Vec<AddressDelta>),
    /// Extended format with chain info (chaininfo = true)
    /// Returns: {"deltas": [...], "start": {...}, "end": {...}}
    WithChainInfo {
        /// The address deltas
        deltas: Vec<AddressDelta>,
        /// Information about the start block
        start: BlockInfo,
        /// Information about the end block
        end: BlockInfo,
    },
}

impl GetAddressDeltasResponse {
    /// Processes transaction objects into address deltas for specific addresses.
    /// This is a pure function that can be easily unit tested.
    pub fn process_transactions_to_deltas(
        transactions: &[Box<TransactionObject>],
        target_addresses: &[String],
    ) -> Vec<AddressDelta> {
        transactions
            .iter()
            .flat_map(|tx| {
                let txid = tx.txid().to_string(); // own it
                let height = tx.height().unwrap_or(0);

                // Inputs (negative deltas)
                let input_deltas = tx.inputs().iter().enumerate().filter_map({
                    let input_txid = txid.clone(); // clone for inputs
                    move |(input_index, input)| {
                        AddressDelta::from_input(
                            input,
                            input_index as u32,
                            &input_txid,
                            height,           // Copy, so fine to move
                            target_addresses, // &[], reference is Copy
                        )
                    }
                });

                // Outputs (positive deltas)
                let output_deltas = tx.outputs().iter().flat_map({
                    let output_txid = txid; // move the original into outputs
                    move |output| {
                        AddressDelta::from_output(output, &output_txid, height, target_addresses)
                    }
                });

                // Return an iterator; flat_map will consume/flatten it.
                input_deltas.chain(output_deltas)
            })
            .collect()
    }
}

impl ResponseToError for GetAddressDeltasResponse {
    // type RpcError = GetAddressDeltasError;
    type RpcError = Infallible;
}

/// Error type used for the `getaddressdeltas` RPC request.
#[derive(Debug, thiserror::Error)]
pub enum GetAddressDeltasError {}

/// Represents a change in the balance of a transparent address.
#[derive(Debug, Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AddressDelta {
    /// The difference in zatoshis (or satoshis equivalent in Zcash)
    satoshis: i64,
    /// The related transaction ID in hex string format
    txid: String,
    /// The related input or output index
    index: u32,
    /// The block height where the change occurred
    height: u32,
    /// The base58check encoded address
    address: String,
}

impl AddressDelta {
    /// Create a delta from a transaction input (spend - negative value)
    pub fn from_input(
        input: &Input,
        input_index: u32,
        txid: &str,
        height: u32,
        target_addresses: &[String],
    ) -> Option<Self> {
        match input {
            Input::NonCoinbase {
                address: Some(addr),
                value_zat: Some(value),
                ..
            } => {
                // Check if this address is in our target addresses
                if target_addresses.iter().any(|req_addr| req_addr == addr) {
                    Some(AddressDelta {
                        satoshis: -value, // Negative for inputs (spends)
                        txid: txid.to_string(),
                        index: input_index,
                        height,
                        address: addr.clone(),
                    })
                } else {
                    None
                }
            }
            _ => None, // Skip coinbase inputs or inputs without address/value
        }
    }

    /// Create a delta from a transaction output (receive - positive value)
    pub fn from_output(
        output: &Output,
        txid: &str,
        height: u32,
        target_addresses: &[String],
    ) -> Vec<Self> {
        if let Some(output_addresses) = &output.script_pub_key().addresses() {
            output_addresses
                .iter()
                .filter(|addr| target_addresses.iter().any(|req_addr| req_addr == *addr))
                .map(|addr| AddressDelta {
                    satoshis: output.value_zat(), // Positive for outputs (receives)
                    txid: txid.to_string(),
                    index: output.n(),
                    height,
                    address: addr.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Block information for getaddressdeltas responses with chaininfo.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BlockInfo {
    /// The block hash, hex-encoded
    pub hash: String,
    /// The block height
    pub height: u32,
}

impl BlockInfo {
    /// Creates a new BlockInfo from hash and height.
    pub fn new(hash: String, height: u32) -> Self {
        Self { hash, height }
    }

    // Note: The from_height method will be implemented as a separate function
    // that takes a StateServiceSubscriber, to avoid circular dependencies
}
