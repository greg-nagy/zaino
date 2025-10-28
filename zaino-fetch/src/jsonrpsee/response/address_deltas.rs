//! Types associated with the `getaddressdeltas` RPC request.

use std::convert::Infallible;

use zebra_rpc::client::{Input, Output, TransactionObject};

use crate::jsonrpsee::connector::ResponseToError;

/// Request parameters for the `getaddressdeltas` RPC method.
/// Extends the basic address/height range with chaininfo support.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GetAddressDeltasRequest {
    /// List of base58check encoded addresses
    pub addresses: Vec<String>,
    /// Start block height (inclusive)
    pub start: u32,
    /// End block height (inclusive)
    pub end: u32,
    /// Whether to include chain info in response (defaults to false)
    #[serde(default)]
    pub chaininfo: bool,
}

impl GetAddressDeltasRequest {
    /// Creates a new request with the given parameters.
    pub fn new(addresses: Vec<String>, start: u32, end: u32, chaininfo: bool) -> Self {
        Self {
            addresses,
            start,
            end,
            chaininfo,
        }
    }

    /// Creates a new request without chaininfo (defaults to false).
    pub fn simple(addresses: Vec<String>, start: u32, end: u32) -> Self {
        Self::new(addresses, start, end, false)
    }

    /// Creates a new request with chaininfo enabled.
    pub fn with_chaininfo(addresses: Vec<String>, start: u32, end: u32) -> Self {
        Self::new(addresses, start, end, true)
    }

    /// Decomposes the request into its constituent parts.
    pub fn into_parts(self) -> (Vec<String>, u32, u32, bool) {
        (self.addresses, self.start, self.end, self.chaininfo)
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
                let txid = hex::encode(tx.hex().as_ref());
                let height = tx.height().unwrap_or(0);

                // Process inputs (spends - negative deltas)
                let txid_clone = txid.clone();
                let input_deltas =
                    tx.inputs()
                        .iter()
                        .enumerate()
                        .filter_map(move |(input_index, input)| {
                            AddressDelta::from_input(
                                input,
                                input_index as u32,
                                &txid_clone,
                                height,
                                target_addresses,
                            )
                        });

                // Process outputs (receives - positive deltas)
                let output_deltas = tx.outputs().iter().flat_map(move |output| {
                    AddressDelta::from_output(output, &txid, height, target_addresses)
                });

                // Chain input and output deltas together
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
