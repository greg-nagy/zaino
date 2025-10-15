//! Types associated with the `getblockheader` RPC request.

use std::{collections::BTreeMap, convert::Infallible};

use serde::{Deserialize, Serialize};
use zebra_rpc::client::BlockHeaderObject;

use crate::jsonrpsee::connector::ResponseToError;

type z = BlockHeaderObject;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetBlockHeader {
    Verbose(VerboseBlockHeader),
    Compact(String),
    Unknown(serde_json::Value),
}

/// Verbose response to a `getblockheader` RPC request.
///
/// See the notes for the `get_block_header` method.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VerboseBlockHeader {
    /// The hash of the requested block.
    pub hash: String,

    /// The number of confirmations of this block in the best chain,
    /// or -1 if it is not in the best chain.
    pub confirmations: i64,

    /// The height of the requested block.
    pub height: u32,

    /// The version field of the requested block.
    pub version: u32,

    /// The merkle root of the requesteed block.
    #[serde(rename = "merkleroot")]
    pub merkle_root: String,

    /// The blockcommitments field of the requested block. Its interpretation changes
    /// depending on the network and height.
    #[serde(
        rename = "blockcommitments",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub block_commitments: Option<String>,

    /// The root of the Sapling commitment tree after applying this block.
    #[serde(rename = "finalsaplingroot")]
    pub final_sapling_root: String,

    /// The block time of the requested block header in non-leap seconds since Jan 1 1970 GMT.
    pub time: i64,

    /// The nonce of the requested block header.
    pub nonce: String,

    /// The Equihash solution in the requested block header.
    pub solution: String,

    /// The difficulty threshold of the requested block header displayed in compact form.
    pub bits: String,

    /// Floating point number that represents the difficulty limit for this block as a multiple
    /// of the minimum difficulty for the network.
    pub difficulty: f64,

    /// Cumulative chain work for this block (hex).
    ///
    /// Present in zcashd, omitted by Zebra.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chainwork: Option<String>,

    /// The previous block hash of the requested block header.
    #[serde(
        rename = "previousblockhash",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_block_hash: Option<String>,

    /// The next block hash after the requested block header.
    #[serde(
        rename = "nextblockhash",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub next_block_hash: Option<String>,

    /// Catch-all for any extra/undocumented fields.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl ResponseToError for GetBlockHeader {
    type RpcError = Infallible;
}
