use std::convert::Infallible;

use zebra_chain::amount::{Amount, NonNegative};

use crate::jsonrpsee::connector::ResponseToError;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct BlockDeltas {
    pub hash: String,
    pub confirmations: i64,
    pub size: i64,
    pub height: u32,
    pub version: u32,

    #[serde(rename = "merkleroot")]
    pub merkle_root: String,

    /// TODO: double check that `FullTransaction` is the correct type
    pub deltas: Vec<BlockDelta>,
    pub time: i64,
    pub mediantime: i64,
    pub nonce: String,
    pub bits: String,
    pub difficulty: f64,
    // `chainwork` would be here, but Zebra does not plan to support it
    // pub chainwork: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previousblockhash: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nextblockhash: Option<String>,
}

impl ResponseToError for BlockDeltas {
    type RpcError = Infallible;
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct BlockDelta {
    pub txid: String,
    pub index: u32,
    pub inputs: Vec<InputDelta>,
    pub outputs: Vec<OutputDelta>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InputDelta {
    pub address: String,
    pub satoshis: Amount,
    pub index: u32,
    pub prevtxid: String,
    pub prevout: u32,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OutputDelta {
    pub address: String,
    pub satoshis: Amount<NonNegative>,
    pub index: u32,
}
