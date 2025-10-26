use std::convert::Infallible;

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
    pub previousblockhash: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nextblockhash: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct BlockDelta {
    pub txid: String,
    pub index: u32,
    pub inputs: Vec<serde_json::Value>,
    pub outputs: Vec<serde_json::Value>,
}

impl Default for BlockDelta {
    fn default() -> Self {
        Self {
            txid: "".to_string(),
            index: 0,
            inputs: vec![],
            outputs: vec![],
        }
    }
}

impl ResponseToError for BlockDeltas {
    type RpcError = Infallible;
}
