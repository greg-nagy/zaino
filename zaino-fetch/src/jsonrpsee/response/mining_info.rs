//! Types associated with the `getmininginfo` RPC request.

use std::{collections::HashMap, convert::Infallible};

use serde::{Deserialize, Serialize};

use crate::jsonrpsee::connector::ResponseToError;

impl ResponseToError for GetMiningInfoWire {
    type RpcError = Infallible;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
enum Num {
    U(u64),
    F(f64),
}
impl Num {
    fn as_f64(self) -> f64 {
        match self {
            Num::U(u) => u as f64,
            Num::F(f) => f,
        }
    }
}

/// Wire superset compatible with `zcashd` and `zebrad`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GetMiningInfoWire {
    #[serde(rename = "blocks")]
    tip_height: u64,

    #[serde(rename = "currentblocksize", default)]
    current_block_size: Option<u64>,

    #[serde(rename = "currentblocktx", default)]
    current_block_tx: Option<u64>,

    #[serde(default)]
    networksolps: Option<Num>,
    #[serde(default)]
    networkhashps: Option<Num>,

    // Present on both zcashd and zebrad
    #[serde(default)]
    chain: String,
    #[serde(default)]
    testnet: bool,

    // zcashd
    #[serde(default)]
    difficulty: Option<f64>,
    #[serde(default)]
    errors: Option<String>,
    #[serde(default)]
    errorstimestamp: Option<serde_json::Value>,
    #[serde(default)]
    genproclimit: Option<i64>,
    #[serde(default)]
    localsolps: Option<Num>,
    #[serde(default)]
    pooledtx: Option<u64>,
    #[serde(default)]
    generate: Option<bool>,

    #[serde(flatten)]
    extras: HashMap<String, serde_json::Value>,
}

/// Internal representation of `GetMiningInfoWire`.
#[derive(Debug, Clone)]
pub struct MiningInfo {
    /// Current tip height.
    pub tip_height: u64,

    /// Size of the last mined block, if present.
    pub current_block_size: Option<u64>,

    /// Transaction count in the last mined block, if present.
    pub current_block_tx: Option<u64>,

    /// Estimated network solution rate (Sol/s), if present.
    pub network_solution_rate: Option<f64>,

    /// Estimated network hash rate (H/s), if present.
    pub network_hash_rate: Option<f64>,

    /// Network name (e.g., "main", "test").
    pub chain: String,

    /// Whether the node is on testnet.
    pub testnet: bool,

    /// Current difficulty, if present.
    pub difficulty: Option<f64>,

    /// Upstream error/status message, if present.
    pub errors: Option<String>,

    /// Extra upstream fields.
    pub extras: HashMap<String, serde_json::Value>,
}

impl From<GetMiningInfoWire> for MiningInfo {
    fn from(w: GetMiningInfoWire) -> Self {
        let network_hash_rate = w.networkhashps.map(|n| n.as_f64());
        let network_solution_rate = w.networksolps.map(|n| n.as_f64());

        Self {
            tip_height: w.tip_height,
            current_block_size: w.current_block_size,
            current_block_tx: w.current_block_tx,
            network_solution_rate,
            network_hash_rate,
            chain: w.chain,
            testnet: w.testnet,

            difficulty: w.difficulty,
            errors: w.errors,

            extras: w.extras,
        }
    }
}

/// Parse a `GetMiningInfoWire` into a `MiningInfo`.
pub fn parse_mining_info(json: &str) -> Result<MiningInfo, serde_json::Error> {
    let wire: GetMiningInfoWire = serde_json::from_str(json)?;
    Ok(wire.into())
}
