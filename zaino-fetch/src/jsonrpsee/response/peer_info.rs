use std::convert::Infallible;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::jsonrpsee::connector::ResponseToError;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum GetPeerInfo {
    Zcashd(Vec<ZcashdPeerInfo>),
    Zebrad(Vec<ZebradPeerInfo>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZebradPeerInfo {
    pub addr: String,
    pub inbound: bool,
}

// TODO: Do not use primitive types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ZcashdPeerInfo {
    pub id: u64,
    pub addr: String,
    // TODO: what does this actually look like?
    pub services: String,
    pub relaytxes: bool,
    pub lastsend: u64,
    pub lastrecv: u64,
    pub bytessent: u64,
    pub bytesrecv: u64,
    pub conntime: u64,
    pub timeoffset: i64,
    pub pingtime: f64,
    pub version: u64,
    pub subver: String,
    pub inbound: bool,
    pub startingheight: i64,
    pub addr_processed: bool,
    pub addr_rate_limited: bool,
    pub whitelisted: bool,

    // Present only sometimes in the C++ codebase:
    #[serde(default)]
    pub addrlocal: Option<String>, // only if not empty

    #[serde(default)]
    pub pingwait: Option<f64>, // only if > 0.0

    // Only when fStateStats == true:
    #[serde(default)]
    pub banscore: Option<i64>,

    #[serde(default, rename = "synced_headers")]
    pub synced_headers: Option<u64>,

    #[serde(default, rename = "synced_blocks")]
    pub synced_blocks: Option<u64>,

    /// Heights array, only when state stats present
    #[serde(default)]
    pub inflight: Option<Vec<i64>>,
}

impl<'de> Deserialize<'de> for GetPeerInfo {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Value::deserialize(de)?;

        if let Ok(zcashd) = serde_json::from_value::<Vec<ZcashdPeerInfo>>(v.clone()) {
            Ok(GetPeerInfo::Zcashd(zcashd))
        } else {
            let zebrad = serde_json::from_value::<Vec<ZebradPeerInfo>>(v)
                .map_err(serde::de::Error::custom)?;
            Ok(GetPeerInfo::Zebrad(zebrad))
        }
    }
}

impl ResponseToError for GetPeerInfo {
    type RpcError = Infallible;
}
