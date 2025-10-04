use std::convert::Infallible;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::jsonrpsee::connector::ResponseToError;

/// Response to a `getpeerinfo` RPC request.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum GetPeerInfo {
    /// The `zcashd` typed response.
    Zcashd(Vec<ZcashdPeerInfo>),

    /// The `zebrad` typed response.
    Zebrad(Vec<ZebradPeerInfo>),

    /// Unrecognized shape. Only enforced to be an array.
    Unknown(Vec<Value>),
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
    /// Deserialize either a `ZcashdPeerInfo` or a `ZebradPeerInfo` depending on the shape of the JSON.
    ///
    /// In the `Unkown` variant, the raw array is preserved for passthrough/logging.
    /// If the value is not an array, an error is returned.
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Value::deserialize(de)?;

        // zcashd first
        if let Ok(zd) = serde_json::from_value::<Vec<ZcashdPeerInfo>>(v.clone()) {
            return Ok(GetPeerInfo::Zcashd(zd));
        }
        // zebrad
        if let Ok(zebra) = serde_json::from_value::<Vec<ZebradPeerInfo>>(v.clone()) {
            return Ok(GetPeerInfo::Zebrad(zebra));
        }
        // unknown
        // TODO: Does it make sense to enforce an unkown value to be an array?
        if v.is_array() {
            let raw: Vec<Value> = serde_json::from_value(v).map_err(serde::de::Error::custom)?;
            Ok(GetPeerInfo::Unknown(raw))
        } else {
            Err(serde::de::Error::custom("getpeerinfo: expected JSON array"))
        }
    }
}

impl ResponseToError for GetPeerInfo {
    type RpcError = Infallible;
}

#[cfg(test)]
mod tests {
    use super::*;
    // use pretty_assertions::assert_eq;

    // TODO: get a real testvector
    #[test]
    fn parses_zcashd_payload() {
        let zcashd_json = r#"
        [
          {
            "id": 1,
            "addr": "127.0.0.1:8233",
            "services": "0000000000000001",
            "relaytxes": true,
            "lastsend": 1690000000,
            "lastrecv": 1690000100,
            "bytessent": 1234,
            "bytesrecv": 5678,
            "conntime": 1690000000,
            "timeoffset": 0,
            "pingtime": 0.001,
            "version": 170002,
            "subver": "/MagicBean:5.8.0/",
            "inbound": false,
            "startingheight": 2000000,
            "addr_processed": true,
            "addr_rate_limited": false,
            "whitelisted": false,
            "addrlocal": "192.168.1.10:8233",
            "pingwait": 0.1,
            "banscore": 0,
            "synced_headers": 1999999,
            "synced_blocks": 1999999,
            "inflight": [2000000, 2000001]
          }
        ]
        "#;

        let parsed: GetPeerInfo = serde_json::from_str(zcashd_json).unwrap();
        match parsed {
            GetPeerInfo::Zcashd(items) => {
                assert_eq!(items.len(), 1);
                let p = &items[0];
                assert_eq!(p.id, 1);
                assert_eq!(p.addr, "127.0.0.1:8233");
                assert_eq!(p.version, 170002);
                assert_eq!(p.inbound, false);
                assert_eq!(p.synced_blocks, Some(1999999));
                assert_eq!(p.pingwait, Some(0.1));
            }
            other => panic!("expected Zcashd variant, got: {:?}", other),
        }
    }

    // TODO: get a real testvector
    #[test]
    fn parses_zebrad_payload() {
        let zebrad_json = r#"
        [
          { "addr": "1.2.3.4:8233", "inbound": true },
          { "addr": "5.6.7.8:8233", "inbound": false }
        ]
        "#;

        let parsed: GetPeerInfo = serde_json::from_str(zebrad_json).unwrap();
        match parsed {
            GetPeerInfo::Zebrad(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].addr, "1.2.3.4:8233");
                assert_eq!(items[0].inbound, true);
                assert_eq!(items[1].addr, "5.6.7.8:8233");
                assert_eq!(items[1].inbound, false);
            }
            other => panic!("expected Zebrad variant, got: {:?}", other),
        }
    }

    // TODO: get a real testvector
    #[test]
    fn falls_back_to_unknown_for_unrecognized_shape() {
        let unknown_json = r#"
        [
          { "foo": 1, "bar": "baz" },
          { "weird": [1,2,3] }
        ]
        "#;

        let parsed: GetPeerInfo = serde_json::from_str(unknown_json).unwrap();
        match parsed {
            GetPeerInfo::Unknown(items) => {
                assert_eq!(items.len(), 2);
                assert!(items[0].get("foo").is_some());
            }
            other => panic!("expected Unknown variant, got: {:?}", other),
        }
    }

    // TODO: get a real testvector
    #[test]
    fn fails_on_non_array() {
        let non_array_json = r#"{"foo": 1, "bar": "baz"}"#;
        let err = serde_json::from_str::<GetPeerInfo>(non_array_json).unwrap_err();
        assert_eq!(err.to_string(), "getpeerinfo: expected JSON array");
    }
}
