use std::convert::Infallible;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::jsonrpsee::connector::ResponseToError;

// TODO: A potential useful test would be to boot up multiple nodes and compare multiple `getpeerinfo` calls
// to a `zcashd` and `zebrad` node.
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
    pub id: i64,
    pub addr: String,
    pub services: ServiceFlags,
    pub relaytxes: bool,
    pub lastsend: i64, // seconds since epoch
    pub lastrecv: i64, // seconds since epoch
    pub bytessent: u64,
    pub bytesrecv: u64,
    pub conntime: i64,   // seconds since epoch
    pub timeoffset: i64, // can be negative
    pub pingtime: f64,   // seconds
    pub version: i64,
    pub subver: String,
    pub inbound: bool,
    pub startingheight: i64,
    pub addr_processed: u64,
    pub addr_rate_limited: u64,
    pub whitelisted: bool,

    // conditional in RPC output
    #[serde(default)]
    pub addrlocal: Option<String>, // only if not empty

    #[serde(default)]
    pub pingwait: Option<f64>, // only if > 0.0

    // only when fStateStats is true
    #[serde(default)]
    pub banscore: Option<i64>,

    #[serde(default, rename = "synced_headers")]
    pub synced_headers: Option<i64>,

    #[serde(default, rename = "synced_blocks")]
    pub synced_blocks: Option<i64>,

    #[serde(default)]
    pub inflight: Option<Vec<i64>>, // heights
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

/// Bitflags for the peer's advertised services (backed by a u64).
/// Serialized as a zero-padded 16-digit lowercase hex string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServiceFlags(pub u64);

impl ServiceFlags {
    pub fn bits(self) -> u64 {
        self.0
    }
    pub fn has(self, mask: u64) -> bool {
        (self.0 & mask) != 0
    }

    pub const NODE_NETWORK: u64 = 1 << 0;
    // Legacy. see NO_BLOOM_VERSION
    pub const NODE_BLOOM: u64 = 1 << 2;

    pub fn has_node_network(self) -> bool {
        self.has(Self::NODE_NETWORK)
    }
    pub fn has_node_bloom(self) -> bool {
        self.has(Self::NODE_BLOOM)
    }

    // Helper to surface forward-compat unknown bits
    pub fn unknown_bits(self) -> u64 {
        let known = Self::NODE_NETWORK | Self::NODE_BLOOM;
        self.bits() & !known
    }
}

impl From<u64> for ServiceFlags {
    fn from(x: u64) -> Self {
        ServiceFlags(x)
    }
}
impl From<ServiceFlags> for u64 {
    fn from(f: ServiceFlags) -> Self {
        f.0
    }
}

impl Serialize for ServiceFlags {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&format!("{:016x}", self.0))
    }
}
impl<'de> Deserialize<'de> for ServiceFlags {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;

        // Optional `0x`
        let s = s.strip_prefix("0x").unwrap_or(&s);
        u64::from_str_radix(s, 16)
            .map(ServiceFlags)
            .map_err(|e| serde::de::Error::custom(format!("invalid services hex: {e}")))
    }
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
            "addr_processed": 1,
            "addr_rate_limited": 0,
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

    mod serviceflags {
        use crate::jsonrpsee::response::peer_info::{ServiceFlags, ZcashdPeerInfo};

        #[test]
        fn serviceflags_roundtrip() {
            let f = ServiceFlags(0x0000_0000_0000_0001);
            let s = serde_json::to_string(&f).unwrap();
            assert_eq!(s, r#""0000000000000001""#); // zero-padded, lowercase
            let back: ServiceFlags = serde_json::from_str(&s).unwrap();
            assert_eq!(back.bits(), 1);
            assert!(back.has(1));
        }

        #[test]
        fn zcashd_peerinfo_deser_with_typed_services() {
            let j = r#"[{
            "id":1,
            "addr":"127.0.0.1:8233",
            "services":"0000000000000003",
            "relaytxes":true,
            "lastsend":1,"lastrecv":2,"bytessent":3,"bytesrecv":4,
            "conntime":5,"timeoffset":0,"pingtime":0.001,
            "version":170002,"subver":"/MagicBean:5.8.0/","inbound":false,
            "startingheight":2000000,"addr_processed":7,"addr_rate_limited":8,"whitelisted":false
        }]"#;

            let v: Vec<ZcashdPeerInfo> = serde_json::from_str(j).unwrap();
            assert_eq!(v[0].services.bits(), 3);
            assert!(v[0].services.has(1));
            assert!(v[0].services.has(2));
        }

        #[test]
        fn zcashd_peerinfo_serializes_back_to_hex() {
            let pi = ZcashdPeerInfo {
                id: 1,
                addr: "127.0.0.1:8233".into(),
                services: ServiceFlags(0x0A0B_0C0D_0E0F),
                relaytxes: true,
                lastsend: 1,
                lastrecv: 2,
                bytessent: 3,
                bytesrecv: 4,
                conntime: 5,
                timeoffset: 0,
                pingtime: 0.1,
                version: 170002,
                subver: "/X/".into(),
                inbound: false,
                startingheight: 42,
                addr_processed: 0,
                addr_rate_limited: 0,
                whitelisted: false,
                addrlocal: None,
                pingwait: None,
                banscore: None,
                synced_headers: None,
                synced_blocks: None,
                inflight: None,
            };

            let v = serde_json::to_value(&pi).unwrap();
            let services_str = v["services"].as_str().unwrap();
            let expected = format!("{:016x}", u64::from(pi.services));
            assert_eq!(services_str, expected); // "00000a0b0c0d0e0f"
        }
    }
}
