use std::{collections::HashMap, convert::Infallible};

use serde::{Deserialize, Serialize};

use crate::jsonrpsee::connector::ResponseToError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GetPeerInfoItemWire {
    /// Peer index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,

    /// Remote address `host:port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,

    /// Local address `host:port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addrlocal: Option<String>,

    /// Service flags as 16-hex string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub services: Option<String>,

    /// Peer asked us to relay txs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relaytxes: Option<bool>,

    /// Last send (unix seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastsend: Option<u64>,

    /// Last recv (unix seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastrecv: Option<u64>,

    /// Total bytes sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytessent: Option<u64>,

    /// Total bytes received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytesrecv: Option<u64>,

    /// Connect time (unix seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conntime: Option<u64>,

    /// Clock offset (seconds, can be negative).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeoffset: Option<i64>,

    /// Ping time (seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pingtime: Option<f64>,

    /// Ping wait (seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pingwait: Option<f64>,

    /// Protocol version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,

    /// User agent string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subver: Option<String>,

    /// Inbound (true) or outbound (false).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound: Option<bool>,

    /// Peer’s starting height.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startingheight: Option<i64>,

    /// Misbehavior score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub banscore: Option<i64>,

    /// Last header in common.
    #[serde(
        default,
        rename = "synced_headers",
        skip_serializing_if = "Option::is_none"
    )]
    pub synced_headers: Option<u64>,

    /// Last block in common.
    #[serde(
        default,
        rename = "synced_blocks",
        skip_serializing_if = "Option::is_none"
    )]
    pub synced_blocks: Option<u64>,

    /// Heights in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inflight: Option<Vec<i64>>,

    /// Address processing state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr_processed: Option<bool>,

    /// Address rate limit state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr_rate_limited: Option<bool>,

    /// Whitelist state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whitelisted: Option<bool>,

    /// Unmodeled fields.
    #[serde(flatten, default, skip_serializing_if = "HashMap::is_empty")]
    pub extras: HashMap<String, serde_json::Value>,
}

/// Full zcashd response for `getpeerinfo`.
pub type GetPeerInfoWire = Vec<GetPeerInfoItemWire>;

impl ResponseToError for GetPeerInfoWire {
    type RpcError = Infallible;
}

/// Internal, daemon-agnostic peer info.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerInfoInternal {
    /// Remote address `host:port`, if known.
    pub addr: Option<String>,
    /// Inbound connection.
    pub inbound: bool,

    /// Protocol version, if known.
    pub version: Option<u64>,
    /// User agent, if known.
    pub subver: Option<String>,

    /// Ping time (s), if known.
    pub ping_time: Option<f64>,
    /// Bytes sent, if known.
    pub bytes_sent: Option<u64>,
    /// Bytes received, if known.
    pub bytes_recv: Option<u64>,

    /// Synced header height, if known.
    pub synced_headers: Option<u64>,
    /// Synced block height, if known.
    pub synced_blocks: Option<u64>,
    /// Heights in flight.
    pub inflight: Vec<i64>,

    /// Anything else we didn’t model.
    pub extras: HashMap<String, serde_json::Value>,
}

impl Default for PeerInfoInternal {
    fn default() -> Self {
        Self {
            addr: None,
            inbound: false,
            version: None,
            subver: None,
            ping_time: None,
            bytes_sent: None,
            bytes_recv: None,
            synced_headers: None,
            synced_blocks: None,
            inflight: Vec::new(),
            extras: HashMap::new(),
        }
    }
}

impl From<GetPeerInfoItemWire> for PeerInfoInternal {
    fn from(w: GetPeerInfoItemWire) -> Self {
        Self {
            addr: w.addr,
            inbound: w.inbound.unwrap_or(false),
            version: w.version,
            subver: w.subver,
            ping_time: w.pingtime,
            bytes_sent: w.bytessent,
            bytes_recv: w.bytesrecv,
            synced_headers: w.synced_headers,
            synced_blocks: w.synced_blocks,
            inflight: w.inflight.unwrap_or_default(),
            extras: w.extras,
        }
    }
}

impl From<zebra_rpc::client::PeerInfo> for PeerInfoInternal {
    fn from(z: zebra_rpc::client::PeerInfo) -> Self {
        Self {
            addr: Some(z.addr().to_string()),
            inbound: z.inbound(),
            ..Self::default()
        }
    }
}

/// Parse `getpeerinfo` JSON (zcashd) into internal items.
pub fn parse_getpeerinfo(json: &str) -> Result<Vec<PeerInfoInternal>, serde_json::Error> {
    let wire: GetPeerInfoWire = serde_json::from_str(json)?;
    Ok(wire.into_iter().map(PeerInfoInternal::from).collect())
}
