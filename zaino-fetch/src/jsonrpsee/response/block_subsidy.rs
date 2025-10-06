//! Types associated with the `getblocksubsidy` RPC request.

use std::convert::Infallible;

use crate::jsonrpsee::{connector::ResponseToError, response::common::amount::Zatoshis};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Struct used to represent both a funding stream and a lockbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FundingStream {
    pub recipient: String,
    pub specification: String,

    /// Amount as ZEC on the wire (string or number) → normalized to zatoshis.
    #[serde(rename = "value")]
    pub value: Zatoshis,

    /// Amount as zatoshis on the wire.
    #[serde(rename = "valueZat")]
    pub value_zat: u64,

    /// Present for funding streams; absent for lockbox streams.
    #[serde(default)]
    pub address: Option<String>,
}

/// Response to a `getblocksubsidy` RPC request. Used for both `zcashd` and `zebrad`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlockSubsidy {
    #[serde(rename = "miner")]
    pub miner_zat: Zatoshis,
    #[serde(rename = "founders")]
    pub founders_zat: Zatoshis,
    #[serde(rename = "fundingstreamstotal")]
    pub funding_streams_total_zat: Zatoshis,
    #[serde(rename = "lockboxtotal")]
    pub lockbox_total_zat: Zatoshis,
    #[serde(rename = "totalblocksubsidy")]
    pub total_block_subsidy_zat: Zatoshis,

    #[serde(rename = "fundingstreams", default)]
    pub funding_streams: Vec<FundingStream>,

    #[serde(rename = "lockboxstreams", default)]
    pub lockbox_streams: Vec<FundingStream>,
}

/// Keep your “Unknown” escape hatch if you want configurability at the binary layer.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum GetBlockSubsidy {
    Known(BlockSubsidy),
    Unknown(Value),
}

impl ResponseToError for GetBlockSubsidy {
    type RpcError = Infallible;
}

impl<'de> Deserialize<'de> for GetBlockSubsidy {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(de)?;
        if let Ok(bs) = serde_json::from_value::<BlockSubsidy>(v.clone()) {
            Ok(GetBlockSubsidy::Known(bs))
        } else {
            Ok(GetBlockSubsidy::Unknown(v))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::jsonrpsee::response::block_subsidy::GetBlockSubsidy;

    #[test]
    fn zcashd_decimals_parse_to_zats() {
        let j = serde_json::json!({
          "miner": 2.5,
          "founders": 0.0,
          "fundingstreamstotal": 0.5,
          "lockboxtotal": 0.0,
          "totalblocksubsidy": 3.0,
          "fundingstreams": [
            {"recipient":"ZCG","specification":"https://spec","value":0.5,"valueZat":50_000_000,"address":"t1abc"}
          ]
        });
        let r: GetBlockSubsidy = serde_json::from_value(j).unwrap();
        match r {
            GetBlockSubsidy::Known(x) => {
                assert_eq!(u64::from(x.miner_zat), 250_000_000);
                assert_eq!(u64::from(x.funding_streams_total_zat), 50_000_000);
                assert_eq!(u64::from(x.total_block_subsidy_zat), 300_000_000);
                assert_eq!(x.funding_streams.len(), 1);
                assert_eq!(x.funding_streams[0].value_zat, 50_000_000);
                assert_eq!(x.funding_streams[0].address.as_deref(), Some("t1abc"));
            }
            _ => panic!("expected Known"),
        }
    }

    #[test]
    fn zebrad_strings_parse_to_zats() {
        let j = serde_json::json!({
          "fundingstreams": [],
          "lockboxstreams": [],
          "miner": "2.5",
          "founders": "0.0",
          "fundingstreamstotal": "0.5",
          "lockboxtotal": "0.0",
          "totalblocksubsidy": "3.0"
        });
        let r: GetBlockSubsidy = serde_json::from_value(j).unwrap();
        match r {
            GetBlockSubsidy::Known(x) => {
                assert_eq!(u64::from(x.miner_zat), 250_000_000);
                assert_eq!(u64::from(x.total_block_subsidy_zat), 300_000_000);
                assert!(x.funding_streams.is_empty());
                assert!(x.lockbox_streams.is_empty());
            }
            _ => panic!("expected Known"),
        }
    }
}
