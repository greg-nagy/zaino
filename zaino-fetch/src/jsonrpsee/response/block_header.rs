//! Types associated with the `getblockheader` RPC request.

use std::{collections::BTreeMap, convert::Infallible};

use serde::{Deserialize, Serialize};

use zebra_rpc::methods::opthex;

use crate::jsonrpsee::connector::ResponseToError;

/// Response to a `getblockheader` RPC request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetBlockHeader {
    /// The verbose variant of the response. Returned when `verbose` is set to `true`.
    Verbose(VerboseBlockHeader),

    /// The compact variant of the response. Returned when `verbose` is set to `false`.
    Compact(String),

    /// An unknown response shape.
    Unknown(serde_json::Value),
}

/// Verbose response to a `getblockheader` RPC request.
///
/// See the notes for the `get_block_header` method.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VerboseBlockHeader {
    /// The hash of the requested block.
    #[serde(with = "hex")]
    pub hash: zebra_chain::block::Hash,

    /// The number of confirmations of this block in the best chain,
    /// or -1 if it is not in the best chain.
    pub confirmations: i64,

    /// The height of the requested block.
    pub height: u32,

    /// The version field of the requested block.
    pub version: u32,

    /// The merkle root of the requesteed block.
    #[serde(rename = "merkleroot")]
    pub merkle_root: zebra_chain::block::merkle::Root,

    /// The blockcommitments field of the requested block. Its interpretation changes
    /// depending on the network and height.
    #[serde(
        with = "opthex",
        rename = "blockcommitments",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub block_commitments: Option<[u8; 32]>,

    /// The root of the Sapling commitment tree after applying this block.
    #[serde(with = "opthex", rename = "finalsaplingroot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_sapling_root: Option<[u8; 32]>,

    /// The root of the Orchard commitment tree after applying this block.
    #[serde(with = "opthex", rename = "finalorchardroot")]
    #[serde(skip_serializing_if = "Option::is_none")]
    final_orchard_root: Option<[u8; 32]>,

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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use hex::FromHex;
    use serde_json::{json, Value};
    use zebra_chain::block;

    /// Zcashd verbose response.
    fn zcashd_verbose_json() -> &'static str {
        r#"{
          "hash": "000000000053d2771290ff1b57181bd067ae0e55a367ba8ddee2d961ea27a14f",
          "confirmations": 10,
          "height": 123456,
          "version": 4,
          "merkleroot": "aa11merkle",
          "finalsaplingroot": "bb22sapling",
          "time": 1700000000,
          "nonce": "11nonce",
          "solution": "22solution",
          "bits": "1d00ffff",
          "difficulty": 123456.789,
          "chainwork": "0000000000000000000000000000000000000000000000000000000000001234",
          "previousblockhash": "prevhash0001",
          "nextblockhash": "nexthash0001",
          "mediantime": 1700000500,
          "nTx": 12
        }"#
    }

    // Zebra verbose response
    // TODO: Add finalorchardroot
    fn zebra_verbose_json() -> &'static str {
        r#"{
          "hash": "00000000001b76b932f31289beccd3988d098ec3c8c6e4a0c7bcaf52e9bdead1",
          "confirmations": 3,
          "height": 42,
          "version": 5,
          "merkleroot": "bb33merkle",
          "blockcommitments": "cc44blockcommitments",
          "finalsaplingroot": "dd55sapling",
          "time": 1699999999,
          "nonce": "33nonce",
          "solution": "44solution",
          "bits": "1c654321",
          "difficulty": 7890.123,
          "previousblockhash": "prevhash0042"
        }"#
    }

    #[test]
    fn deserialize_verbose_zcashd_includes_chainwork_and_extra() {
        let block_header: GetBlockHeader = serde_json::from_str(zcashd_verbose_json()).unwrap();
        match block_header {
            GetBlockHeader::Verbose(v) => {
                assert_eq!(
                    v.hash,
                    block::Hash::from_str(
                        "000000000053d2771290ff1b57181bd067ae0e55a367ba8ddee2d961ea27a14f"
                    )
                    .unwrap()
                );
                assert_eq!(v.confirmations, 10);
                assert_eq!(v.height, 123_456);
                assert_eq!(v.version, 4);
                assert_eq!(
                    v.merkle_root,
                    block::merkle::Root::from_hex("aa11merkle").unwrap()
                );
                assert_eq!(v.final_sapling_root.unwrap(), "bb22sapling".as_bytes());
                assert_eq!(v.time, 1_700_000_000);
                assert_eq!(v.nonce, "11nonce");
                assert_eq!(v.solution, "22solution");
                assert_eq!(v.bits, "1d00ffff");
                assert!((v.difficulty - 123_456.789).abs() < f64::EPSILON);

                assert_eq!(
                    v.chainwork.as_deref(),
                    Some("0000000000000000000000000000000000000000000000000000000000001234")
                );

                assert_eq!(v.previous_block_hash.as_deref(), Some("prevhash0001"));
                assert_eq!(v.next_block_hash.as_deref(), Some("nexthash0001"));

                // Extras
                assert_eq!(v.extra.get("mediantime"), Some(&json!(1_700_000_500)));
                assert_eq!(v.extra.get("nTx"), Some(&json!(12)));
            }
            _ => panic!("expected Verbose variant"),
        }
    }

    #[test]
    fn deserialize_verbose_zebra_includes_blockcommitments_and_omits_chainwork() {
        let block_header: GetBlockHeader = serde_json::from_str(zebra_verbose_json()).unwrap();
        match block_header {
            GetBlockHeader::Verbose(v) => {
                assert_eq!(
                    v.hash,
                    block::Hash::from_str(
                        "00000000001b76b932f31289beccd3988d098ec3c8c6e4a0c7bcaf52e9bdead1"
                    )
                    .unwrap()
                );
                assert_eq!(v.confirmations, 3);
                assert_eq!(v.height, 42);
                assert_eq!(v.version, 5);
                assert_eq!(
                    v.merkle_root,
                    block::merkle::Root::from_hex("bb33merkle").unwrap()
                );

                assert_eq!(
                    v.block_commitments.unwrap(),
                    "cc44blockcommitments".as_bytes()
                );

                assert_eq!(v.final_sapling_root.unwrap(), "dd55sapling".as_bytes());
                assert_eq!(v.time, 1_699_999_999);
                assert_eq!(v.nonce, "33nonce");
                assert_eq!(v.solution, "44solution");
                assert_eq!(v.bits, "1c654321");
                assert!((v.difficulty - 7890.123).abs() < f64::EPSILON);

                assert!(v.chainwork.is_none());

                // Zebra always sets previous
                assert_eq!(v.previous_block_hash.as_deref(), Some("prevhash0042"));
                assert!(v.next_block_hash.is_none());

                // No extras
                assert!(v.extra.is_empty());
            }
            _ => panic!("expected Verbose variant"),
        }
    }

    #[test]
    fn compact_header_is_hex_string() {
        let s = r#""040102deadbeef""#;
        let block_header: GetBlockHeader = serde_json::from_str(s).unwrap();
        match block_header.clone() {
            GetBlockHeader::Compact(hex) => assert_eq!(hex, "040102deadbeef"),
            _ => panic!("expected Compact variant"),
        }

        // Roundtrip
        let out = serde_json::to_string(&block_header).unwrap();
        assert_eq!(out, s);
    }

    #[test]
    fn unknown_shape_falls_back_to_unknown_variant() {
        let weird = r#"{ "weird": 1, "unexpected": ["a","b","c"] }"#;
        let block_header: GetBlockHeader = serde_json::from_str(weird).unwrap();
        match block_header {
            GetBlockHeader::Unknown(v) => {
                assert_eq!(v["weird"], json!(1));
                assert_eq!(v["unexpected"], json!(["a", "b", "c"]));
            }
            _ => panic!("expected Unknown variant"),
        }
    }

    #[test]
    fn zebra_roundtrip_does_not_inject_chainwork_field() {
        let block_header: GetBlockHeader = serde_json::from_str(zebra_verbose_json()).unwrap();
        let header_value: Value = serde_json::to_value(&block_header).unwrap();

        let header_object = header_value
            .as_object()
            .expect("verbose should serialize to object");
        assert!(!header_object.contains_key("chainwork"));

        assert_eq!(
            header_object.get("blockcommitments"),
            Some(&json!("cc44blockcommitments"))
        );
    }

    #[test]
    fn zcashd_roundtrip_preserves_chainwork_and_extras() {
        let block_header: GetBlockHeader = serde_json::from_str(zcashd_verbose_json()).unwrap();
        let header_value: Value = serde_json::to_value(&block_header).unwrap();
        let header_object = header_value.as_object().unwrap();

        assert_eq!(
            header_object.get("chainwork"),
            Some(&json!(
                "0000000000000000000000000000000000000000000000000000000000001234"
            ))
        );

        assert_eq!(header_object.get("mediantime"), Some(&json!(1_700_000_500)));
        assert_eq!(header_object.get("nTx"), Some(&json!(12)));
    }

    #[test]
    fn previous_and_next_optional_edges() {
        // Simulate genesis
        let genesis_like = r#"{
          "hash": "00000000001b76b932f31289beccd3988d098ec3c8c6e4a0c7bcaf52e9bdead1",
          "confirmations": 1,
          "height": 0,
          "version": 4,
          "merkleroot": "000000000053d2771290ff1b57181bd067ae0e55a367ba8ddee2d961ea27a14f",
          "finalsaplingroot": "000000000053d2771290ff1b57181bd067ae0e55a367ba8ddee2d961ea27a14f",
          "finalorchardroot": "000000000053d2771290ff1b57181bd067ae0e55a367ba8ddee2d961ea27a14f",
          "time": 1477641369,
          "nonce": "nonce",
          "solution": "solution",
          "bits": "1d00ffff",
          "difficulty": 1.0
        }"#;

        let block_header: GetBlockHeader = serde_json::from_str(genesis_like).unwrap();
        match block_header {
            GetBlockHeader::Verbose(v) => {
                assert!(v.previous_block_hash.is_none());
                assert!(v.next_block_hash.is_none());
            }
            GetBlockHeader::Compact(_) => panic!("expected Verbose variant, got Compact"),
            GetBlockHeader::Unknown(_) => panic!("expected Verbose variant, got Unknown"),
        }
    }
}
