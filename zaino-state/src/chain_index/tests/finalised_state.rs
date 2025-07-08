//! Zaino-State ChainIndex Finalised State (ZainoDB) unit tests.

use core2::io::{self, Read};
use prost::Message;
use std::io::BufReader;
use std::path::Path;
use std::{fs::File, path::PathBuf};
use zaino_proto::proto::compact_formats::CompactBlock;
use zebra_rpc::methods::GetAddressUtxos;

use crate::{read_u32_le, ChainBlock, CompactSize, ZainoVersionedSerialise as _};

/// Reads test data from file.
fn read_vectors_from_file<P: AsRef<Path>>(
    base_dir: P,
) -> io::Result<(
    Vec<(u32, ChainBlock, CompactBlock)>,
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    (Vec<String>, Vec<GetAddressUtxos>, u64),
)> {
    let base = base_dir.as_ref();

    let mut chain_blocks = Vec::<(u32, ChainBlock)>::new();
    {
        let mut r = BufReader::new(File::open(base.join("chain_blocks.dat"))?);
        loop {
            let height = match read_u32_le(&mut r) {
                Ok(h) => h,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            let len: usize = CompactSize::read_t(&mut r)?;
            let mut buf = vec![0u8; len];
            r.read_exact(&mut buf)?;
            let chain = ChainBlock::from_bytes(&buf)?; // <── new
            chain_blocks.push((height, chain));
        }
    }

    let mut full_blocks = Vec::<(u32, ChainBlock, CompactBlock)>::with_capacity(chain_blocks.len());
    {
        let mut r = BufReader::new(File::open(base.join("compact_blocks.dat"))?);
        for (h1, chain) in chain_blocks {
            let h2 = read_u32_le(&mut r)?;
            if h1 != h2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "height mismatch between ChainBlock and CompactBlock streams",
                ));
            }
            let len: usize = CompactSize::read_t(&mut r)?;
            let mut buf = vec![0u8; len];
            r.read_exact(&mut buf)?;
            let compact = CompactBlock::decode(&*buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            full_blocks.push((h1, chain, compact));
        }
    }

    let faucet = serde_json::from_reader(File::open(base.join("faucet_data.json"))?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let recipient = serde_json::from_reader(File::open(base.join("recipient_data.json"))?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok((full_blocks, faucet, recipient))
}

#[test]
fn vectors_can_be_loaded_and_deserialised() -> io::Result<()> {
    // <repo>/zaino-state/src/chain_index/tests/vectors
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("chain_index")
        .join("tests")
        .join("vectors");
    let (blocks, faucet, recipient) = read_vectors_from_file(&base_dir)?;

    // Chech block data..
    assert!(
        !blocks.is_empty(),
        "expected at least one block in test-vectors"
    );
    let mut prev_h: u32 = 0;
    for (h, chain_block, compact_block) in &blocks {
        println!("Checking block at height {h}");

        assert_eq!(
            prev_h,
            (h - 1),
            "Chain continuity check failed at height {h}"
        );
        prev_h = *h;

        let compact_block_hash = compact_block.hash.clone();
        let chain_block_hash_bytes = chain_block.hash().0.to_vec();

        assert_eq!(
            compact_block_hash, chain_block_hash_bytes,
            "Block hash check failed at height {h}"
        );

        // ChainBlock round trip check.
        let bytes = chain_block.to_bytes()?;
        let reparsed = ChainBlock::from_bytes(&bytes)?;
        assert_eq!(
            chain_block, &reparsed,
            "ChainBlock round-trip failed at height {h}"
        );
    }

    // check taddrs.
    let (_, utxos_f, _) = faucet;
    let (_, utxos_r, _) = recipient;

    println!("\nFaucet UTXO address:");
    let (addr, _hash, _outindex, _script, _value, _height) = utxos_f[0].into_parts();
    println!("addr: {}", addr);

    println!("\nRecipient UTXO address:");
    let (addr, _hash, _outindex, _script, _value, _height) = utxos_r[0].into_parts();
    println!("addr: {}", addr);

    // Script hash <-> Taddress round trip check.

    Ok(())
}
