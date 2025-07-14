//! Zaino-State ChainIndex Finalised State (ZainoDB) unit tests.

use core2::io::{self, Read};
use prost::Message;
use std::io::BufReader;
use std::path::Path;
use std::{fs::File, path::PathBuf};
use tempfile::TempDir;

use zaino_proto::proto::compact_formats::CompactBlock;
use zebra_rpc::methods::GetAddressUtxos;

use crate::bench::BlockCacheConfig;
use crate::chain_index::finalised_state::ZainoDB;
use crate::error::FinalisedStateError;
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

fn load_test_vectors() -> io::Result<(
    Vec<(u32, ChainBlock, CompactBlock)>,
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    (Vec<String>, Vec<GetAddressUtxos>, u64),
)> {
    // <repo>/zaino-state/src/chain_index/tests/vectors
    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("chain_index")
        .join("tests")
        .join("vectors");
    read_vectors_from_file(&base_dir)
}

async fn spawn_default_zaino_db() -> Result<(TempDir, ZainoDB), FinalisedStateError> {
    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();

    let config = BlockCacheConfig {
        map_capacity: None,
        map_shard_amount: None,
        db_path,
        db_size: None,
        network: zebra_chain::parameters::Network::new_regtest(
            zebra_chain::parameters::testnet::ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(1),
                sapling: Some(1),
                blossom: Some(1),
                heartwood: Some(1),
                canopy: Some(1),
                nu5: Some(1),
                nu6: Some(1),
                // see https://zips.z.cash/#nu6-1-candidate-zips for info on NU6.1
                nu6_1: None,
                nu7: None,
            },
        ),
        no_sync: false,
        no_db: false,
    };

    let zaino_db = ZainoDB::spawn(&config).await.unwrap();

    Ok((temp_dir, zaino_db))
}

async fn load_vectors_and_spawn_and_sync_zaino_db() -> (
    Vec<(u32, ChainBlock, CompactBlock)>,
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    TempDir,
    ZainoDB,
) {
    let (blocks, faucet, recipient) = load_test_vectors().unwrap();
    let (db_dir, zaino_db) = spawn_default_zaino_db().await.unwrap();
    for (_h, chain_block, _compact_block) in blocks.clone() {
        // dbg!("Writing block at height {}", _h);
        zaino_db.write_block(chain_block).await.unwrap();
    }
    (blocks, faucet, recipient, db_dir, zaino_db)
}

// *** ZainoDB Tests ***

#[tokio::test]
async fn vectors_can_be_loaded_and_deserialised() {
    let (blocks, faucet, recipient) = load_test_vectors().unwrap();

    // Chech block data..
    assert!(
        !blocks.is_empty(),
        "expected at least one block in test-vectors"
    );
    let mut prev_h: u32 = 0;
    for (h, chain_block, compact_block) in &blocks {
        // println!("Checking block at height {h}");

        assert_eq!(
            prev_h,
            (h - 1),
            "Chain height continuity check failed at height {h}"
        );
        prev_h = *h;

        let compact_block_hash = compact_block.hash.clone();
        let chain_block_hash_bytes = chain_block.hash().0.to_vec();

        assert_eq!(
            compact_block_hash, chain_block_hash_bytes,
            "Block hash check failed at height {h}"
        );

        // ChainBlock round trip check.
        let bytes = chain_block.to_bytes().unwrap();
        let reparsed = ChainBlock::from_bytes(&bytes).unwrap();
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
}

#[tokio::test]
async fn add_blocks_to_db_and_verify() {
    let (_blocks, _faucet, _recipient, _db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;
    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    dbg!(zaino_db.tip_height().await.unwrap());
}

#[tokio::test]
async fn delete_blocks_from_db() {
    let (_blocks, _faucet, _recipient, _db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;

    for h in (1..=200).rev() {
        // dbg!("Deleting block at height {}", h);
        zaino_db
            .delete_block_at_height(crate::Height(h))
            .await
            .unwrap();
    }

    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    dbg!(zaino_db.tip_height().await.unwrap());
}

#[tokio::test]
async fn load_db_from_file() {
    let (blocks, _faucet, _recipient) = load_test_vectors().unwrap();

    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();
    let config = BlockCacheConfig {
        map_capacity: None,
        map_shard_amount: None,
        db_path,
        db_size: None,
        network: zebra_chain::parameters::Network::new_regtest(
            zebra_chain::parameters::testnet::ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(1),
                sapling: Some(1),
                blossom: Some(1),
                heartwood: Some(1),
                canopy: Some(1),
                nu5: Some(1),
                nu6: Some(1),
                // see https://zips.z.cash/#nu6-1-candidate-zips for info on NU6.1
                nu6_1: None,
                nu7: None,
            },
        ),
        no_sync: false,
        no_db: false,
    };

    {
        let zaino_db = ZainoDB::spawn(&config).await.unwrap();
        for (_h, chain_block, _compact_block) in blocks.clone() {
            zaino_db.write_block(chain_block).await.unwrap();
        }

        zaino_db.wait_until_ready().await;
        dbg!(zaino_db.status().await);
        dbg!(zaino_db.tip_height().await.unwrap());

        dbg!(zaino_db.close().await.unwrap());
    }

    {
        let zaino_db_2 = ZainoDB::spawn(&config).await.unwrap();

        zaino_db_2.wait_until_ready().await;
        dbg!(zaino_db_2.status().await);
        let db_height = dbg!(zaino_db_2.tip_height().await.unwrap()).unwrap();

        assert_eq!(db_height.0, 200);

        dbg!(zaino_db_2.close().await.unwrap());
    }
}

#[tokio::test]
async fn try_write_invalid_block() {
    let (blocks, _faucet, _recipient, _db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;

    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    dbg!(zaino_db.tip_height().await.unwrap());

    let (height, mut chain_block, _compact_block) = blocks.last().unwrap().clone();

    chain_block.index.height = Some(crate::Height(height + 1));
    dbg!(chain_block.index.height);

    let db_err = dbg!(zaino_db.write_block(chain_block).await);

    // TODO: Update with concrete err type.
    assert!(db_err.is_err());

    dbg!(zaino_db.tip_height().await.unwrap());
}

#[tokio::test]
async fn try_delete_block_with_invalid_height() {
    let (blocks, _faucet, _recipient, _db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;

    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    dbg!(zaino_db.tip_height().await.unwrap());

    let (height, _chain_block, _compact_block) = blocks.last().unwrap().clone();

    let delete_height = height - 1;

    let db_err = dbg!(
        zaino_db
            .delete_block_at_height(crate::Height(delete_height))
            .await
    );

    // TODO: Update with concrete err type.
    assert!(db_err.is_err());

    dbg!(zaino_db.tip_height().await.unwrap());
}

#[tokio::test]
async fn create_db_reader() {
    let (_blocks, _faucet, _recipient, _db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;

    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    let db_height = dbg!(zaino_db.tip_height().await.unwrap()).unwrap();

    let db_reader = zaino_db.to_reader().await;

    let db_reader_height = dbg!(db_reader.tip_height().await.unwrap()).unwrap();

    assert_eq!(db_height, db_reader_height);
}

// *** DbReader Tests ***

// chain block
// compact block
// txids
// utxos
// balance
// spent

// #[tokio::test]
// async fn

// #[tokio::test]
// async fn

// #[tokio::test]
// async fn

// #[tokio::test]
// async fn
