//! Holds tests for the V0 database.

use std::path::PathBuf;
use tempfile::TempDir;

use zaino_proto::proto::compact_formats::CompactBlock;
use zebra_rpc::methods::GetAddressUtxos;

use crate::bench::BlockCacheConfig;
use crate::chain_index::finalised_state::reader::DbReader;
use crate::chain_index::finalised_state::ZainoDB;
use crate::chain_index::source::test::MockchainSource;
use crate::chain_index::tests::vectors::{build_mockchain_source, load_test_vectors};
use crate::error::FinalisedStateError;
use crate::{ChainBlock, Height};

async fn spawn_v0_zaino_db(
    source: MockchainSource,
) -> Result<(TempDir, ZainoDB), FinalisedStateError> {
    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();

    let config = BlockCacheConfig {
        map_capacity: None,
        map_shard_amount: None,
        db_version: 0,
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

    let zaino_db = ZainoDB::spawn(config, source).await.unwrap();

    Ok((temp_dir, zaino_db))
}

async fn load_vectors_and_spawn_and_sync_zaino_db() -> (
    Vec<(
        u32,
        ChainBlock,
        CompactBlock,
        zebra_chain::block::Block,
        (
            zebra_chain::sapling::tree::Root,
            u64,
            zebra_chain::orchard::tree::Root,
            u64,
        ),
    )>,
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    TempDir,
    ZainoDB,
) {
    let (blocks, faucet, recipient) = load_test_vectors().unwrap();

    let source = build_mockchain_source(blocks.clone());

    let (db_dir, zaino_db) = spawn_v0_zaino_db(source).await.unwrap();
    for (_h, chain_block, _compact_block, _zebra_block, _block_roots) in blocks.clone() {
        // dbg!("Writing block at height {}", _h);
        // if _h == 1 {
        //     dbg!(&chain_block);
        // }
        zaino_db.write_block(chain_block).await.unwrap();
    }
    (blocks, faucet, recipient, db_dir, zaino_db)
}

async fn load_vectors_db_and_reader() -> (
    Vec<(
        u32,
        ChainBlock,
        CompactBlock,
        zebra_chain::block::Block,
        (
            zebra_chain::sapling::tree::Root,
            u64,
            zebra_chain::orchard::tree::Root,
            u64,
        ),
    )>,
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    (Vec<String>, Vec<GetAddressUtxos>, u64),
    TempDir,
    std::sync::Arc<ZainoDB>,
    DbReader,
) {
    let (blocks, faucet, recipient, db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;

    let zaino_db = std::sync::Arc::new(zaino_db);

    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    dbg!(zaino_db.db_height().await.unwrap()).unwrap();

    let db_reader = zaino_db.to_reader();
    dbg!(db_reader.db_height().await.unwrap()).unwrap();

    (blocks, faucet, recipient, db_dir, zaino_db, db_reader)
}

// *** ZainoDB Tests ***

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_blocks_to_db_and_verify() {
    let (_blocks, _faucet, _recipient, _db_dir, zaino_db) =
        load_vectors_and_spawn_and_sync_zaino_db().await;
    zaino_db.wait_until_ready().await;
    dbg!(zaino_db.status().await);
    dbg!(zaino_db.db_height().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
    dbg!(zaino_db.db_height().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_db_from_file() {
    let (blocks, _faucet, _recipient) = load_test_vectors().unwrap();

    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();
    let config = BlockCacheConfig {
        map_capacity: None,
        map_shard_amount: None,
        db_version: 1,
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

    let source = build_mockchain_source(blocks.clone());
    let source_clone = source.clone();

    let blocks_clone = blocks.clone();
    let config_clone = config.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let zaino_db = ZainoDB::spawn(config_clone, source).await.unwrap();
            for (_h, chain_block, _compact_block, _zebra_block, _block_roots) in blocks_clone {
                zaino_db.write_block(chain_block).await.unwrap();
            }

            zaino_db.wait_until_ready().await;
            dbg!(zaino_db.status().await);
            dbg!(zaino_db.db_height().await.unwrap());

            dbg!(zaino_db.shutdown().await.unwrap());
        });
    })
    .join()
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(1000));

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            dbg!(config.db_path.read_dir().unwrap().collect::<Vec<_>>());
            let zaino_db_2 = ZainoDB::spawn(config, source_clone).await.unwrap();

            zaino_db_2.wait_until_ready().await;
            dbg!(zaino_db_2.status().await);
            let db_height = dbg!(zaino_db_2.db_height().await.unwrap()).unwrap();

            assert_eq!(db_height.0, 200);

            dbg!(zaino_db_2.shutdown().await.unwrap());
        });
    })
    .join()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_db_reader() {
    let (blocks, _faucet, _recipient, _db_dir, zaino_db, db_reader) =
        load_vectors_db_and_reader().await;

    let (data_height, _, _, _, _) = blocks.last().unwrap();
    let db_height = dbg!(zaino_db.db_height().await.unwrap()).unwrap();
    let db_reader_height = dbg!(db_reader.db_height().await.unwrap()).unwrap();

    assert_eq!(data_height, &db_height.0);
    assert_eq!(db_height, db_reader_height);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_compact_blocks() {
    let (blocks, _faucet, _recipient, _db_dir, _zaino_db, db_reader) =
        load_vectors_db_and_reader().await;

    for (height, _, compact_block, _, _) in blocks.iter() {
        let reader_compact_block = db_reader.get_compact_block(Height(*height)).await.unwrap();
        assert_eq!(compact_block, &reader_compact_block);
        println!("CompactBlock at height {height} OK");
    }
}
