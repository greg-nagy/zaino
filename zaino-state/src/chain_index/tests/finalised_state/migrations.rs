//! Holds database migration tests.

use std::path::PathBuf;
use tempfile::TempDir;

use crate::bench::BlockCacheConfig;
use crate::chain_index::finalised_state::ZainoDB;
use crate::chain_index::tests::init_tracing;
use crate::chain_index::tests::vectors::{build_mockchain_source, load_test_vectors};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_to_v1_full() {
    init_tracing();

    let (blocks, _faucet, _recipient) = load_test_vectors().unwrap();

    let temp_dir: TempDir = tempfile::tempdir().unwrap();
    let db_path: PathBuf = temp_dir.path().to_path_buf();
    let db_path_clone = db_path.clone();

    let v0_config = BlockCacheConfig {
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

    let v1_config = BlockCacheConfig {
        map_capacity: None,
        map_shard_amount: None,
        db_version: 1,
        db_path: db_path_clone,
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
    let v0_config_clone = v0_config.clone();

    // Build v0 database.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let zaino_db = ZainoDB::spawn(v0_config_clone, source).await.unwrap();
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

    // Open v1 database and check migration.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            dbg!(v0_config.db_path.read_dir().unwrap().collect::<Vec<_>>());
            let zaino_db_2 = ZainoDB::spawn(v1_config, source_clone).await.unwrap();

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
