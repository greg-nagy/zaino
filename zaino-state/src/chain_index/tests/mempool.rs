//! Zaino-State ChainIndex Mempool unit tests.

use std::{io::Cursor, str::FromStr as _};

use zebra_chain::serialization::ZcashDeserialize as _;

use crate::{
    chain_index::{
        mempool::MempoolSubscriber,
        source::test::MockchainSource,
        tests::vectors::{build_active_mockchain_source, load_test_vectors},
    },
    Mempool, MempoolKey, MempoolValue,
};

async fn spawn_mempool_and_mockchain() -> (
    Mempool<MockchainSource>,
    MempoolSubscriber,
    MockchainSource,
    Vec<zebra_chain::block::Block>,
) {
    let (blocks, _faucet, _recipient) = load_test_vectors().unwrap();

    let mockchain = build_active_mockchain_source(blocks.clone());

    let mempool = Mempool::spawn(mockchain.clone(), None).await.unwrap();

    let subscriber = mempool.subscriber();

    let block_data = blocks
        .iter()
        .map(|(_height, _chain_block, _compact_block, zebra_block, _roots)| zebra_block.clone())
        .collect();

    (mempool, subscriber, mockchain, block_data)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_mempool() {
    let (_mempool, subscriber, mockchain, block_data) = spawn_mempool_and_mockchain().await;

    let mut active_chain_height = dbg!(mockchain.active_height());
    assert_eq!(active_chain_height, 0);
    let max_chain_height = mockchain.max_chain_height();

    for _ in 0..=max_chain_height {
        let mempool_index = (active_chain_height as usize) + 1;
        let mempool_transactions = block_data
            .get(mempool_index)
            .map(|b| b.transactions.clone())
            .unwrap_or_default();
        for transaction in mempool_transactions.clone().into_iter() {
            dbg!(&transaction.hash());
        }

        let subscriber_tx = subscriber.get_mempool().await;
        for (hash, _tx) in subscriber_tx.iter() {
            dbg!(&hash.0);
        }

        for transaction in mempool_transactions.into_iter() {
            let transaction_hash = dbg!(transaction.hash());

            let (subscriber_tx_hash, subscriber_tx) = subscriber_tx
                .iter()
                .find(|(k, _)| k.0 == transaction_hash.to_string())
                .map(|(MempoolKey(h), MempoolValue(tx))| {
                    (
                        zebra_chain::transaction::Hash::from_str(h).unwrap(),
                        tx.clone(),
                    )
                })
                .unwrap();
            dbg!(&subscriber_tx_hash);

            let subscriber_transaction = zebra_chain::transaction::Transaction::zcash_deserialize(
                Cursor::new(subscriber_tx.as_ref()),
            )
            .unwrap();

            assert_eq!(transaction_hash, subscriber_tx_hash);
            assert_eq!(*transaction, subscriber_transaction);
        }

        if active_chain_height < max_chain_height {
            mockchain.mine_blocks(10);
            active_chain_height = dbg!(mockchain.active_height());

            std::thread::sleep(std::time::Duration::from_millis(2000));
        }
    }
}

// get_filtered_mempool
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_filtered_mempool() {
    let (_mempool, subscriber, mockchain, block_data) = spawn_mempool_and_mockchain().await;

    mockchain.mine_blocks(150);
    let active_chain_height = dbg!(mockchain.active_height());

    std::thread::sleep(std::time::Duration::from_millis(2000));

    let mempool_index = (active_chain_height as usize) + 1;
    let mempool_transactions = block_data
        .get(mempool_index)
        .map(|b| b.transactions.clone())
        .unwrap_or_default();
    for transaction in mempool_transactions.clone().into_iter() {
        dbg!(&transaction.hash());
    }

    let exclude_hash = mempool_transactions[0].hash();
    // Reverse format to client type.
    let client_exclude_txid: String = exclude_hash
        .to_string()
        .chars()
        .collect::<Vec<_>>()
        .chunks(2)
        .rev()
        .map(|chunk| chunk.iter().collect::<String>())
        .collect();
    dbg!(&client_exclude_txid);

    let subscriber_tx = subscriber
        .get_filtered_mempool(vec![client_exclude_txid])
        .await;
    for (hash, _tx) in subscriber_tx.iter() {
        dbg!(&hash.0);
    }

    println!("Checking transactions..");

    for transaction in mempool_transactions.into_iter() {
        let transaction_hash = dbg!(transaction.hash());
        if transaction_hash == exclude_hash {
            // check tx is *not* in mempool transactions
            let maybe_subscriber_tx = subscriber_tx
                .iter()
                .find(|(k, _)| k.0 == transaction_hash.to_string())
                .map(|(MempoolKey(h), MempoolValue(tx))| {
                    (
                        zebra_chain::transaction::Hash::from_str(h).unwrap(),
                        tx.clone(),
                    )
                });

            assert!(maybe_subscriber_tx.is_none());
        } else {
            let (subscriber_tx_hash, subscriber_tx) = subscriber_tx
                .iter()
                .find(|(k, _)| k.0 == transaction_hash.to_string())
                .map(|(MempoolKey(h), MempoolValue(tx))| {
                    (
                        zebra_chain::transaction::Hash::from_str(h).unwrap(),
                        tx.clone(),
                    )
                })
                .unwrap();
            dbg!(&subscriber_tx_hash);

            let subscriber_transaction = zebra_chain::transaction::Transaction::zcash_deserialize(
                Cursor::new(subscriber_tx.as_ref()),
            )
            .unwrap();

            assert_eq!(transaction_hash, subscriber_tx_hash);
            assert_eq!(*transaction, subscriber_transaction);
        }
    }
}

// get_mempool_stream

// get_mempool_transaction

// get_mempool_info
