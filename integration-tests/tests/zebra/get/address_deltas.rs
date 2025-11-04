use super::*;

// Test constants
const EXPECTED_TX_HEIGHT: u32 = 102;
const EXPECTED_CHAIN_TIP: u32 = 104;
const HEIGHT_BEYOND_TIP: u32 = 200;
const NON_EXISTENT_ADDRESS: &str = "tmVqEASZxBNKFTbmASZikGa5fPLkd68iJyx";

async fn setup_chain(test_manager: &mut TestManager) -> (String, String) {
    let mut clients = test_manager
        .clients
        .take()
        .expect("Clients are not initialized");
    let recipient_taddr = clients.get_recipient_address("transparent").await;
    let faucet_taddr = clients.get_faucet_address("transparent").await;

    clients.faucet.sync_and_await().await.unwrap();

    // Generate blocks and perform transaction
    test_manager.local_net.generate_blocks(100).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    clients.faucet.sync_and_await().await.unwrap();
    clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
    test_manager.local_net.generate_blocks(1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    clients.faucet.sync_and_await().await.unwrap();

    from_inputs::quick_send(
        &mut clients.faucet,
        vec![(recipient_taddr.as_str(), 250_000, None)],
    )
    .await
    .unwrap();
    test_manager.local_net.generate_blocks(1).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    clients.recipient.sync_and_await().await.unwrap();

    (recipient_taddr, faucet_taddr)
}

async fn test_simple_query(
    subscriber: &StateServiceSubscriber,
    recipient_taddr: &str,
) {
    let params = GetAddressDeltasParams::Address(recipient_taddr.to_string());
    let response = subscriber.get_address_deltas(params).await.unwrap();

    if let GetAddressDeltasResponse::Simple(address_deltas) = response {
        assert!(!address_deltas.is_empty(), "Expected at least one delta");
        let recipient_delta = address_deltas
            .iter()
            .find(|d| d.height >= EXPECTED_TX_HEIGHT)
            .expect("Should find recipient transaction delta");
        assert!(
            recipient_delta.height >= EXPECTED_TX_HEIGHT,
            "Transaction should be at expected height"
        );
        assert_eq!(recipient_delta.index, 0, "Expected output index 0");
    } else {
        panic!("Expected Simple variant");
    }
}

async fn test_filtered_start_zero(
    subscriber: &StateServiceSubscriber,
    recipient_taddr: &str,
    faucet_taddr: &str,
) {
    let start_height = 0;
    let end_height = EXPECTED_CHAIN_TIP;

    let params = GetAddressDeltasParams::Filtered {
        addresses: vec![recipient_taddr.to_string(), faucet_taddr.to_string()],
        start: start_height,
        end: end_height,
        chain_info: true,
    };
    let response = subscriber.get_address_deltas(params).await.unwrap();

    if let GetAddressDeltasResponse::Simple(address_deltas) = response {
        assert!(!address_deltas.is_empty(), "Expected deltas for both addresses");
        assert!(
            address_deltas.len() >= 2,
            "Expected deltas from multiple addresses"
        );
    } else {
        panic!("Expected Simple variant");
    }
}

async fn test_with_chaininfo(
    subscriber: &StateServiceSubscriber,
    recipient_taddr: &str,
    faucet_taddr: &str,
) {
    let start_height = 1;
    let end_height = EXPECTED_CHAIN_TIP;

    let params = GetAddressDeltasParams::Filtered {
        addresses: vec![recipient_taddr.to_string(), faucet_taddr.to_string()],
        start: start_height,
        end: end_height,
        chain_info: true,
    };
    let response = subscriber.get_address_deltas(params).await.unwrap();

    if let GetAddressDeltasResponse::WithChainInfo { deltas, start, end } = response {
        assert!(!deltas.is_empty(), "Expected deltas with chain info");
        assert_eq!(start.height, start_height, "Start block should match request");
        assert_eq!(end.height, end_height, "End block should match request");
        assert!(
            start.height < end.height,
            "Start height should be less than end height"
        );
    } else {
        panic!("Expected WithChainInfo variant");
    }
}

async fn test_height_clamping(
    subscriber: &StateServiceSubscriber,
    recipient_taddr: &str,
    faucet_taddr: &str,
) {
    let start_height = 1;
    let end_height = HEIGHT_BEYOND_TIP;

    let params = GetAddressDeltasParams::Filtered {
        addresses: vec![recipient_taddr.to_string(), faucet_taddr.to_string()],
        start: start_height,
        end: end_height,
        chain_info: true,
    };
    let response = subscriber.get_address_deltas(params).await.unwrap();

    if let GetAddressDeltasResponse::WithChainInfo { deltas, start, end } = response {
        assert!(!deltas.is_empty(), "Expected deltas with clamped range");
        assert_eq!(start.height, start_height, "Start should match request");
        assert!(
            end.height < end_height,
            "End height should be clamped below requested value"
        );
        assert!(
            end.height <= EXPECTED_CHAIN_TIP,
            "End height should not exceed chain tip region"
        );
    } else {
        panic!("Expected WithChainInfo variant");
    }
}

async fn test_non_existent_address(subscriber: &StateServiceSubscriber) {
    let start_height = 1;
    let end_height = HEIGHT_BEYOND_TIP;

    let params = GetAddressDeltasParams::Filtered {
        addresses: vec![NON_EXISTENT_ADDRESS.to_string()],
        start: start_height,
        end: end_height,
        chain_info: true,
    };
    let response = subscriber.get_address_deltas(params).await.unwrap();

    if let GetAddressDeltasResponse::WithChainInfo { deltas, start, end } = response {
        assert!(deltas.is_empty(), "Non-existent address should have no deltas");
        assert_eq!(start.height, start_height, "Start height should match request");
        assert!(end.height > 0, "End height should be set");
    } else {
        panic!("Expected WithChainInfo variant");
    }
}

pub(super) async fn main() {
    let (
        mut test_manager,
        _fetch_service,
        _fetch_service_subscriber,
        _state_service,
        state_service_subscriber,
    ) = super::create_test_manager_and_services(
        &ValidatorKind::Zebrad,
        None,
        true,
        true,
        None,
    )
    .await;

    let (recipient_taddr, faucet_taddr) = setup_chain(&mut test_manager).await;

    // ============================================================
    // Test 1: Simple address query (single address, no filters)
    // ============================================================
    test_simple_query(&state_service_subscriber, &recipient_taddr).await;

    // ============================================================
    // Test 2: Filtered query with start=0 (should return Simple variant)
    // ============================================================
    test_filtered_start_zero(
        &state_service_subscriber,
        &recipient_taddr,
        &faucet_taddr,
    )
    .await;

    // ============================================================
    // Test 3: Filtered query with start>0 and chain_info=true
    // ============================================================
    test_with_chaininfo(&state_service_subscriber, &recipient_taddr, &faucet_taddr)
        .await;

    // ============================================================
    // Test 4: Height clamping (end beyond chain tip)
    // ============================================================
    test_height_clamping(&state_service_subscriber, &recipient_taddr, &faucet_taddr)
        .await;

    // ============================================================
    // Test 5: Non-existent address (should return empty deltas)
    // ============================================================
    test_non_existent_address(&state_service_subscriber).await;

    test_manager.close().await;
}
