use zaino_state::{
    bench::chain_index::non_finalised_state::{BlockchainSource, NonFinalizedState},
    BackendType, StateService, StateServiceConfig, ZcashService as _,
};
use zaino_testutils::{TestManager, Validator as _, ValidatorKind};

async fn create_test_manager_and_nfs(
    validator: &ValidatorKind,
    chain_cache: Option<std::path::PathBuf>,
    enable_zaino: bool,
    zaino_no_sync: bool,
    zaino_no_db: bool,
    enable_clients: bool,
) -> (
    TestManager,
    StateService,
    zaino_state::bench::chain_index::non_finalised_state::NonFinalizedState,
) {
    let test_manager = TestManager::launch(
        validator,
        &BackendType::Fetch,
        None,
        chain_cache.clone(),
        enable_zaino,
        false,
        false,
        zaino_no_sync,
        zaino_no_db,
        enable_clients,
    )
    .await
    .unwrap();

    let state_chain_cache_dir = match chain_cache {
        Some(dir) => dir,
        None => test_manager.data_dir.clone(),
    };
    let network = match test_manager.network.to_string().as_str() {
        "Regtest" => zebra_chain::parameters::Network::new_regtest(
            zebra_chain::parameters::testnet::ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(1),
                sapling: Some(1),
                blossom: Some(1),
                heartwood: Some(1),
                canopy: Some(1),
                nu5: Some(1),
                nu6: Some(1),
                // TODO: What is network upgrade 6.1? What does a minor version NU mean?
                nu6_1: None,
                nu7: None,
            },
        ),
        "Testnet" => zebra_chain::parameters::Network::new_default_testnet(),
        "Mainnet" => zebra_chain::parameters::Network::Mainnet,
        _ => panic!("Incorrect newtork type found."),
    };
    let state_service = StateService::spawn(StateServiceConfig::new(
        zebra_state::Config {
            cache_dir: state_chain_cache_dir,
            ephemeral: false,
            delete_old_database: true,
            debug_stop_at_height: None,
            debug_validity_check_interval: None,
        },
        test_manager.zebrad_rpc_listen_address,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        test_manager
            .local_net
            .data_dir()
            .path()
            .to_path_buf()
            .join("zaino"),
        None,
        network.clone(),
        true,
        true,
    ))
    .await
    .unwrap();

    let non_finalized_state = NonFinalizedState::initialize(
        BlockchainSource::State(state_service.read_state_service().clone()),
        network,
    )
    .await
    .unwrap();

    (test_manager, state_service, non_finalized_state)
}

#[tokio::test]
async fn nfs_simple_sync() {
    let (test_manager, _json_service, non_finalized_state) =
        create_test_manager_and_nfs(&ValidatorKind::Zebrad, None, true, false, false, true).await;

    let snapshot = non_finalized_state.get_snapshot();
    assert_eq!(
        snapshot.best_tip.0,
        zaino_state::Height::try_from(1).unwrap()
    );

    test_manager.generate_blocks_with_delay(5).await;
    non_finalized_state.sync().await.unwrap();
    let snapshot = non_finalized_state.get_snapshot();
    assert_eq!(
        snapshot.best_tip.0,
        zaino_state::Height::try_from(6).unwrap()
    );
}
