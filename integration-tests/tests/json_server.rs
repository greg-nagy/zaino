use futures::StreamExt as _;
use zaino_fetch::jsonrpsee::connector::{test_node_and_return_url, JsonRpSeeConnector};
use zaino_proto::proto::service::{
    AddressList, BlockId, BlockRange, Exclude, GetAddressUtxosArg, GetSubtreeRootsArg,
    TransparentAddressBlockFilter, TxFilter,
};
use zaino_state::{
    config::FetchServiceConfig,
    fetch::{FetchService, FetchServiceSubscriber},
    indexer::{LightWalletIndexer, ZcashIndexer as _, ZcashService as _},
    status::StatusType,
};
use zaino_testutils::Validator as _;
use zaino_testutils::{TestManager, ValidatorKind};
use zebra_chain::{parameters::Network, subtree::NoteCommitmentSubtreeIndex};
use zebra_rpc::methods::{AddressStrings, GetAddressTxIdsRequest};

async fn create_test_manager_and_fetch_services(
    enable_cookie_auth: bool,
) -> (
    TestManager,
    FetchService,
    FetchServiceSubscriber,
    FetchService,
    FetchServiceSubscriber,
) {
    let test_manager = TestManager::launch(
        &ValidatorKind::Zcashd,
        None,
        None,
        true,
        true,
        enable_cookie_auth,
        true,
        true,
        false,
    )
    .await
    .unwrap();

    let zcashd_fetch_service = FetchService::spawn(FetchServiceConfig::new(
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
        Network::new_regtest(Some(1), Some(1)),
        true,
        true,
    ))
    .await
    .unwrap();
    let zcashd_subscriber = zcashd_fetch_service.get_subscriber().inner();

    let zaino_fetch_service = FetchService::spawn(FetchServiceConfig::new(
        test_manager.zebrad_rpc_listen_address,
        enable_cookie_auth,
        test_manager
            .json_server_cookie_dir
            .clone()
            .map(|p| p.to_string_lossy().into_owned()),
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
        Network::new_regtest(Some(1), Some(1)),
        true,
        true,
    ))
    .await
    .unwrap();
    let zaino_subscriber = zcashd_fetch_service.get_subscriber().inner();

    (
        test_manager,
        zcashd_fetch_service,
        zcashd_subscriber,
        zaino_fetch_service,
        zaino_subscriber,
    )
}
