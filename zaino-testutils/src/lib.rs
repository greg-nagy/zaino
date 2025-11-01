//! Zaino Testing Utilities.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

/// Convenience reexport of zaino_testvectors
pub mod test_vectors {
    pub use zaino_testvectors::*;
}

use once_cell::sync::Lazy;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};
use tracing_subscriber::EnvFilter;
use zaino_common::{
    network::ActivationHeights, validator::ValidatorConfig, CacheConfig, DatabaseConfig, Network,
    ServiceConfig, StorageConfig,
};
use zaino_serve::server::config::{GrpcServerConfig, JsonRpcServerConfig};
use zaino_state::BackendType;
pub use zcash_local_net as services;
use zcash_local_net::validator::zcashd::ZcashdConfig;
use zcash_local_net::validator::Validator;
use zebra_chain::parameters::NetworkKind;
use zingo_test_vectors::seeds;
pub use zingolib::get_base_address_macro;
pub use zingolib::lightclient::LightClient;
pub use zingolib::testutils::lightclient::from_inputs;
use zingolib::testutils::scenarios::ClientBuilder;

/// Helper to get the test binary path from the TEST_BINARIES_DIR env var.
fn binary_path(binary_name: &str) -> Option<PathBuf> {
    std::env::var("TEST_BINARIES_DIR")
        .ok()
        .map(|dir| PathBuf::from(dir).join(binary_name))
}

fn make_uri(indexer_port: portpicker::Port) -> http::Uri {
    format!("http://127.0.0.1:{indexer_port}")
        .try_into()
        .unwrap()
}

// temporary until activation heights are unified to zebra-chain type.
// from/into impls not added in zaino-common to avoid unecessary addition of zcash-protocol dep to non-test code
fn local_network_from_activation_heights(
    activation_heights: ActivationHeights,
) -> zcash_protocol::local_consensus::LocalNetwork {
    zcash_protocol::local_consensus::LocalNetwork {
        overwinter: activation_heights
            .overwinter
            .map(zcash_protocol::consensus::BlockHeight::from),
        sapling: activation_heights
            .sapling
            .map(zcash_protocol::consensus::BlockHeight::from),
        blossom: activation_heights
            .blossom
            .map(zcash_protocol::consensus::BlockHeight::from),
        heartwood: activation_heights
            .heartwood
            .map(zcash_protocol::consensus::BlockHeight::from),
        canopy: activation_heights
            .canopy
            .map(zcash_protocol::consensus::BlockHeight::from),
        nu5: activation_heights
            .nu5
            .map(zcash_protocol::consensus::BlockHeight::from),
        nu6: activation_heights
            .nu6
            .map(zcash_protocol::consensus::BlockHeight::from),
        nu6_1: activation_heights
            .nu6_1
            .map(zcash_protocol::consensus::BlockHeight::from),
    }
}

/// Path for zcashd binary.
pub static ZCASHD_BIN: Lazy<Option<PathBuf>> = Lazy::new(|| binary_path("zcashd"));

/// Path for zcash-cli binary.
pub static ZCASH_CLI_BIN: Lazy<Option<PathBuf>> = Lazy::new(|| binary_path("zcash-cli"));

/// Path for zebrad binary.
pub static ZEBRAD_BIN: Lazy<Option<PathBuf>> = Lazy::new(|| binary_path("zebrad"));

/// Path for lightwalletd binary.
pub static LIGHTWALLETD_BIN: Lazy<Option<PathBuf>> = Lazy::new(|| binary_path("lightwalletd"));

/// Path for zainod binary.
pub static ZAINOD_BIN: Lazy<Option<PathBuf>> = Lazy::new(|| binary_path("zainod"));

/// Path for zcashd chain cache.
pub static ZCASHD_CHAIN_CACHE_DIR: Lazy<Option<PathBuf>> = Lazy::new(|| {
    let mut workspace_root_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    workspace_root_path.pop();
    Some(workspace_root_path.join("integration-tests/chain_cache/client_rpc_tests"))
});

/// Path for zebrad chain cache.
pub static ZEBRAD_CHAIN_CACHE_DIR: Lazy<Option<PathBuf>> = Lazy::new(|| {
    let mut workspace_root_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    workspace_root_path.pop();
    Some(workspace_root_path.join("integration-tests/chain_cache/client_rpc_tests_large"))
});

/// Path for the Zebra chain cache in the user's home directory.
pub static ZEBRAD_TESTNET_CACHE_DIR: Lazy<Option<PathBuf>> = Lazy::new(|| {
    let home_path = PathBuf::from(std::env::var("HOME").unwrap());
    Some(home_path.join(".cache/zebra"))
});

#[derive(PartialEq, Clone, Copy)]
/// Represents the type of validator to launch.
pub enum ValidatorKind {
    /// Zcashd.
    Zcashd,
    /// Zebrad.
    Zebrad,
}

/// Config for validators.
pub enum ValidatorTestConfig {
    /// Zcashd Config.
    ZcashdConfig(ZcashdConfig),
    /// Zebrad Config.
    ZebradConfig(zcash_local_net::validator::zebrad::ZebradConfig),
}

/// Holds zingo lightclients along with the lightclient builder for wallet-2-validator tests.
pub struct Clients {
    /// Lightclient builder.
    pub client_builder: ClientBuilder,
    /// Faucet (zingolib lightclient).
    ///
    /// Mining rewards are received by this client for use in tests.
    pub faucet: zingolib::lightclient::LightClient,
    /// Recipient (zingolib lightclient).
    pub recipient: zingolib::lightclient::LightClient,
}

impl Clients {
    /// Returns the zcash address of the faucet.
    pub async fn get_faucet_address(&self, pool: &str) -> String {
        zingolib::get_base_address_macro!(self.faucet, pool)
    }

    /// Returns the zcash address of the recipient.
    pub async fn get_recipient_address(&self, pool: &str) -> String {
        zingolib::get_base_address_macro!(self.recipient, pool)
    }
}

/// Configuration data for Zingo-Indexer Tests.
pub struct TestManager<C: Validator> {
    /// Control plane for a validator
    pub local_net: C,
    /// Data directory for the validator.
    pub data_dir: PathBuf,
    /// Network (chain) type:
    pub network: NetworkKind,
    /// Zebrad/Zcashd JsonRpc listen address.
    pub full_node_rpc_listen_address: SocketAddr,
    /// Zebrad/Zcashd gRpc listen address.
    pub full_node_grpc_listen_address: SocketAddr,
    /// Zaino Indexer JoinHandle.
    pub zaino_handle: Option<tokio::task::JoinHandle<Result<(), zainodlib::error::IndexerError>>>,
    /// Zaino JsonRPC listen address.
    pub zaino_json_rpc_listen_address: Option<SocketAddr>,
    /// Zaino gRPC listen address.
    pub zaino_grpc_listen_address: Option<SocketAddr>,
    /// JsonRPC server cookie dir.
    pub json_server_cookie_dir: Option<PathBuf>,
    /// Zingolib lightclients.
    pub clients: Option<Clients>,
}

impl<C: Validator> TestManager<C> {
    /// Launches zcash-local-net<Empty, Validator>.
    ///
    /// Possible validators: Zcashd, Zebrad.
    ///
    /// If chain_cache is given a path the chain will be loaded.
    ///
    /// If clients is set to active zingolib lightclients will be created for test use.
    ///
    /// TODO: Add TestManagerConfig struct and constructor methods of common test setups.
    pub async fn launch(
        validator: &ValidatorKind,
        backend: &BackendType,
        network: Option<NetworkKind>,
        activation_heights: Option<ActivationHeights>,
        chain_cache: Option<PathBuf>,
        enable_zaino: bool,
        enable_zaino_jsonrpc_server: bool,
        enable_clients: bool,
    ) -> Result<Self, std::io::Error> {
        if (validator == &ValidatorKind::Zcashd) && (backend == &BackendType::State) {
            return Err(std::io::Error::other(
                "Cannot use state backend with zcashd.",
            ));
        }
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_target(true)
            .try_init();

        let activation_heights = activation_heights.unwrap_or_default();
        let network_kind = network.unwrap_or(NetworkKind::Regtest);
        let zaino_network_kind =
            Network::from_network_kind_and_activation_heights(&network_kind, &activation_heights);

        if enable_clients && !enable_zaino {
            return Err(std::io::Error::other(
                "Cannot enable clients when zaino is not enabled.",
            ));
        }

        // Launch LocalNet:
        let grpc_listen_port = portpicker::pick_unused_port().expect("No ports free");
        let full_node_grpc_listen_address =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), grpc_listen_port);
        let local_net = C::launch_default()
            .await
            .expect("to launch a default validator");
        let rpc_listen_port = local_net.get_port();
        let full_node_rpc_listen_address =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), rpc_listen_port);

        let data_dir = local_net.data_dir().path().to_path_buf();
        let zaino_db_path = data_dir.join("zaino");

        let zebra_db_path = match chain_cache {
            Some(cache) => cache,
            None => data_dir.clone(),
        };

        // Launch Zaino:
        let (
            zaino_grpc_listen_address,
            zaino_json_listen_address,
            zaino_json_server_cookie_dir,
            zaino_handle,
        ) = if enable_zaino {
            let zaino_grpc_listen_port = portpicker::pick_unused_port().expect("No ports free");
            let zaino_grpc_listen_address =
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), zaino_grpc_listen_port);
            let zaino_json_listen_port = portpicker::pick_unused_port().expect("No ports free");
            let zaino_json_listen_address =
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), zaino_json_listen_port);
            let zaino_json_server_cookie_dir: Option<PathBuf> = None;
            let indexer_config = zainodlib::config::ZainodConfig {
                // TODO: Make configurable.
                backend: *backend,
                json_server_settings: if enable_zaino_jsonrpc_server {
                    Some(JsonRpcServerConfig {
                        json_rpc_listen_address: zaino_json_listen_address,
                        cookie_dir: zaino_json_server_cookie_dir.clone(),
                    })
                } else {
                    None
                },
                grpc_settings: GrpcServerConfig {
                    listen_address: zaino_grpc_listen_address,
                    tls: None,
                },
                validator_settings: ValidatorConfig {
                    validator_jsonrpc_listen_address: full_node_rpc_listen_address,
                    validator_grpc_listen_address: full_node_grpc_listen_address,
                    validator_cookie_path: None,
                    validator_user: Some("xxxxxx".to_string()),
                    validator_password: Some("xxxxxx".to_string()),
                },
                service: ServiceConfig::default(),
                storage: StorageConfig {
                    cache: CacheConfig::default(),
                    database: DatabaseConfig {
                        path: zaino_db_path,
                        ..Default::default()
                    },
                },
                zebra_db_path,
                network: zaino_network_kind,
            };
            let handle = zainodlib::indexer::spawn_indexer(indexer_config)
                .await
                .unwrap();

            // NOTE: This is required to give the server time to launch, this is not used in production code but could be rewritten to improve testing efficiency.
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            (
                Some(zaino_grpc_listen_address),
                Some(zaino_json_listen_address),
                zaino_json_server_cookie_dir,
                Some(handle),
            )
        } else {
            (None, None, None, None)
        };
        // Launch Zingolib Lightclients:
        let clients = if enable_clients {
            let mut client_builder = ClientBuilder::new(
                make_uri(
                    zaino_grpc_listen_address
                        .expect("Error launching zingo lightclients. `enable_zaino` is None.")
                        .port(),
                ),
                tempfile::tempdir().unwrap(),
            );
            let faucet = client_builder.build_faucet(
                true,
                local_network_from_activation_heights(activation_heights),
            );
            let recipient = client_builder.build_client(
                seeds::HOSPITAL_MUSEUM_SEED.to_string(),
                1,
                true,
                local_network_from_activation_heights(activation_heights),
            );
            Some(Clients {
                client_builder,
                faucet,
                recipient,
            })
        } else {
            None
        };
        let test_manager = Self {
            local_net,
            data_dir,
            network: network_kind,
            full_node_rpc_listen_address,
            full_node_grpc_listen_address,
            zaino_handle,
            zaino_json_rpc_listen_address: zaino_json_listen_address,
            zaino_grpc_listen_address,
            json_server_cookie_dir: zaino_json_server_cookie_dir,
            clients,
        };

        // Generate an extra block to turn on NU5 and NU6,
        // as they currently must be turned on at block height = 2.
        // Generates `blocks` regtest blocks.
        test_manager
            .local_net
            .generate_blocks_with_delay(1)
            .await
            .expect("indexer to match validator");
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        Ok(test_manager)
    }

    /// Generates `blocks` regtest blocks.
    /// Adds a delay between blocks to allow zaino / zebra to catch up with test.
    pub async fn generate_blocks_with_delay(&self, blocks: u32) {
        for _ in 0..blocks {
            self.local_net.generate_blocks(1).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }
    }

    /// Closes the TestManager.
    pub async fn close(&mut self) {
        if let Some(handle) = self.zaino_handle.take() {
            handle.abort();
        }
    }
}

impl<C: Validator> Drop for TestManager<C> {
    fn drop(&mut self) {
        if let Some(handle) = &self.zaino_handle {
            handle.abort();
        };
    }
}

#[cfg(test)]
mod launch_testmanager {

    use zaino_common::network::ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS;
    use zcash_client_backend::proto::service::compact_tx_streamer_client::CompactTxStreamerClient;
    use zingo_netutils::{GetClientError, GrpcConnector, UnderlyingService};

    use super::*;

    /// Builds a client for creating RPC requests to the indexer/light-node
    async fn build_client(
        uri: http::Uri,
    ) -> Result<CompactTxStreamerClient<UnderlyingService>, GetClientError> {
        GrpcConnector::new(uri).get_client().await
    }

    mod zcashd {

        use zcash_local_net::validator::zcashd::Zcashd;

        use super::*;

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn basic() {
            let mut test_manager = TestManager::<Zcashd>::launch(
                &ValidatorKind::Zcashd,
                &BackendType::Fetch,
                None,
                Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                None,
                false,
                false,
                false,
            )
            .await
            .unwrap();
            assert_eq!(2, (test_manager.local_net.get_chain_height().await));
            test_manager.close().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn generate_blocks() {
            let mut test_manager = TestManager::<Zcashd>::launch(
                &ValidatorKind::Zcashd,
                &BackendType::Fetch,
                None,
                Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                None,
                false,
                false,
                false,
            )
            .await
            .unwrap();
            assert_eq!(2, (test_manager.local_net.get_chain_height().await));
            test_manager.local_net.generate_blocks(1).await.unwrap();
            assert_eq!(3, (test_manager.local_net.get_chain_height().await));
            test_manager.close().await;
        }

        #[ignore = "chain cache needs development"]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn with_chain() {
            let mut test_manager = TestManager::<Zcashd>::launch(
                &ValidatorKind::Zcashd,
                &BackendType::Fetch,
                None,
                Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                ZCASHD_CHAIN_CACHE_DIR.clone(),
                false,
                false,
                false,
            )
            .await
            .unwrap();
            assert_eq!(10, (test_manager.local_net.get_chain_height().await));
            test_manager.close().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn zaino() {
            let mut test_manager = TestManager::<Zcashd>::launch(
                &ValidatorKind::Zcashd,
                &BackendType::Fetch,
                None,
                Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                None,
                true,
                false,
                false,
            )
            .await
            .unwrap();
            let _grpc_client = build_client(services::network::localhost_uri(
                test_manager
                    .zaino_grpc_listen_address
                    .expect("Zaino listen port is not available but zaino is active.")
                    .port(),
            ))
            .await
            .unwrap();
            test_manager.close().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn zaino_clients() {
            let mut test_manager = TestManager::<Zcashd>::launch(
                &ValidatorKind::Zcashd,
                &BackendType::Fetch,
                None,
                Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                None,
                true,
                false,
                true,
            )
            .await
            .unwrap();
            let clients = test_manager
                .clients
                .as_ref()
                .expect("Clients are not initialized");
            dbg!(clients.faucet.do_info().await);
            dbg!(clients.recipient.do_info().await);
            test_manager.close().await;
        }

        /// This test shows nothing about zebrad.
        /// This is not the case with Zcashd and should not be the case here.
        /// Even if rewards need 100 confirmations these blocks should not have to be mined at the same time.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        pub(crate) async fn zaino_clients_receive_mining_reward() {
            let mut test_manager = TestManager::<Zcashd>::launch(
                &ValidatorKind::Zcashd,
                &BackendType::Fetch,
                None,
                Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                None,
                true,
                false,
                true,
            )
            .await
            .unwrap();
            let mut clients = test_manager
                .clients
                .take()
                .expect("Clients are not initialized");

            clients.faucet.sync_and_await().await.unwrap();
            dbg!(clients
                .faucet
                .account_balance(zip32::AccountId::ZERO)
                .await
                .unwrap());

            assert!(
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0
                        || clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64() > 0,
                    "No mining reward received from Zcashd. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
                );

            test_manager.close().await;
        }
    }

    mod zebrad {

        use super::*;

        mod fetch_service {

            use zcash_local_net::validator::zebrad::Zebrad;
            use zip32::AccountId;

            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn basic() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    false,
                    false,
                    false,
                )
                .await
                .unwrap();
                assert_eq!(
                    2,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn generate_blocks() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    false,
                    false,
                    false,
                )
                .await
                .unwrap();
                assert_eq!(
                    2,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.local_net.generate_blocks(1).await.unwrap();
                assert_eq!(3, (test_manager.local_net.get_chain_height().await));
                test_manager.close().await;
            }

            #[ignore = "chain cache needs development"]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn with_chain() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    ZEBRAD_CHAIN_CACHE_DIR.clone(),
                    false,
                    false,
                    false,
                )
                .await
                .unwrap();
                assert_eq!(
                    52,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let _grpc_client = build_client(services::network::localhost_uri(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("Zaino listen port not available but zaino is active.")
                        .port(),
                ))
                .await
                .unwrap();
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_clients() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();
                let clients = test_manager
                    .clients
                    .as_ref()
                    .expect("Clients are not initialized");
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }

            /// This test shows currently we do not receive mining rewards from Zebra unless we mine 100 blocks at a time.
            /// This is not the case with Zcashd and should not be the case here.
            /// Even if rewards need 100 confirmations these blocks should not have to be mined at the same time.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_clients_receive_mining_reward() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();
                let mut clients = test_manager
                    .clients
                    .take()
                    .expect("Clients are not initialized");

                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                test_manager.local_net.generate_blocks(100).await.unwrap();
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0
                        || clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64() > 0,
                    "No mining reward received from Zebrad. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_clients_receive_mining_reward_and_send() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();
                let mut clients = test_manager
                    .clients
                    .take()
                    .expect("Clients are not initialized");

                test_manager.local_net.generate_blocks(100).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                        > 0,
                    "No mining reward received from Zebrad. Faucet Transparent Balance: {:}.",
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                );

                // *Send all transparent funds to own orchard address.
                clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
                test_manager.local_net.generate_blocks(1).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0,
                "No funds received from shield. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                let recipient_zaddr = clients.get_recipient_address("sapling").await.to_string();
                zingolib::testutils::lightclient::from_inputs::quick_send(
                    &mut clients.faucet,
                    vec![(&recipient_zaddr, 250_000, None)],
                )
                .await
                .unwrap();

                test_manager.local_net.generate_blocks(1).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                clients.recipient.sync_and_await().await.unwrap();
                dbg!(clients
                    .recipient
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert_eq!(
                    clients
                        .recipient
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_sapling_balance
                        .unwrap()
                        .into_u64(),
                    250_000
                );

                test_manager.close().await;
            }

            #[ignore = "requires fully synced testnet."]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_testnet() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::Fetch,
                    Some(NetworkKind::Testnet),
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    ZEBRAD_TESTNET_CACHE_DIR.clone(),
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();
                let clients = test_manager
                    .clients
                    .as_ref()
                    .expect("Clients are not initialized");
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }
        }

        mod state_service {

            use zcash_local_net::validator::zebrad::Zebrad;
            use zip32::AccountId;

            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn basic() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    false,
                    false,
                    false,
                )
                .await
                .unwrap();
                assert_eq!(
                    2,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn generate_blocks() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    false,
                    false,
                    false,
                )
                .await
                .unwrap();
                assert_eq!(
                    2,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.generate_blocks_with_delay(1).await;
                assert_eq!(
                    3,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.close().await;
            }

            #[ignore = "chain cache needs development"]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn with_chain() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    ZEBRAD_CHAIN_CACHE_DIR.clone(),
                    false,
                    false,
                    false,
                )
                .await
                .unwrap();
                assert_eq!(
                    52,
                    u32::from(test_manager.local_net.get_chain_height().await)
                );
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    false,
                )
                .await
                .unwrap();
                let _grpc_client = build_client(services::network::localhost_uri(
                    test_manager
                        .zaino_grpc_listen_address
                        .expect("Zaino listen port not available but zaino is active.")
                        .port(),
                ))
                .await
                .unwrap();
                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_clients() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();
                let clients = test_manager
                    .clients
                    .as_ref()
                    .expect("Clients are not initialized");
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }

            /// This test shows currently we do not receive mining rewards from Zebra unless we mine 100 blocks at a time.
            /// This is not the case with Zcashd and should not be the case here.
            /// Even if rewards need 100 confirmations these blocks should not have to be mined at the same time.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_clients_receive_mining_reward() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();

                let mut clients = test_manager
                    .clients
                    .take()
                    .expect("Clients are not initialized");

                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                let _ = test_manager.local_net.generate_blocks_with_delay(100).await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0
                        || clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64() > 0,
                    "No mining reward received from Zebrad. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                    clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                test_manager.close().await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_clients_receive_mining_reward_and_send() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    None,
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    None,
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();

                let mut clients = test_manager
                    .clients
                    .take()
                    .expect("Clients are not initialized");

                let _ = test_manager.local_net.generate_blocks_with_delay(100).await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                        > 0,
                    "No mining reward received from Zebrad. Faucet Transparent Balance: {:}.",
                    clients
                        .faucet
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_transparent_balance
                        .unwrap()
                        .into_u64()
                );

                // *Send all transparent funds to own orchard address.
                clients.faucet.quick_shield(AccountId::ZERO).await.unwrap();
                let _ = test_manager.local_net.generate_blocks_with_delay(1).await;
                clients.faucet.sync_and_await().await.unwrap();
                dbg!(clients
                    .faucet
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert!(
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64() > 0,
                "No funds received from shield. Faucet Orchard Balance: {:}. Faucet Transparent Balance: {:}.",
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().total_orchard_balance.unwrap().into_u64(),
                clients.faucet.account_balance(zip32::AccountId::ZERO).await.unwrap().confirmed_transparent_balance.unwrap().into_u64()
            );

                let recipient_zaddr = clients.get_recipient_address("sapling").await.to_string();
                zingolib::testutils::lightclient::from_inputs::quick_send(
                    &mut clients.faucet,
                    vec![(&recipient_zaddr, 250_000, None)],
                )
                .await
                .unwrap();

                let _ = test_manager.local_net.generate_blocks_with_delay(1).await;
                clients.recipient.sync_and_await().await.unwrap();
                dbg!(clients
                    .recipient
                    .account_balance(zip32::AccountId::ZERO)
                    .await
                    .unwrap());

                assert_eq!(
                    clients
                        .recipient
                        .account_balance(zip32::AccountId::ZERO)
                        .await
                        .unwrap()
                        .confirmed_sapling_balance
                        .unwrap()
                        .into_u64(),
                    250_000
                );

                test_manager.close().await;
            }

            #[ignore = "requires fully synced testnet."]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            pub(crate) async fn zaino_testnet() {
                let mut test_manager = TestManager::<Zebrad>::launch(
                    &ValidatorKind::Zebrad,
                    &BackendType::State,
                    Some(NetworkKind::Testnet),
                    Some(ZEBRAD_DEFAULT_ACTIVATION_HEIGHTS),
                    ZEBRAD_TESTNET_CACHE_DIR.clone(),
                    true,
                    false,
                    true,
                )
                .await
                .unwrap();
                let clients = test_manager
                    .clients
                    .as_ref()
                    .expect("Clients are not initialized");
                dbg!(clients.faucet.do_info().await);
                dbg!(clients.recipient.do_info().await);
                test_manager.close().await;
            }
        }
    }
}
