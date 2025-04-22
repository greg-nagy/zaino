//! Zaino's JsonRPC Server Implementation.

use crate::{
    rpc::{jsonrpc::service::ZcashIndexerRpcServer as _, JsonRpcClient},
    server::{config::JsonRpcConfig, error::ServerError},
};

use zaino_state::{
    fetch::FetchServiceSubscriber,
    indexer::IndexerSubscriber,
    status::{AtomicStatus, StatusType},
};

use jsonrpsee::server::ServerBuilder;
use std::time::Duration;
use tokio::time::interval;
use tracing::warn;

/// JSON-RPC server capable of servicing clients over TCP.
pub struct JsonRpcServer {
    /// Current status of the server.
    pub status: AtomicStatus,
    /// JoinHandle for the servers `serve` task.
    pub server_handle: Option<tokio::task::JoinHandle<Result<(), ServerError>>>,
}

impl JsonRpcServer {
    /// Starts the JSON-RPC service.
    ///
    /// Launches all components then enters command loop:
    /// - Updates the ServerStatus.
    /// - Checks for shutdown signal, shutting down server if received.
    pub async fn spawn(
        service_subscriber: IndexerSubscriber<FetchServiceSubscriber>,
        server_config: JsonRpcConfig,
    ) -> Result<Self, ServerError> {
        let status = AtomicStatus::new(StatusType::Spawning.into());

        let rpc_impl = JsonRpcClient {
            service_subscriber: service_subscriber.clone(),
        };

        let server = ServerBuilder::default()
            .build(server_config.json_rpc_listen_address)
            .await
            .map_err(|e| {
                ServerError::ServerConfigError(format!("JSON-RPC server build error: {}", e))
            })?;

        let server_handle = server.start(rpc_impl.into_rpc());

        let shutdown_check_status = status.clone();
        let mut shutdown_check_interval = interval(Duration::from_millis(100));
        let shutdown_signal = async move {
            loop {
                shutdown_check_interval.tick().await;
                if StatusType::from(shutdown_check_status.load()) == StatusType::Closing {
                    break;
                }
            }
        };

        let task_status = status.clone();
        let server_task_handle = tokio::task::spawn({
            let server_handle_clone = server_handle.clone();
            async move {
                task_status.store(StatusType::Ready.into());

                tokio::select! {
                    _ = shutdown_signal => {
                        let _ = server_handle_clone.stop();
                    }
                    _ = server_handle.stopped() => {},
                }

                task_status.store(StatusType::Offline.into());
                Ok(())
            }
        });

        Ok(JsonRpcServer {
            status,
            server_handle: Some(server_task_handle),
        })
    }

    /// Sets the servers to close gracefully.
    pub async fn close(&mut self) {
        self.status.store(StatusType::Closing as usize);

        if let Some(handle) = self.server_handle.take() {
            let _ = handle.await;
        }
    }

    /// Returns the servers current status.
    pub fn status(&self) -> StatusType {
        self.status.load().into()
    }
}

impl Drop for JsonRpcServer {
    fn drop(&mut self) {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            warn!(
                "Warning: JsonRpcServer dropped without explicit shutdown. Aborting server task."
            );
        }
    }
}
