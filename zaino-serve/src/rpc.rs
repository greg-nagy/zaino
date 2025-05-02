//! gRPC / JsonRPC service implementations.

use zaino_state::{FetchServiceSubscriber, IndexerSubscriber};

pub mod grpc;
pub mod jsonrpc;

#[derive(Clone)]
/// Zaino gRPC service.
pub struct GrpcClient {
    /// Chain fetch service subscriber.
    pub service_subscriber: IndexerSubscriber<FetchServiceSubscriber>,
}

#[derive(Clone)]
/// Zaino gRPC service.
pub struct JsonRpcClient {
    /// Chain fetch service subscriber.
    pub service_subscriber: IndexerSubscriber<FetchServiceSubscriber>,
}
