use crate::api::routes::{build_routes, ApiState};
use crate::auth::http::HttpAuthentication;
use crate::auth::provider::AuthProvider;
use crate::config::configuration::{ConfigOptions, ConfigurationManager, IronFlowConfig};
use crate::execution::execution::WorkflowExecutor;
use crate::listener::listener::Listener;
use crate::model::Graph;
use crate::persistence::persistence::{InMemoryRepository, Repository};
use crate::secret::secrets::SecretManager;
use crate::yaml::yaml::{from_yaml, from_yaml_to_auth};
use axum::extract::DefaultBodyLimit;
use axum::Router;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::signal;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio_util::task::TaskTracker;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tracing::Level;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone)]
pub struct IronFlow {
    config: IronFlowConfig,
    configuration_manager: Arc<RwLock<ConfigurationManager>>,
    pub(crate) workflow_executor: Arc<WorkflowExecutor>,
    listener: Arc<Listener>,
    task_tracker: TaskTracker,
    shutdown_tx: watch::Sender<bool>,
    pub(crate) repository: Arc<Repository>
}

impl IronFlow {
    async fn new(iron_flow_config: IronFlowConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false); // Watch channel for shutdown signal
        let repository = Arc::new(Repository::of_dynamodb().await);
        let configuration_manager = ConfigurationManager::new(iron_flow_config.clone());
        let secret_manager = SecretManager::new();
        let auth_provider = Arc::new(AuthProvider::new(secret_manager, configuration_manager.clone()));
        configuration_manager.write().await.load().await;
        let workflow_executor = Arc::new(WorkflowExecutor::new(iron_flow_config.execution_config.clone(), configuration_manager.clone(), repository.clone(), auth_provider.clone())
            .await);
        IronFlow {
            config: iron_flow_config.clone(),
            configuration_manager: configuration_manager.clone(),
            workflow_executor: workflow_executor.clone(),
            listener: Arc::new(Listener::new(iron_flow_config.lister_config.clone(),shutdown_rx, workflow_executor).await),
            task_tracker: TaskTracker::new(),
            shutdown_tx,
            repository: repository.clone(),
        }
    }

    async fn start(self) {
        let engine = Arc::new(self);
        tracing::info!("Starting Engine");
        let listener = engine.listener.clone();
        let engine_cloned = engine.clone();
        engine.task_tracker.spawn(async move {
            listener.start().await;
        });
        tracing::info!("Engine started");
        engine.task_tracker.spawn(async move {
            engine_cloned.run_api().await;
        });
        signal::ctrl_c().await.expect("Failed to listen for shutdown signal");
        tracing::info!("received ctrl-c, shutting down");
        engine.shutdown_tx.send(true).unwrap();
        engine.task_tracker.close();
    }

    async fn run_api(self: Arc<Self>) {
        let routes = build_routes(ApiState {
            workflow_executor: self.workflow_executor.clone(),
            repository: self.repository.clone(),
        }).await;
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.config.api_config.port)).await.unwrap();
        axum::serve(listener, routes).await.unwrap();
    }

    pub async fn run(iron_flow_config: IronFlowConfig) {
        IronFlow::new(iron_flow_config)
            .await.start()
            .await;
    }
}
