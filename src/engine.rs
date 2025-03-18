use crate::api::routes::{build_routes, ApiState};
use crate::auth::http::HttpAuthentication;
use crate::auth::provider::AuthProvider;
use crate::config::configuration::{ConfigOptions, ConfigurationManager, IronFlowConfig};
use crate::execution::execution::WorkflowExecutor;
use crate::in_memory::adapters::{in_memory_persistence};
use crate::listener::listener::{Listener, Message};
use crate::model::Graph;
use crate::persistence::persistence::{Repository};
use crate::secret::secrets::SecretManager;
use crate::yaml::yaml::{from_yaml, from_yaml_to_auth};
use axum::extract::DefaultBodyLimit;
use axum::Router;
use serde_json::Value;
use std::alloc::System;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::AsyncWriteExt;
use tokio::signal;
use tokio::sync::mpsc::Sender;
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
    pub(crate) repository: Arc<Repository>,
}

impl IronFlow {

    pub async fn run(iron_flow_config: IronFlowConfig) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false); // Watch channel for shutdown signal
        let (poll_tx, poll_rx) = watch::channel(SystemTime::now()); // Watch channel for polling signal
        let (task_sender, mut task_receiver) = mpsc::channel(32); // message channel
        let (task_deletion_sender, mut task_deletion_receiver) = mpsc::channel(32); // message deletion channel
        let configuration_manager = ConfigurationManager::new(iron_flow_config.clone());
        let repository = Arc::new(Repository::new(iron_flow_config.persistence_config).await);
        let secret_manager = SecretManager::new();
        let auth_provider = Arc::new(AuthProvider::new(
            secret_manager,
            configuration_manager.clone(),
        ));
        configuration_manager.write().await.load().await;
        let workflow_executor = Arc::new(
            WorkflowExecutor::new(
                iron_flow_config.execution_config.clone(),
                configuration_manager.clone(),
                repository.clone(),
                auth_provider.clone(),
                poll_tx.clone(),
            )
            .await,
        );
        let listener = Arc::new(
            Listener::new(
                iron_flow_config.lister_config.clone(),
                shutdown_rx.clone(),
                task_sender,
                workflow_executor.clone(),
                poll_rx
            )
            .await,
        );
        let task_tracker = TaskTracker::new();
        let task_listener = listener.clone();
        task_tracker.spawn(async move {
            task_listener.listen_messages().await;
        });
        let wf_executor_cloned = workflow_executor.clone();
        let task_deletion_sender_cloned = task_deletion_sender.clone();
        let mut shutdown_listener = shutdown_rx.clone();
        task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = task_receiver.recv() => {
                        let continue_result = wf_executor_cloned.continue_execution(&message.node_execution_state).await;
                        match continue_result {
                            Ok(_) => {
                                 task_deletion_sender_cloned.send(message.id).await.unwrap();
                            }
                            Err(_) => {}
                        }
                    }

                    _ = shutdown_listener.changed() => {
                        tracing::info!("Shutdown workflow executor");
                        break;
                    }
                }
            }
        });
        let mut shutdown_listener = shutdown_rx.clone();
        let task_deleter = listener.clone();
        task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = task_deletion_receiver.recv() => {
                        task_deleter.delete_message(&message).await;
                    }
                    _ = shutdown_listener.changed() => {
                        tracing::info!("Shutdown message cleaner");
                        break;
                    }
                }
            }
        });
        task_tracker.close();
        poll_tx.send(SystemTime::now()).unwrap();
        let routes = build_routes(ApiState {
            workflow_executor: workflow_executor.clone(),
            repository: repository.clone(),
        })
        .await;
        let listener =
            tokio::net::TcpListener::bind(format!("0.0.0.0:{}", iron_flow_config.api_config.port))
                .await
                .unwrap();
        axum::serve(listener, routes)
            .with_graceful_shutdown(async move {
                signal::ctrl_c().await.unwrap();
                tracing::info!("Shutting down");
                shutdown_tx.send(true).unwrap();
                task_tracker.wait().await;
            })
            .await
            .unwrap();
    }
}
