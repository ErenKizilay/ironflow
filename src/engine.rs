use crate::api::routes::{build_routes, AppState};
use crate::auth::http::AuthenticationProvider;
use crate::execution::execution::WorkflowExecutor;
use crate::listener::listener::Listener;
use crate::model::Graph;
use crate::persistence::persistence::{InMemoryRepository, Repository};
use crate::yaml::yaml::{from_yaml, from_yaml_to_auth};
use axum::extract::DefaultBodyLimit;
use axum::Router;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::signal;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_util::task::TaskTracker;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tracing::Level;

#[derive(Clone)]
pub struct Engine {
    workflows_by_id: HashMap<String, Graph>,
    auth_providers: Vec<AuthenticationProvider>,
    listener: Arc<Listener>,
    task_tracker: TaskTracker,
    shutdown_tx: watch::Sender<bool>,
    pub(crate) repository: Arc<Repository>
}

impl Engine {
    pub async fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false); // Watch channel for shutdown signal
        let repository = Arc::new(Repository::of_dynamodb().await);
        Engine {
            listener: Arc::new(Listener::new(shutdown_rx, repository.clone()).await),
            workflows_by_id: Default::default(),
            auth_providers: vec![],
            task_tracker: TaskTracker::new(),
            shutdown_tx,
            repository: repository.clone(),
        }
    }

    pub fn load_auth_providers_locally(mut self, auth_providers_path: &str) -> Self {
        let mut provider_details = from_yaml_to_auth(auth_providers_path);
        self.auth_providers
            .append(provider_details.providers.as_mut());
        self
    }

    pub async fn load_workflows_locally(mut self, workflows_directory: &str) -> Self {
        if let Ok(entries) = fs::read_dir(workflows_directory) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(path_str) = path.to_str() {
                        let workflow_result = from_yaml(path_str);
                        match workflow_result {
                            Ok(workflow) => {
                                self.workflows_by_id.insert(workflow.id.clone(), workflow);
                            }
                            Err(err) => {
                                panic!("Error loading workflow: {}", err);
                            }
                        }
                    }
                }
            }
        }
        self
    }

    pub async fn start(self) {
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
    }

    pub async fn run_workflow(&self, workflow_id: String, execution_id: String, input: Value) {
        let workflow = self.workflows_by_id.get(&workflow_id).unwrap();
        let auth_providers = self.auth_providers
            .iter()
            .filter(|authentication_provider| workflow.config.auth_providers.contains(&authentication_provider.name))
            .cloned()
            .collect::<Vec<AuthenticationProvider>>();
        tracing::info!("Will run workflow: {}", workflow.id);
        self.listener.run_workflow(execution_id, workflow.clone(), auth_providers, input).await;
    }

    async fn run_api(self: Arc<Self>) {
        let routes = build_routes(AppState {
            engine: self,
        }).await;
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        axum::serve(listener, routes).await.unwrap();
    }
}
