use crate::auth::http::AuthenticationProvider;
use crate::execution::execution::WorkflowExecutor;
use crate::model::Graph;
use crate::persistence::persistence::Repository;
use crate::yaml::yaml::{from_yaml, from_yaml_to_auth};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::{mpsc, Mutex};
use tokio_util::task::TaskTracker;

pub struct Engine {
    repository: Arc<Repository>,
    workflow_executor: Arc<WorkflowExecutor>,
    workflows_by_id: HashMap<String, Graph>,
    auth_providers: Vec<AuthenticationProvider>,
    task_tracker: TaskTracker,
}

impl Engine {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(32);
        let repository = Repository::new(tx);
        Engine {
            repository: repository.clone(),
            workflow_executor: Arc::new(WorkflowExecutor::new(
                repository,
                Arc::new(Mutex::new(rx)),
            )),
            workflows_by_id: Default::default(),
            auth_providers: vec![],
            task_tracker: TaskTracker::new(),
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
                        let workflow = from_yaml(path_str);
                        self.repository
                            .save_workflow(workflow.clone())
                            .await
                            .unwrap();
                        self.workflows_by_id.insert(workflow.id.clone(), workflow);
                    }
                }
            }
        }
        self
    }

    pub async fn start(&self) {
        tracing::info!("Starting Engine");
        let arc = self.workflow_executor.clone();
        self.task_tracker.spawn(async move {
            arc.listen().await;
        });
        tracing::info!("Engine started");
    }

    pub async fn stop(self) {
        match signal::ctrl_c().await {
            Ok(_) => {
                tracing::info!("Shutting down Engine");
                drop(self.repository);
                self.workflow_executor.stop().await;
                drop(self.workflow_executor);
                self.task_tracker.close();
                self.task_tracker.wait().await;
                tracing::info!("Engine stopped");
            }
            Err(_) => {
                tracing::error!("Failed to install CTRL-C handler");
            }
        }
    }

    pub async fn run_workflow(&self, workflow_id: String, execution_id: String, input: Value) {
        let workflow = self.workflows_by_id.get(&workflow_id).unwrap();
        let auth_providers = self.auth_providers
            .iter()
            .filter(|authentication_provider| workflow.config.auth_providers.contains(&authentication_provider.name))
            .cloned()
            .collect::<Vec<AuthenticationProvider>>();
        tracing::info!("Will run workflow: {}", workflow.id);
        self.workflow_executor
            .start(workflow.clone(), execution_id, input, auth_providers)
            .await;
    }
}
