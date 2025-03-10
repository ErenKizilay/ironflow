use crate::auth::http::AuthenticationProvider;
use crate::engine::Engine;
use crate::execution::execution::WorkflowExecutor;
use crate::execution::model::NodeExecutionState;
use crate::model::Graph;
use crate::persistence::persistence::{InMemoryRepository, Repository};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_util::task::TaskTracker;

pub struct Listener {
    workflow_executor: Arc<WorkflowExecutor>,
    receiver: Mutex<Receiver<NodeExecutionState>>,
    task_tracker: TaskTracker,
    shutdown_receiver: watch::Receiver<bool>,
}

impl Listener {
    pub async fn new(shutdown_receiver: watch::Receiver<bool>) -> Self {
        let (tx, mut rx) = mpsc::channel(32);
        let sender: Mutex<Sender<NodeExecutionState>> = Mutex::new(tx);
        let receiver: Mutex<Receiver<NodeExecutionState>> = Mutex::new(rx);
        let repository = Repository::of_dynamodb().await;
        Listener {
            shutdown_receiver,
            workflow_executor: Arc::new(WorkflowExecutor::new(Arc::new(repository))),
            receiver,
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(self: Arc<Self>) {
        let mut receiver = self.receiver.lock().await;
        let mut shutdown_receiver = self.shutdown_receiver.clone();
        loop {
            tokio::select! {
                Some(state) = receiver.recv() => {
                    tracing::info!("Listener received state: {:?}", state);
                    let workflow_executor = self.workflow_executor.clone();
                    self.task_tracker.spawn(async move {
                        workflow_executor.continue_execution(&state).await;
                    });
                }
                _ = shutdown_receiver.changed() => {
                    if *shutdown_receiver.borrow() {
                        tracing::info!("Shutdown signal received. Exiting listener loop...");
                        receiver.close();
                    }
                }
            }
        }
    }

    pub async fn run_workflow(
        &self,
        execution_id: String,
        workflow: Graph,
        auth_provider: Vec<AuthenticationProvider>,
        input: Value,
    ) {
        tracing::info!("Will run workflow: {}", workflow.id);
        self.workflow_executor
            .start(workflow.clone(), execution_id, input, auth_provider)
            .await;
    }
}
