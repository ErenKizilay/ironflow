use crate::auth::http::HttpAuthentication;
use crate::engine::IronFlow;
use crate::execution::execution::WorkflowExecutor;
use crate::execution::model::NodeExecutionState;
use crate::listener::sqs::sqs_poller::SqsPoller;
use crate::model::Graph;
use crate::persistence::persistence::{InMemoryRepository, Repository};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_util::task::TaskTracker;
use crate::config::configuration::ListenerConfig;

pub struct Listener {
    listener_config: ListenerConfig,
    workflow_executor: Arc<WorkflowExecutor>,
    receiver: Mutex<Receiver<Message>>,
    message_deletion_sender: Arc<Mutex<Sender<String>>>,
    task_tracker: TaskTracker,
    shutdown_receiver: watch::Receiver<bool>,
    sqs_poller: Arc<SqsPoller>,
}

#[derive(Debug)]
pub struct Message {
    pub node_execution_state: NodeExecutionState,
    pub id: String,
}

impl Listener {
    pub async fn new(listener_config: ListenerConfig, shutdown_receiver: watch::Receiver<bool>, workflow_executor: Arc<WorkflowExecutor>) -> Self {
        let (tx, mut rx) = mpsc::channel(32);
        let (message_deletion_tx, mut message_deletion_rx) = mpsc::channel(32);
        let sender: Mutex<Sender<Message>> = Mutex::new(tx);
        let receiver: Mutex<Receiver<Message>> = Mutex::new(rx);
        Listener {
            listener_config: listener_config.clone(),
            shutdown_receiver,
            workflow_executor,
            receiver,
            message_deletion_sender: Arc::new(Mutex::new(message_deletion_tx)),
            task_tracker: TaskTracker::new(),
            sqs_poller: Arc::new(SqsPoller::new(listener_config, sender, Mutex::new(message_deletion_rx), "ironflow_node_executions".to_string()).await),
        }
    }

    pub async fn start(self: Arc<Self>) {
        let mut receiver = self.receiver.lock().await;
        let mut shutdown_receiver = self.shutdown_receiver.clone();
        self.sqs_poller.clone().start().await;
        loop {
            tokio::select! {
                Some(message) = receiver.recv() => {
                    tracing::info!("Listener received state:\n{:#?}", message.node_execution_state);
                    let workflow_executor = self.workflow_executor.clone();
                    let mut message_deletion_sender = self.message_deletion_sender.clone();
                    self.task_tracker.spawn(async move {
                        let exec_result = workflow_executor.continue_execution(&message.node_execution_state).await;
                        match exec_result {
                            Ok(_) => {
                                message_deletion_sender.lock().await.send(message.id).await
                                .unwrap();
                            },
                            Err(err) => {
                                tracing::error!("Failed to execute node execution: {}", err);
                            }
                        }
                    });
                }
                _ = shutdown_receiver.changed() => {
                    if *shutdown_receiver.borrow() {
                        self.sqs_poller.stop().await;
                        receiver.close();
                    }
                }
            }
        }
    }
}
