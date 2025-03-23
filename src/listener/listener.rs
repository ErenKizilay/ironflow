use crate::auth::http::HttpAuthentication;
use crate::config::configuration::{ListenerConfig, PersistenceProvider, QueueProvider};
use crate::engine::IronFlow;
use crate::execution::execution::WorkflowExecutor;
use crate::execution::model::NodeExecutionState;
use crate::listener::sqs::sqs_poller::SqsAdapter;
use crate::model::Graph;
use crate::persistence::persistence::{Repository};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_util::task::TaskTracker;
use crate::in_memory::adapters::{in_memory_persistence, in_memory_queue};
use crate::listener::queue::QueuePort;

pub struct Listener {
    listener_config: ListenerConfig,
    workflow_executor: Arc<WorkflowExecutor>,
    task_channel_sender: Sender<Message>,
    shutdown_receiver: Receiver<bool>,
    queue_port: Arc<dyn QueuePort>,
    poll_receiver: Receiver<SystemTime>,

}

#[derive(Debug)]
pub struct Message {
    pub node_execution_state: NodeExecutionState,
    pub id: String,
}

impl Listener {
    pub async fn new(
        persistence_provider: PersistenceProvider,
        listener_config: ListenerConfig,
        shutdown_receiver: watch::Receiver<bool>,
        task_channel_sender: Sender<Message>,
        workflow_executor: Arc<WorkflowExecutor>,
        poll_receiver: Receiver<SystemTime>,
    ) -> Self {
        Listener {
            task_channel_sender,
            listener_config: listener_config.clone(),
            shutdown_receiver,
            workflow_executor,
            queue_port: match listener_config.queue_provider {
                QueueProvider::SQS => {
                    tracing::info!("Queue port: SQS");
                    SqsAdapter::new(persistence_provider, listener_config, "ironflow_node_executions".to_string())
                        .await
                }
                QueueProvider::InMemory => {
                    tracing::info!("Queue port: InMemory");
                    in_memory_queue().clone()
                }
            },
            poll_receiver,
        }
    }

    pub async fn listen_messages(&self) {
        let mut shutdown_receiver = self.shutdown_receiver.clone();
        let mut poll_receiver = self.poll_receiver.clone();
        loop {
            tokio::select! {
                _ = poll_receiver.changed() => {
                    tracing::info!("Polling activated...");
                    while SystemTime::now().duration_since(*poll_receiver.borrow_and_update()).unwrap().as_secs() < 5 {
                        let messages = self.queue_port.receive_messages().await;
                        for message in messages {
                            let send_result = self.task_channel_sender.send(message).await;
                            match send_result {
                                Ok(_) => {}
                                Err(_) => {
                                    tracing::warn!("Will stop polling since shutdown signal received");
                                    break;
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    tracing::info!("Polling deactivated.");
                }
                _ = shutdown_receiver.changed() => {
                    tracing::info!("Lister is shutting down");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(self.listener_config.poll_interval_ms)).await
        }
    }

    pub async fn delete_message(&self, message_id: &String) {
        self.queue_port.delete_message(message_id).await;
    }
}
