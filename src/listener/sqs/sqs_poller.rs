use crate::execution::model::NodeExecutionState;
use aws_config::BehaviorVersion;
use aws_sdk_sqs::Client;
use std::sync::Arc;
use std::time::Duration;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_sqs::operation::delete_message::{DeleteMessageError, DeleteMessageOutput};
use aws_sdk_sqs::operation::receive_message::{ReceiveMessageError, ReceiveMessageOutput};
use serde_dynamo::{from_item, Item};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tracing::trace;
use crate::listener::listener::Message;

pub struct SqsPoller {
    message_channel: Mutex<Sender<Message>>,
    message_deletion_receiver: Mutex<Receiver<String>>,
    sqs_client: Arc<Client>,
    queue_url: String,
}

impl SqsPoller {
    pub async fn new(message_channel: Mutex<Sender<Message>>, message_deletion_receiver: Mutex<Receiver<String>>, queue_name: String) -> Self {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        SqsPoller {
            message_deletion_receiver,
            message_channel,
            sqs_client: Arc::new(client),
            queue_url: format!("https://sqs.{}.amazonaws.com/{}/{}", config.region().unwrap().as_ref(), "975050130741", queue_name),
        }
    }

    async fn poll(&self) {
        let result = self.sqs_client.receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(10)
            .visibility_timeout(Duration::from_secs(5).as_secs() as i32)
            .send().await;
        match result {
            Ok(message_output) => {
                let messages = message_output.messages.unwrap_or_default();
                tracing::info!("received messages size: {:?}", messages.len());
                for message in messages {
                    let message_body = message.body.unwrap();
                    let item: Item = serde_json::from_str(message_body.as_ref())
                        .expect("expected to deserialize DynamoDB JSON format");
                    let node_exec_state: NodeExecutionState = from_item(item.clone())
                        .expect(format!("expected NodeExecutionState in DynamoDB JSON format, got {:?}", item).as_str());
                    self.message_channel.lock()
                        .await.send(Message {
                        node_execution_state: node_exec_state,
                        id: message.receipt_handle.unwrap(),
                    })
                        .await.unwrap();
                }
            }
            Err(err) => {
                tracing::error!("Error polling sqs message: {}", err);
            }
        }
    }

    async fn delete_message(&self) {
        while let Some(message_id) = self.message_deletion_receiver.lock().await.recv().await {
            let result = self.sqs_client.delete_message()
                .queue_url(&self.queue_url)
                .receipt_handle(message_id.clone())
                .send().await;
            match result {
                Ok(_) => {
                    tracing::info!("Message deleted successfully");
                }
                Err(err) => {
                    tracing::error!("Error deleting message: {}", err);
                }
            }
        }
    }

    pub async fn start(self: Arc<Self>) {
        tracing::info!("Starting SQS poller");
        let cloned_self = self.clone();
        tokio::spawn(async move {
            cloned_self.delete_message().await;
        });
        tokio::spawn(async move {
            loop {
                self.poll().await;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    pub async fn stop(&self) {
        self.message_channel.lock().await.closed().await;
    }
}