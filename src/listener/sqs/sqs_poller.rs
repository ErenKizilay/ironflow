use crate::execution::model::NodeExecutionState;
use crate::listener::listener::Message;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_sqs::operation::delete_message::{DeleteMessageError, DeleteMessageOutput};
use aws_sdk_sqs::operation::receive_message::{ReceiveMessageError, ReceiveMessageOutput};
use aws_sdk_sqs::Client;
use serde_dynamo::{from_item, Item};
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tracing::trace;
use crate::config::configuration::ListenerConfig;
use crate::listener::queue::QueuePort;

pub struct SqsAdapter {
    sqs_client: Arc<Client>,
    queue_url: String,
    listener_config: ListenerConfig
}

impl SqsAdapter {
    pub(crate) async fn new(listener_config: ListenerConfig, queue_name: String) -> Arc<SqsAdapter> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        let queue_url = client.list_queues()
            .queue_name_prefix(queue_name.clone())
            .send().await.unwrap().queue_urls
            .unwrap()
            .iter().next().unwrap().clone();
        Arc::new(
            SqsAdapter {
                sqs_client: Arc::new(client),
                queue_url: queue_url,
                listener_config,
            }
        )
    }
}

#[async_trait]
impl QueuePort for SqsAdapter {

    async fn receive_messages(&self) -> Vec<Message> {
        let result = self.sqs_client.receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(10)
            .visibility_timeout(self.listener_config.message_visibility_timeout.as_secs() as i32)
            .send().await;
        match result {
            Ok(message_output) => {
                let messages = message_output.messages.unwrap_or_default();
                messages.iter()
                    .map(|sqs_message| {
                        let message_body = sqs_message.clone().body.unwrap();
                        let item: Item = serde_json::from_str(message_body.as_ref())
                            .expect("expected to deserialize DynamoDB JSON format");
                        let node_exec_state: NodeExecutionState = from_item(item.clone())
                            .expect(format!("expected NodeExecutionState in DynamoDB JSON format, got {:?}", item).as_str());
                        Message {
                            node_execution_state: node_exec_state,
                            id: sqs_message.clone().receipt_handle.unwrap(),
                        }
                    }).collect()
            }
            Err(err) => {
                tracing::error!("Error polling sqs message: {}", err);
                vec![]
            }
        }
    }

    async fn delete_message(&self, message_id: &String) {
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
