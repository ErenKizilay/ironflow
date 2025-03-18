use async_trait::async_trait;
use crate::listener::listener::Message;

#[async_trait]
pub trait QueuePort: Send + Sync {
    async fn receive_messages(&self) -> Vec<Message>;

    async fn delete_message(&self, message_id: &String);
}