use crate::execution::model::{NodeExecutionState, WorkflowExecution};
use crate::model::Graph;
use crate::persistence::dynamodb::repository::DynamoDbRepository;
use crate::persistence::model::{PersistencePort, WriteWorkflowExecutionRequest};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsOutput;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct InMemoryRepository {
    workflows: Mutex<HashMap<String, Graph>>,
    workflow_executions: Arc<Mutex<HashMap<String, WorkflowExecution>>>,
    node_executions: Arc<Mutex<HashMap<String, NodeExecutionState>>>,
    sender: Mutex<Sender<NodeExecutionState>>,

}

pub struct Repository {
    pub port: PersistencePort
}

impl Repository {
    pub fn new(port: PersistencePort) -> Self {
        Self { port }
    }
    pub async fn of_dynamodb() -> Self {
        Repository::new(PersistencePort::DynamoDb(DynamoDbRepository::new().await))
    }
}

impl PersistencePort {

    pub async fn write_workflow_execution(&self, request: WriteWorkflowExecutionRequest) {
        match self {
            PersistencePort::DynamoDb(dynamo_db_repo) => {
                dynamo_db_repo.transact_write_items(request).await.unwrap();
            }
            PersistencePort::InMemory => {}
        }
    }

    pub async fn get_workflow_execution(
        &self,
        workflow_id: &String,
        execution_id: &String,
    ) -> Result<Option<WorkflowExecution>, PersistenceError> {

        match self {
            PersistencePort::DynamoDb(dynamo_db_repo) => {
                Ok(dynamo_db_repo.get_workflow_execution(workflow_id, execution_id).await)
            }
            _ => {todo!()}
        }
    }

    pub async fn get_node_execution(
        &self,
        workflow_id: &String,
        execution_id: &String,
        state_id: &String,
    ) -> Result<Option<NodeExecutionState>, PersistenceError> {

        match self {
            PersistencePort::DynamoDb(dynamo_db_repo) => {
                Ok(dynamo_db_repo.get_node_execution(workflow_id, execution_id, state_id).await)
            }
            _ => {todo!()}
        }
    }

    pub async fn get_node_executions(
        &self,
        workflow_id: &String,
        execution_id: &String,
        state_ids: Vec<String>,
    ) -> Vec<NodeExecutionState> {

        match self {
            PersistencePort::DynamoDb(dynamo_db_repo) => {
                dynamo_db_repo.get_node_executions(workflow_id, execution_id, state_ids).await
            }
            _ => {todo!()}
        }
    }
}

#[derive(Debug)]
pub enum PersistenceError {
    ConditionFailure(String),
    Internal(String),
}

impl Display for PersistenceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
