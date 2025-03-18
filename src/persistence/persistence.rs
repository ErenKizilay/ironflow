use crate::execution::model::{NodeExecutionState, WorkflowExecution};
use crate::persistence::dynamodb::adapter::DynamoDbRepository;
use crate::persistence::model::WriteWorkflowExecutionRequest;
use async_trait::async_trait;
use std::fmt::Display;
use std::sync::Arc;
use crate::config::configuration::{PersistenceConfig, PersistenceProvider};
use crate::in_memory::adapters::{in_memory_persistence, InMemoryRepositoryAdapter};

#[async_trait]
pub trait PersistencePort: Send + Sync {
    async fn write_workflow_execution(&self, request: WriteWorkflowExecutionRequest) -> Result<(), PersistenceError>;

    async fn get_workflow_execution(
        &self,
        workflow_id: &String,
        execution_id: &String,
    ) -> Result<Option<WorkflowExecution>, PersistenceError>;

    async fn get_node_execution(
        &self,
        workflow_id: &String,
        execution_id: &String,
        state_id: &String,
    ) -> Result<Option<NodeExecutionState>, PersistenceError>;

    async fn get_node_executions(
        &self,
        workflow_id: &String,
        execution_id: &String,
        state_ids: Vec<String>,
    ) -> Vec<NodeExecutionState>;
}

pub struct Repository {
    pub delegate: Arc<dyn PersistencePort>,
}

impl Repository {

    pub async fn new(config: PersistenceConfig) -> Self {
        match config.provider {
            PersistenceProvider::DynamoDb => {
                tracing::info!("Persistence port: DynamoDb");
                Self::of_dynamodb().await
            }
            PersistenceProvider::InMemory => {
                tracing::info!("Persistence port: InMemory");
                Self::of_in_memory().await
            }
        }
    }

    fn of(delegate: Arc<dyn PersistencePort>) -> Self {
        Self { delegate }
    }
    async fn of_dynamodb() -> Self {
        Repository::of(Arc::new(DynamoDbRepository::new().await))
    }

    async fn of_in_memory() -> Self {
        Repository::of(in_memory_persistence().clone())
    }
    pub async fn write_workflow_execution(&self, request: WriteWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        self.delegate.write_workflow_execution(request).await
    }

    pub async fn get_workflow_execution(&self, workflow_id: &String, execution_id: &String) -> Result<Option<WorkflowExecution>, PersistenceError> {
        self.delegate.get_workflow_execution(workflow_id, execution_id).await
    }

    pub async fn get_node_execution(&self, workflow_id: &String, execution_id: &String, state_id: &String) -> Result<Option<NodeExecutionState>, PersistenceError> {
        self.delegate.get_node_execution(workflow_id, execution_id, state_id).await
    }

    pub async fn get_node_executions(&self, workflow_id: &String, execution_id: &String, state_ids: Vec<String>) -> Vec<NodeExecutionState> {
        self.delegate.get_node_executions(workflow_id, execution_id, state_ids).await
    }
}

#[derive(Debug)]
pub enum PersistenceError {
    ConditionFailure(String),
    Internal(String),
}


