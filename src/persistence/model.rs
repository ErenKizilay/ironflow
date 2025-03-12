use crate::auth::http::AuthenticationProvider;
use crate::execution::model::{Execution, NodeExecutionState, Status, WorkflowExecutionError};
use crate::model::{Graph, NodeId};
use bon::Builder;
use serde_json::Value;
use std::collections::HashMap;
use crate::persistence::dynamodb::repository::DynamoDbRepository;

pub struct WorkflowExecutionIdentifier {
    pub workflow_id: String,
    pub execution_id: String,
}

pub enum PersistencePort {
    DynamoDb(DynamoDbRepository),
    InMemory,
}

#[derive(Builder)]
pub struct UpdateWorkflowExecutionRequest {
    pub workflow_id: String,
    pub execution_id: String,
    pub status: Status,
    pub node_states_by_id: Vec<(String, NodeExecutionState)>,
    pub result: Option<Result<Value, WorkflowExecutionError>>,
    pub state_keys: Vec<(NodeId, String)>,
    pub increment_index: bool,
    pub parent_execution_updates_by_state_id: Option<HashMap<String, Execution>>,
}

pub struct WriteWorkflowExecutionRequest {
    pub identifier: WorkflowExecutionIdentifier,
    pub write_requests: Vec<WriteRequest>,
}

impl WriteWorkflowExecutionRequest {
    pub fn builder() -> WriteWorkflowExecutionRequestBuilder {
        WriteWorkflowExecutionRequestBuilder {
            workflow_id: "".to_string(),
            execution_id: "".to_string(),
            write_requests: vec![],
        }
    }
}


pub struct WriteWorkflowExecutionRequestBuilder {
    workflow_id: String,
    execution_id: String,
    write_requests: Vec<WriteRequest>,
}

impl WriteWorkflowExecutionRequestBuilder {
    pub fn new(workflow_id: String, execution_id: String) -> Self {
        WriteWorkflowExecutionRequestBuilder {
            workflow_id,
            execution_id,
            write_requests: vec![],
        }
    }

    pub fn workflow_id(mut self, workflow_id: String) -> Self {
        self.workflow_id = workflow_id;
        self
    }

    pub fn execution_id(mut self, execution_id: String) -> Self {
        self.execution_id = execution_id;
        self
    }

    pub fn write(mut self, request: WriteRequest) -> Self {
        self.write_requests.push(request);
        self
    }

    pub fn writes(mut self, requests: Vec<WriteRequest>) -> Self {
        self.write_requests.extend(requests);
        self
    }

    pub fn build(self) -> WriteWorkflowExecutionRequest {
        WriteWorkflowExecutionRequest {
            identifier: WorkflowExecutionIdentifier {
                workflow_id: self.workflow_id,
                execution_id: self.execution_id,
            },
            write_requests: self.write_requests.clone(),
        }
    }
}

#[derive(Clone)]
pub enum WriteRequest {
    InitiateWorkflowExecution(InitiateWorkflowExecDetails),
    IncrementWorkflowIndex(IncrementWorkflowIndexDetails),
    UpdateWorkflowStatus(Status),
    InitiateNodeExec(InitiateNodeExecDetails),
    LockNodeExec(LockNodeExecDetails),
    SaveNodeExec(NodeExecutionState),
    UpdateNodeStatus(UpdateNodeStatusDetails),
    IncrementBranchIndex(IncrementBranchIndexDetails),
    IncrementConditionIndex(IncrementConditionIndexDetails),

}

#[derive(Clone, Debug)]
pub struct UpdateNodeStatusDetails {
    pub node_id: NodeId,
    pub state_id: String,
    pub status: Status,
}

#[derive(Clone, Debug)]
pub struct IncrementWorkflowIndexDetails {
    pub increment: bool,
    pub current_index: usize
}

#[derive(Clone, Debug)]
pub struct IncrementBranchIndexDetails {
    pub node_id: NodeId,
    pub state_id: String,
    pub branch_name: String,
    pub current_branch_index: usize
}

#[derive(Clone, Debug)]
pub struct LockNodeExecDetails {
    pub node_id: NodeId,
    pub state_id: String,
    pub retry_count: usize,
}

#[derive(Clone, Debug)]
pub struct IncrementConditionIndexDetails {
    pub node_id: NodeId,
    pub state_id: String,
    pub current_index: usize
}

#[derive(Clone, Debug)]
pub struct InitiateNodeExecDetails {
    pub node_id: NodeId,
    pub state_id: String,
    pub dept: Vec<NodeId>
}

#[derive(Clone, Debug)]
pub struct InitiateWorkflowExecDetails {
    pub input: Value,
    pub authentication_providers: Vec<AuthenticationProvider>,
    pub workflow: Graph,
}