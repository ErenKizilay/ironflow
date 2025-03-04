use crate::execution::model::{Execution, NodeExecutionState, Status, WorkflowExecutionError};
use crate::model::NodeId;
use bon::Builder;
use serde_json::Value;
use std::collections::HashMap;

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