use crate::auth::http::AuthenticationProvider;
use crate::model::{Graph, NodeId};
use serde_json::Value;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use uuid::Timestamp;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeExecutionState {
    pub workflow_id: String,
    pub execution_id: String,
    pub state_id: String,
    pub node_id: NodeId,
    pub status: Status,
    pub execution: Option<Execution>,
    pub depth: Vec<NodeId>,
    pub created_at: i64,
    pub updated_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Execution {
    Step(StepExecution),
    Loop(LoopExecution),
    Branch(BranchExecution),
    Condition(ConditionExecution),
    Assertion(AssertionExecution),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepExecution {
    pub retry_count: usize,
    pub result: Result<Value, StepExecutionError>
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecution {
    pub execution_id: String,
    pub input: Value,
    pub index: usize,
    pub status: Status,
    pub result: Option<Result<Value, WorkflowExecutionError>>,
    pub state_keys_by_node_id: HashMap<NodeId, String>,
    pub workflow: Graph,
    pub authentication_providers: Vec<AuthenticationProvider>,
    pub started_at: i64
}

impl WorkflowExecution {

    pub fn get_state_id_of_node(&self, node_id: &NodeId) -> String {
        self.state_keys_by_node_id.get(node_id).cloned().unwrap_or_default()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoopExecution {
    pub iteration_count: usize,
    pub index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConditionExecution {
    pub true_branch: bool,
    pub index: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssertionExecution {
    pub passed: bool,
    pub errors: Vec<Option<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchExecution {
    pub branch_index: HashMap<String, usize>
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Queued,
    Running,
    Success,
    InProgress,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum  WorkflowExecutionError {
    StepFailed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum  StepExecutionError {
    RunFailed(String),
}

impl NodeExecutionState {
    pub fn get_context(&self) -> Value {
        match &self.execution {
            None => {
                Value::Null
            }
            Some(exec) => {
                match exec {
                    Execution::Step(step_exec) => {
                        match &step_exec.result {
                            Ok(result) => {
                                result.clone()
                            }
                            Err(_) => {
                                Value::Null
                            }
                        }
                    }
                    _ => {
                        Value::Null
                    }
                }
            }
        }
    }
}

impl Status {

    pub fn is_complete(&self) -> bool {
        match self {
            Status::Success | Status::Failure => {true}
            _ => {false}
        }
    }
}