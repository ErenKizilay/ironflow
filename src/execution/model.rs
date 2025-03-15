use crate::model::{Graph, Node, NodeId};
use bon::Builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;

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
    Workflow(ChainExecution),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainExecution {
    pub child_identifier: WorkflowExecutionIdentifier,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepExecution {
    pub retry_count: usize,
    pub result: Result<Value, StepExecutionError>
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowExecution {
    pub execution_id: String,
    pub source: ExecutionSource,
    pub input: Value,
    pub index: usize,
    pub status: Status,
    pub state_keys_by_node_id: HashMap<NodeId, String>,
    pub last_executed_node_id: Option<NodeId>,
    pub workflow: Graph,
    pub depth: Vec<WorkflowExecutionIdentifier>,
    pub started_at: i64,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum  ExecutionSource {
    Manual,
    Workflow(WorkflowSource),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowSource {
    pub execution_identifier: WorkflowExecutionIdentifier,
    pub caller_node_id: NodeId,
    pub caller_node_state_id: String,
}

impl WorkflowExecution {

    pub fn state_id_of_node(&self, node_id: &NodeId) -> String {
        self.state_keys_by_node_id.get(node_id).cloned().unwrap_or_default()
    }

    pub fn last_executed_node(&self) -> Option<Node> {
        let index = if self.workflow.node_ids.len() == self.index {self.index - 1} else {self.index};
        let workflow = &self.workflow;
        match workflow.node_ids.get(index) {
            None => {None}
            Some(last_executed_node_id) => {
                let node = workflow.nodes_by_id.get(last_executed_node_id).unwrap();
                Some(Node::of(last_executed_node_id.clone(), node.clone()))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoopExecution {
    pub iteration_count: usize,
    pub index: usize,
    pub error: Option<String>,
    pub items: Vec<Value>,
}

impl LoopExecution {

    pub fn get_item(&self) -> Value {
        self.items[self.iteration_count].clone()
    }
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
    WillRetried,
    Running,
    Success,
    InProgress,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum  WorkflowExecutionError {
    StepFailed(String),
    AssertionFailed(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum  StepExecutionError {
    RunFailed(Value),
}

impl Display for StepExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StepExecutionError::RunFailed(value) => {
                write!(f, "{}", value)
            }
        }
    }
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

//if child_state is empty then parent node execution triggerred by itself meaning that it is just started
#[derive(Builder)]
pub(crate) struct ContinueParentNodeExecutionCommand {
    pub(crate) workflow_execution: WorkflowExecution,
    pub(crate) parent_state: NodeExecutionState,
    pub(crate) child_state: Option<NodeExecutionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowExecutionIdentifier {
    pub workflow_id: String,
    pub execution_id: String,
}

#[derive(Builder)]
pub(crate) struct StartWorkflowCommand {
    pub workflow_id: String,
    pub execution_id: Option<String>,
    pub input: Value,
    pub source: ExecutionSource,
    pub dept_so_far: Vec<WorkflowExecutionIdentifier>,
}