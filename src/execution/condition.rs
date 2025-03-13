use crate::execution::model::ContinueParentNodeExecutionCommand;
use crate::execution::model::Execution::Condition;
use crate::execution::model::{ConditionExecution, Execution, NodeExecutionState, Status};
use crate::model::{ConditionConfig, NodeConfig};
use crate::persistence::model::{IncrementConditionIndexDetails, IncrementWorkflowIndexDetails, InitiateNodeExecDetails, UpdateNodeStatusDetails, UpdateWorkflowExecutionRequest, WriteRequest, WriteWorkflowExecutionRequest};
use crate::persistence::persistence::{InMemoryRepository, Repository};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

pub async fn initiate_execution(condition_config: &ConditionConfig, context: &Value) -> (Status, Execution) {
    let condition_result = condition_config.expression.evaluate(context.clone());
    (
        Status::InProgress,
        Condition(ConditionExecution {
            true_branch: condition_result.is_boolean() && condition_result.as_bool().unwrap(),
            index: 0,
        }),
    )
}

pub async fn continue_execution(repository: Arc<Repository>, command: ContinueParentNodeExecutionCommand) {
    let workflow_execution = command.workflow_execution;
    let parent_state = command.parent_state;
    let workflow = workflow_execution.workflow.clone();
    let parent_node = workflow.get_node(&parent_state.node_id).unwrap();
    let parent_state_id = workflow_execution.get_state_id_of_node(&parent_state.node_id);
    match parent_node {
        NodeConfig::ConditionNode(condition_config) => {
            if let Some(Condition(ref condition_exec)) = parent_state.execution {
                if command.child_state.is_none() && condition_exec.index > 0 {
                    tracing::info!("Condition[{}] execution is already started", parent_state.node_id.name);
                    return;
                }
                let child_nodes = if condition_exec.true_branch {
                    condition_config.true_branch.clone()
                } else {
                    condition_config.false_branch.clone()
                };
                if condition_exec.index == child_nodes.len() {
                    tracing::info!("Condition Execution over!");
                    repository.port
                        .write_workflow_execution(
                            WriteWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .write(WriteRequest::UpdateNodeStatus(UpdateNodeStatusDetails {
                                    node_id: parent_state.node_id.clone(),
                                    state_id: parent_state_id.clone(),
                                    status: Status::Success,
                                }))
                                .build(),
                        )
                        .await;
                } else {
                    let child_node_id = child_nodes.get(condition_exec.index).unwrap();
                    let child_state_id =
                        format!("{}_{}", child_node_id.name, condition_exec.true_branch);
                    tracing::info!("will initiate condition child node with state id: {}", child_state_id);
                    let mut path_so_far = parent_state.depth.clone();
                    path_so_far.push(parent_state.node_id.clone());
                    repository.port
                        .write_workflow_execution(
                            WriteWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .write(WriteRequest::InitiateNodeExec(InitiateNodeExecDetails {
                                    node_id: child_node_id.clone(),
                                    state_id: child_state_id.clone(),
                                    dept: path_so_far.clone(),
                                }))
                                .write(WriteRequest::IncrementConditionIndex(IncrementConditionIndexDetails {
                                    node_id: parent_state.node_id.clone(),
                                    state_id: parent_state_id.clone(),
                                    current_index: condition_exec.index,
                                }))
                                .build(),
                        )
                        .await;
                }
            }
        }
        _ => {
            unreachable!()
        }
    }
}