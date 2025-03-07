use std::collections::HashMap;
use std::sync::Arc;
use serde_json::Value;
use crate::execution::execution::ContinueParentNodeExecutionCommand;
use crate::execution::model::Execution::Condition;
use crate::execution::model::{ConditionExecution, Execution, NodeExecutionState, Status};
use crate::model::{ConditionConfig, NodeConfig};
use crate::persistence::model::UpdateWorkflowExecutionRequest;
use crate::persistence::persistence::Repository;

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
                let child_nodes = if condition_exec.true_branch {
                    condition_config.true_branch.clone()
                } else {
                    condition_config.true_branch.clone()
                };
                if condition_exec.index == child_nodes.len() {
                    tracing::info!("Condition Execution over!");
                    repository
                        .update_execution(
                            UpdateWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .status(Status::InProgress)
                                .increment_index(parent_state.depth.is_empty())
                                .node_states_by_id(vec![(
                                    parent_state_id,
                                    NodeExecutionState {
                                        workflow_id: workflow.id.clone(),
                                        execution_id: workflow_execution.execution_id.clone(),
                                        node_id: parent_state.node_id.clone(),
                                        status: Status::Success,
                                        execution: parent_state.execution.clone(),
                                        depth: parent_state.depth.clone(),
                                    },
                                )])
                                .state_keys(vec![])
                                .build(),
                        )
                        .await;
                } else {
                    let child_node_id = child_nodes.get(condition_exec.index).unwrap();
                    let child_state_id =
                        format!("{}_{}", child_node_id.name, condition_exec.true_branch);
                    let mut path_so_far = parent_state.depth.clone();
                    path_so_far.push(parent_state.node_id.clone());
                    repository
                        .update_execution(
                            UpdateWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .status(Status::InProgress)
                                .increment_index(false)
                                .node_states_by_id(vec![(
                                    child_state_id.clone(),
                                    NodeExecutionState {
                                        workflow_id: workflow.id.clone(),
                                        execution_id: workflow_execution.execution_id.clone(),
                                        node_id: child_node_id.clone(),
                                        status: Status::Queued,
                                        execution: None,
                                        depth: path_so_far,
                                    },
                                )])
                                .state_keys(vec![(child_node_id.clone(), child_state_id)])
                                .parent_execution_updates_by_state_id(HashMap::from([(
                                    parent_state_id.clone(),
                                    Condition(ConditionExecution {
                                        true_branch: condition_exec.true_branch,
                                        index: condition_exec.index + 1,
                                    }),
                                )]))
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