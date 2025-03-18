use crate::execution::model::ContinueParentNodeExecutionCommand;
use crate::execution::model::{BranchExecution, Execution, NodeExecutionState, Status};
use crate::model::{Branch, BranchConfig, ConditionConfig, NodeId};
use crate::persistence::model::{IncrementBranchIndexDetails, IncrementWorkflowIndexDetails, InitiateNodeExecDetails, UpdateNodeStatusDetails, UpdateWorkflowExecutionRequest, WriteRequest, WriteWorkflowExecutionRequest};
use crate::persistence::persistence::{Repository};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn initiate_execution(branch_config: &BranchConfig, context: &Value) -> (Status, Execution) {
    let branch_index: HashMap<String, usize> = branch_config
        .branches
        .iter()
        .filter(|branch| {
            match &branch.condition {
                None => {true}
                Some(expr) => {
                    let condition_result = expr.evaluate(context.clone());
                    condition_result.is_boolean() && condition_result.as_bool().unwrap()
                }
            }
        })
        .map(|branch| (branch.name.clone(), 0))
        .collect();
    (
        Status::InProgress,
        Execution::Branch(BranchExecution { branch_index }),
    )
}

pub async fn continue_execution(repository: Arc<Repository>, command: ContinueParentNodeExecutionCommand) {
    let parent_state = command.parent_state;
    let workflow_execution = command.workflow_execution;
    let workflow = workflow_execution.workflow.clone();
    let parent_state_id = workflow_execution.state_id_of_node(&parent_state.node_id);
    let branch_node = workflow.get_branch_node(&parent_state.node_id).unwrap();
    let branches_by_name = branch_node
        .branches
        .iter()
        .map(|branch| (branch.name.clone(), branch.clone()))
        .collect::<HashMap<String, Branch>>();
    let mut path_so_far = parent_state.depth.clone();
    path_so_far.push(parent_state.node_id.clone());
    if let Some(ref parent_exec) = parent_state.execution {
        if let Execution::Branch(branch_exec) = parent_exec {
            match command.child_state {
                None => {
                    if branch_exec.branch_index.values().into_iter().any(|size|size.gt(&0))  {
                        tracing::info!("Branch[{}] execution is already started", parent_state.node_id.name);
                        return;
                    }
                    let queued_executions: Vec<WriteRequest> = branch_exec
                        .branch_index
                        .iter()
                        .map(|(branch_name, branch_index)| {
                            let branch = branches_by_name.get(branch_name).unwrap();
                            let child_node_id: &NodeId =
                                branch.nodes.get(branch_index.clone()).unwrap();
                            let child_state_id =
                                format!("{}_{}", branch.name, child_node_id.name);
                            WriteRequest::InitiateNodeExec(InitiateNodeExecDetails {
                                node_id: child_node_id.clone(),
                                state_id: child_state_id.clone(),
                                dept: path_so_far.clone(),
                            })
                        })
                        .collect();
                    repository
                        .write_workflow_execution(WriteWorkflowExecutionRequest::builder()
                            .workflow_id(workflow.id.clone())
                            .execution_id(workflow_execution.execution_id.clone())
                            .writes(queued_executions)
                            .build())
                        .await;
                }
                Some(child_state) => {
                    let must_finish_branch_exec =
                        branch_exec.branch_index.iter().all(|(b_name, index)| {
                            index.clone()
                                == branches_by_name.get(b_name).unwrap().nodes.len() - 1
                        });
                    if must_finish_branch_exec {
                        tracing::info!("Branch execution over!");
                        repository
                            .write_workflow_execution(WriteWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .write(WriteRequest::UpdateNodeStatus(UpdateNodeStatusDetails {
                                    node_id: parent_state.node_id.clone(),
                                    state_id: parent_state_id.clone(),
                                    status: Status::Success,
                                }))
                                .build()).await;
                    } else {
                        let branch = branch_node
                            .branches
                            .iter()
                            .find(|branch| branch.nodes.contains(&child_state.node_id))
                            .unwrap();
                        let branch_index = branch_exec.branch_index.get(&branch.name).unwrap();
                        let mut branch_indexes = branch_exec.branch_index.clone();
                        if branch_index.clone() == branch.nodes.len() - 1 {
                            tracing::info!("Branch[{}] execution over!", branch.name);
                        } else {
                            branch_indexes.insert(branch.name.clone(), branch_index + 1);
                            let next_node_id = branch.nodes.get(branch_index.clone()).unwrap();
                            let next_state_id =
                                format!("{}_{}", branch.name, next_node_id.name);
                            repository
                                .write_workflow_execution(WriteWorkflowExecutionRequest::builder()
                                    .workflow_id(workflow.id.clone())
                                    .execution_id(workflow_execution.execution_id.clone())
                                    .write(WriteRequest::IncrementBranchIndex(IncrementBranchIndexDetails{
                                        node_id: parent_state.node_id.clone(),
                                        state_id: parent_state_id.clone(),
                                        branch_name: branch.name.clone(),
                                        current_branch_index: branch_index.clone(),
                                    }))
                                    .write(WriteRequest::InitiateNodeExec(InitiateNodeExecDetails {
                                        node_id: next_node_id.clone(),
                                        state_id: next_state_id,
                                        dept: path_so_far,
                                    }))
                                    .build())
                                .await;
                        }
                    }
                }
            }
        }
    }
}