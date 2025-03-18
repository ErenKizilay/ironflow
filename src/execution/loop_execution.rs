use crate::execution::model::Execution::Loop;
use crate::execution::model::{ContinueParentNodeExecutionCommand, Execution, LoopExecution, Status};
use crate::model::{LoopConfig, NodeConfig};
use crate::persistence::model::{IncrementLoopIndexDetails, InitiateNodeExecDetails, UpdateNodeStatusDetails, WriteRequest, WriteWorkflowExecutionRequest};
use crate::persistence::persistence::Repository;
use serde_json::Value;
use std::sync::Arc;

pub async fn initiate_execution(loop_config: &LoopConfig, context: &Value) -> (Status, Execution) {
    let condition_result = loop_config.array.resolve(context.clone());
    match condition_result {
        Value::Array(items) => {
            (Status::InProgress, Loop(LoopExecution {
                iteration_count: 0,
                index: 0,
                error: None,
                items,
            }))
        }
        _ => {
            (Status::Failure, Loop(LoopExecution {
                iteration_count: 0,
                index: 0,
                error: Some(format!("Expected an array, but found: {}", condition_result)),
                items: vec![],
            }))
        }
    }
}

pub async fn continue_execution(repository: Arc<Repository>, command: ContinueParentNodeExecutionCommand) {
    let parent_state = command.parent_state;
    let workflow_execution = command.workflow_execution;
    let workflow = workflow_execution.workflow.clone();
    let parent_state_id = workflow_execution.state_id_of_node(&parent_state.node_id);
    let loop_node = workflow.get_node(&parent_state.node_id).unwrap();
    match loop_node {
        NodeConfig::LoopNode(loop_config) => {
            if let Some(Loop(ref loop_execution)) = parent_state.execution {
                let items = &loop_execution.items;
                match command.child_state {
                    None => {
                        if loop_execution.iteration_count >= items.len() {
                            repository.write_workflow_execution(WriteWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .write(WriteRequest::UpdateNodeStatus(UpdateNodeStatusDetails {
                                    node_id: parent_state.node_id.clone(),
                                    state_id: parent_state_id.clone(),
                                    status: Status::Success,
                                }))
                                .build()).await.unwrap();
                        }
                        else {
                            let mut path_so_far = parent_state.depth.clone();
                            path_so_far.push(parent_state.node_id.clone());
                            let child_node_to_queue = loop_config.nodes.get(loop_execution.index).unwrap();
                            repository.write_workflow_execution(WriteWorkflowExecutionRequest::builder()
                                .workflow_id(workflow.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .write(WriteRequest::InitiateNodeExec(InitiateNodeExecDetails {
                                    node_id: child_node_to_queue.clone(),
                                    state_id: format!("{}_{}", child_node_to_queue.name, loop_execution.iteration_count),
                                    dept: path_so_far,
                                }))
                                .build()).await.unwrap();
                        }
                    }
                    Some(child_state) => {
                        repository.write_workflow_execution(WriteWorkflowExecutionRequest::builder()
                            .workflow_id(workflow.id.clone())
                            .execution_id(workflow_execution.execution_id.clone())
                            .write(WriteRequest::IncrementLoopIndex(IncrementLoopIndexDetails {
                                node_id: parent_state.node_id.clone(),
                                state_id: parent_state_id.clone(),
                                next_index: get_next_index(&loop_config, loop_execution),
                                iteration_count: get_iteration_count(&loop_config, loop_execution),
                            }))
                            .build()).await.unwrap();
                    }
                }
            }
        }
        _ => {
            unreachable!()
        }
    }
}

fn get_next_index(loop_config: &LoopConfig, loop_execution: &LoopExecution, ) -> usize {
    let number_of_nodes = loop_config.nodes.len();
    if loop_execution.index + 1 == number_of_nodes {
        0
    } else {
        loop_execution.index + 1
    }
}

fn get_iteration_count(loop_config: &LoopConfig, loop_execution: &LoopExecution, ) -> usize {
    let number_of_nodes = loop_config.nodes.len();
    if loop_execution.index + 1 == number_of_nodes {
        loop_execution.iteration_count + 1
    } else {
        loop_execution.iteration_count
    }
}