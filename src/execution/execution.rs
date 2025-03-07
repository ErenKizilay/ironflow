use crate::auth::http::AuthenticationProvider;
use crate::execution::model::Execution::Condition;
use crate::execution::model::{AssertionExecution, BranchExecution, ConditionExecution, Execution, NodeExecutionState, Status, StepExecution, StepExecutionError, WorkflowExecution, WorkflowExecutionError};
use crate::http::http::{HttpClient, HttpRequest};
use crate::model::{Branch, Graph, HttpConfig, NodeConfig, NodeId, StepTarget};
use crate::persistence::model::UpdateWorkflowExecutionRequest;
use crate::persistence::persistence::Repository;
use bon::Builder;
use jmespath::functions::Function;
use serde_json::Value::Object;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use crate::execution::{assertion, branch, condition, step};

#[derive(Debug)]
pub struct WorkflowExecutor {
    repository: Arc<Repository>,
    receiver: Arc<Mutex<Receiver<NodeExecutionState>>>,
    http_client: Arc<HttpClient>,
    task_tracker: TaskTracker,
}

impl WorkflowExecutor {
    pub fn new(repository: Arc<Repository>, receiver: Arc<Mutex<Receiver<NodeExecutionState>>>) -> Self {
        Self {
            repository,
            receiver,
            http_client: HttpClient::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn listen(self: Arc<Self>) {
        let cloned_receiver = self.receiver.clone();
        let mut rx = cloned_receiver.lock().await;
        while let Some(state) = rx.recv().await {
            tracing::info!("WorkflowExecutor received state: {:?}", state);
            let executor = Arc::clone(&self);
            self.task_tracker.spawn(async move {
                executor.continue_execution(&state).await;
            });
        }
        tracing::info!("Receiver closed, exiting listen()");
    }

    pub async fn stop(&self) {
        self.task_tracker.close();
        self.task_tracker.wait().await;
        drop(self.receiver.lock().await);
        tracing::info!("WorkflowExecutor stopped");
    }

    pub async fn start(&self, workflow: Graph, execution_id: String, input: Value, authentication_providers: Vec<AuthenticationProvider>) {
        if workflow.node_ids.is_empty() {
            tracing::warn!("No nodes found");
        } else {
            let first_node_id = workflow.node_ids.first().unwrap();
            let init_exec_result = self
                .repository
                .initiate_execution(workflow.clone(), execution_id.clone(), input, authentication_providers)
                .await;
            match init_exec_result {
                Ok(workflow_exec) => {
                    let queued_state = NodeExecutionState {
                        workflow_id: workflow.id.clone(),
                        execution_id,
                        node_id: first_node_id.clone(),
                        status: Status::Queued,
                        execution: None,
                        depth: vec![],
                    };
                    self.repository
                        .initiate_node_execution(
                            first_node_id.name.clone(),
                            queued_state,
                        )
                        .await
                        .expect("TODO: panic message");
                }
                Err(error) => {
                    tracing::error!("Error initiating execution: {}", error);
                }
            }
        }
    }

    //#[tracing::instrument]
    pub async fn continue_execution(&self, state: &NodeExecutionState) {
        let workflow_execution = self
            .repository
            .get_workflow_execution(&state.workflow_id, &state.execution_id)
            .await
            .unwrap()
            .unwrap();
        let graph = &workflow_execution.workflow;
        let status = &state.status;
        let execution = &state.execution;
        let depth = &state.depth;

        let all_nodes_are_visited = workflow_execution.index >= graph.node_ids.len();
        let workflow_completed = workflow_execution.status.is_complete();
        if all_nodes_are_visited || workflow_completed {
            tracing::info!("Workflow[{}] execution over!", graph.id);
            self.repository
                .update_execution(
                    UpdateWorkflowExecutionRequest::builder()
                        .workflow_id(graph.id.clone())
                        .execution_id(workflow_execution.execution_id.clone())
                        .status(if workflow_completed {workflow_execution.status.clone()} else {Status::Success})
                        .increment_index(false)
                        .node_states_by_id(vec![])
                        .state_keys(vec![])
                        .build(),
                )
                .await;
            return;
        }
        tracing::info!("Node[{}] execution status: {:?}", state.node_id.name, status);
        match status {
            Status::Queued => {
                let node = graph.nodes_by_id.get(&state.node_id).unwrap();
                let node_execution_state = self.execute_node(&workflow_execution, state, node)
                    .await;
                let state_id = node_execution_state.clone().node_id.name;
                if resolve_retry_count(state) > 1 {
                    tracing::warn!("Retrying node[{:?}] execution...", state.node_id.name);
                }
                self.repository.update_execution(
                        UpdateWorkflowExecutionRequest::builder()
                            .workflow_id(graph.id.clone())
                            .execution_id(workflow_execution.execution_id.clone())
                            .status(Status::InProgress)
                            .increment_index(depth.is_empty() && Status::Success.eq(&node_execution_state.status))
                            .node_states_by_id(vec![(
                                state_id.clone(),
                                node_execution_state.clone(),
                            )])
                            .state_keys(vec![(
                                node_execution_state.clone().node_id.clone(),
                                state_id,
                            )])
                            .build(),
                    )
                    .await;
            }
            Status::Success => {
                if depth.is_empty() {
                    let next_node_id = &graph.node_ids
                        .get(workflow_execution.index)
                        .unwrap()
                        .clone();
                    tracing::info!("Will queue: {:?}", next_node_id.clone().name);
                    let state_id = next_node_id.clone().name;
                    self.repository
                        .update_execution(
                            UpdateWorkflowExecutionRequest::builder()
                                .workflow_id(graph.id.clone())
                                .execution_id(workflow_execution.execution_id.clone())
                                .status(Status::InProgress)
                                .increment_index(false)
                                .node_states_by_id(vec![(
                                    state_id.clone(),
                                    NodeExecutionState {
                                        workflow_id: graph.id.clone(),
                                        execution_id: workflow_execution.execution_id.clone(),
                                        node_id: next_node_id.clone(),
                                        status: Status::Queued,
                                        execution: None,
                                        depth: vec![],
                                    },
                                )])
                                .state_keys(vec![])
                                .build(),
                        )
                        .await;
                } else {
                    self.continue_parent_node_exec(&workflow_execution, state, true)
                        .await;
                }
            }
            Status::InProgress => {
                match execution.clone().unwrap() {
                    Execution::Step(step_exec) => {
                        unreachable!()
                    }
                    _ => {
                        self.continue_parent_node_exec(&workflow_execution, state, false)
                            .await;
                    }
                }
            },
            Status::Failure => {
                if let Some(execution) = execution {
                    match execution {
                        Execution::Step(step_exec) => {
                            let retry_count = resolve_retry_count(state);
                            let max_retry_count = graph.config.max_retry_count.unwrap();
                            let new_status = if retry_count > max_retry_count { Status::Failure } else { Status::InProgress };
                            self.repository
                                .update_execution(UpdateWorkflowExecutionRequest::builder()
                                        .workflow_id(graph.id.clone())
                                        .execution_id(workflow_execution.execution_id.clone())
                                        .status(new_status.clone())
                                        .increment_index(false)
                                        .node_states_by_id(vec![(
                                            state.node_id.clone().name,
                                            NodeExecutionState {
                                                workflow_id: graph.id.clone(),
                                                execution_id: workflow_execution.execution_id.clone(),
                                                node_id: state.node_id.clone(),
                                                status: if retry_count > max_retry_count { Status::Failure } else { Status::Queued },
                                                execution: Some(Execution::Step(step_exec.clone())),
                                                depth: depth.clone(),
                                            },
                                        )])
                                        .state_keys(vec![])
                                                      .maybe_result(if Status::Failure.eq(&new_status) {
                                                          Some(Err(WorkflowExecutionError::StepFailed(state.node_id.clone().name)))
                                                      } else { None })
                                        .build(),
                                )
                                .await;
                        }
                        Execution::Loop(_) => {}
                        Execution::Branch(_) => {}
                        Condition(_) => {}
                        Execution::Assertion(assertion_exec) => {
                            self.repository
                                .update_execution(UpdateWorkflowExecutionRequest::builder()
                                                      .workflow_id(graph.id.clone())
                                                      .execution_id(workflow_execution.execution_id.clone())
                                                      .status(Status::Failure)
                                                      .increment_index(false)
                                                      .node_states_by_id(vec![])
                                                      .state_keys(vec![])
                                                      .build(),
                                )
                                .await;
                        }
                    }
                }
            }
        }
    }

    async fn continue_parent_node_exec(
        &self,
        workflow_execution: &WorkflowExecution,
        state: &NodeExecutionState,
        is_triggerred_by_child: bool,
    ) {
        let graph = &workflow_execution.workflow;
        let parent_state = if is_triggerred_by_child {
            let parent_node_id = &state.depth.last().unwrap().clone();
            let parent_node_state_id = workflow_execution.get_state_id_of_node(parent_node_id);
            self.repository
                .get_node_execution(
                    &graph.id,
                    &workflow_execution.execution_id,
                    &parent_node_state_id,
                )
                .await
                .unwrap()
                .unwrap()
        } else {
            state.clone()
        };

        let continue_command = ContinueParentNodeExecutionCommand::builder()
            .workflow_execution(workflow_execution.clone())
            .parent_state(parent_state)
            .maybe_child_state(if is_triggerred_by_child {
                Some(state.clone())
            } else {
                None
            })
            .build();

        if let Some(execution) = &continue_command.parent_state.execution {
            match execution {
                Execution::Step(_) | Execution::Assertion(_) => {
                    unreachable!();
                }
                Execution::Loop(_) => {}
                Execution::Branch(_) => {
                    branch::continue_execution(self.repository.clone(), continue_command).await;
                }
                Condition(_) => {
                    condition::continue_execution(self.repository.clone(), continue_command).await;
                }
            }
        }
    }

    async fn execute_node(
        &self,
        workflow_execution: &WorkflowExecution,
        state: &NodeExecutionState,
        config: &NodeConfig,
    ) -> NodeExecutionState {
        let workflow = &workflow_execution.workflow;
        let node_id = &state.node_id;
        let depth = &state.depth;
        tracing::info!(
            "Executing node {:?} with depth {:?} wf index {:?}",
            node_id.name, depth, workflow_execution.index);
        let state_keys_by_node_ids = &workflow_execution.state_keys_by_node_id;
        let referred_node_ids: Vec<NodeId> = config
            .get_expressions()
            .iter()
            .flat_map(|expr| expr.get_referred_nodes(state_keys_by_node_ids.clone()
                .into_keys()
                .collect()))
            .collect();
        tracing::debug!("referred_node_ids: {:?}", referred_node_ids);
        let referred_state_ids: Vec<String> = referred_node_ids
            .iter()
            .map(|node_id| state_keys_by_node_ids.get(node_id))
            .filter(|key| key.is_some())
            .map(|key| key.unwrap().clone())
            .collect();
        let referred_node_states = self.repository
            .get_node_executions(
                &workflow.id.clone(),
                &workflow_execution.execution_id,
                referred_state_ids,
            )
            .await
            .unwrap_or_default();

        let mut context: Map<String, Value> = referred_node_states
            .iter()
            .filter(|state| state.execution.is_some())
            .map(|state| (state.node_id.clone().name, state.get_context()))
            .collect();
        context.insert("input".to_string(), workflow_execution.input.clone());
        let final_context = Object(context);
        let retry_count = resolve_retry_count(state);
        let (status, execution) = match config {
            NodeConfig::StepNode(step_target) => {
                step::initiate_execution(self.http_client.clone(), retry_count, &step_target, &workflow_execution, final_context).await
            }

            NodeConfig::ConditionNode(condition_config) => {
                condition::initiate_execution(condition_config, &final_context).await
            }
            NodeConfig::BranchNode(branch_config) => {
                branch::initiate_execution(branch_config, &final_context).await
            }
            NodeConfig::AssertionNode(assertion_config) => {
                assertion::initiate_execution(assertion_config, final_context).await
            }
        };
        NodeExecutionState {
            workflow_id: workflow.id.clone(),
            execution_id: workflow_execution.execution_id.clone(),
            node_id: node_id.clone(),
            status,
            execution: Some(execution),
            depth: depth.clone(),
        }
    }
}

fn resolve_retry_count(state: &NodeExecutionState) -> usize {
    let retry_count = match &state.execution {
        None => { 0 }
        Some(exec) => {
            match exec {
                Execution::Step(step_exec) => { step_exec.retry_count }
                _ => { 0 }
            }
        }
    };
    retry_count
}

//if child_state is empty then parent node execution triggerred by itself meaning that it is just started
#[derive(Builder)]
pub(crate) struct ContinueParentNodeExecutionCommand {
    pub(crate) workflow_execution: WorkflowExecution,
    pub(crate) parent_state: NodeExecutionState,
    pub(crate) child_state: Option<NodeExecutionState>,
}
