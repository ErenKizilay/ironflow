use crate::auth::http::HttpAuthentication;
use crate::auth::provider::AuthProvider;
use crate::aws_lambda::client::LambdaClient;
use crate::config::configuration::{ConfigurationManager, ExecutionConfig};
use crate::execution::context::build_context;
use crate::execution::model::Execution::Condition;
use crate::execution::model::WorkflowExecutionError::AssertionFailed;
use crate::execution::model::{
    AssertionExecution, BranchExecution, ChainExecution, ConditionExecution,
    ContinueParentNodeExecutionCommand, Execution, ExecutionSource, NodeExecutionState,
    StartWorkflowCommand, Status, StepExecution, StepExecutionError, WorkflowExecution,
    WorkflowExecutionError, WorkflowExecutionIdentifier, WorkflowSource,
};
use crate::execution::step::StepExecutor;
use crate::execution::{assertion, branch, condition, loop_execution, step};
use crate::expression::expression::value_as_string;
use crate::http::http::{HttpClient, HttpRequest};
use crate::model::{Branch, Graph, HttpConfig, NodeConfig, NodeId, StepTarget};
use crate::persistence::model::{
    IncrementWorkflowIndexDetails, InitiateNodeExecDetails, InitiateWorkflowExecDetails,
    LockNodeExecDetails, UpdateNodeStatusDetails, UpdateWorkflowExecutionRequest, WriteRequest,
    WriteWorkflowExecutionRequest, WriteWorkflowExecutionRequestBuilder,
};
use crate::persistence::persistence::{PersistenceError, Repository};
use bon::Builder;
use jmespath::functions::Function;
use serde_json::Value::Object;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch::Sender;
use tokio::sync::{Mutex, RwLock};
use tokio_util::task::TaskTracker;
use tracing::{instrument, span, Level};
use tracing_subscriber::fmt::format;
use uuid::Uuid;

#[derive(Debug)]
pub struct WorkflowExecutor {
    execution_config: ExecutionConfig,
    configuration_manager: Arc<ConfigurationManager>,
    repository: Arc<Repository>,
    step_executor: Arc<StepExecutor>,
    poll_sender: Sender<SystemTime>,
}

impl WorkflowExecutor {
    pub async fn new(
        execution_config: ExecutionConfig,
        configuration_manager: Arc<ConfigurationManager>,
        repository: Arc<Repository>,
        auth_provider: Arc<AuthProvider>,
        poll_sender: Sender<SystemTime>,
    ) -> Self {
        Self {
            execution_config,
            configuration_manager,
            repository,
            step_executor: StepExecutor::new(auth_provider).await,
            poll_sender,
        }
    }

    pub async fn start(&self, command: StartWorkflowCommand) -> Result<String, String> {
        match self
            .configuration_manager
            .get_workflow(&command.workflow_id).await
        {
            Some(workflow) => {
                let workflow_id = &workflow.id;
                let node_ids = workflow.node_ids.clone();
                let first_node_id = node_ids.first().unwrap();
                let execution_id = command.execution_id
                    .map_or_else(|| Uuid::new_v4().to_string(), |exec_id| exec_id);
                self.poll_sender.send(SystemTime::now()).unwrap();
                self.repository
                    .write_workflow_execution(
                        WriteWorkflowExecutionRequestBuilder::new(
                            workflow_id.clone(),
                            execution_id.clone(),
                        )
                        .write(WriteRequest::InitiateWorkflowExecution(
                            InitiateWorkflowExecDetails {
                                input: command.input.clone(),
                                workflow: workflow.clone(),
                                source: command.source,
                                depth: command.dept_so_far.clone(),
                            },
                        ))
                        .write(WriteRequest::InitiateNodeExec(InitiateNodeExecDetails {
                            node_id: first_node_id.clone(),
                            state_id: first_node_id.name.clone(),
                            dept: vec![],
                        }))
                        .build(),
                    )
                    .await
                    .unwrap();
                Ok(execution_id.clone().to_string())
            }
            None => Err(format!("No workflow found with id {}", command.workflow_id)),
        }
    }

    #[instrument(level = "info", skip(self), fields(workflow_id = state.workflow_id, execution_id = state.execution_id))]
    pub async fn continue_execution(&self, state: &NodeExecutionState) -> Result<(), PersistenceError> {
        let workflow_execution = self
            .repository
            .get_workflow_execution(&state.workflow_id, &state.execution_id)
            .await
            .unwrap()
            .unwrap();
        if workflow_execution.status.is_complete() {
            tracing::info!("Workflow execution completed with status {:?}", workflow_execution.status);
            return Ok(());
        }
        let graph = &workflow_execution.workflow;
        let status = &state.status;
        let execution = &state.execution;
        self.poll_sender.send(SystemTime::now()).unwrap();
        tracing::info!(
            "Node[{}] execution status: {:?}",
            state.node_id.name,
            status
        );
        match status {
            Status::Queued | Status::WillRetried => {
                let exec_result = self.handle_queued_and_retry(state, &workflow_execution, graph)
                    .await;
                self.poll_sender.send(SystemTime::now()).unwrap();
                exec_result
            }
            Status::Success => {
                self.handle_success(state, &workflow_execution, graph).await
            }
            Status::InProgress => match execution.clone().unwrap() {
                Execution::Step(_) => {
                    unreachable!()
                }
                _ => {
                    self.continue_parent_node_exec(&workflow_execution, state, false)
                        .await
                }
            },
            Status::Failure => {
                let mut write_requests = Vec::new();
                if let ExecutionSource::Workflow(workflow_source) = &workflow_execution.source {
                    write_requests.push(WriteRequest::UpdateNodeStatus(UpdateNodeStatusDetails {
                        node_id: workflow_source.caller_node_id.clone(),
                        state_id: workflow_source.caller_node_state_id.clone(),
                        status: Status::Failure,
                    }));
                }
                write_requests.push(WriteRequest::UpdateWorkflowStatus(Status::Failure));
                self.repository
                    .write_workflow_execution(
                        WriteWorkflowExecutionRequest::builder()
                            .workflow_id(state.workflow_id.clone())
                            .execution_id(state.execution_id.clone())
                            .writes(write_requests)
                            .build(),
                    )
                    .await
            }
            Status::Running => {
                tracing::debug!("Running node: {:?}", state.node_id.name);
                Ok(())
            }
        };
        Ok(())
    }

    async fn handle_success(
        &self,
        state: &NodeExecutionState,
        workflow_execution: &WorkflowExecution,
        graph: &Graph,
    ) -> Result<(), PersistenceError> {
        let mut write_requests = Vec::new();
        if state.depth.is_empty() {
            let next_index = workflow_execution.index + 1;
            if next_index < graph.node_ids.len() {
                let next_node_id = &graph.node_ids.get(next_index).unwrap().clone();
                tracing::info!("Will queue: {:?}", next_node_id.clone().name);
                let state_id = next_node_id.clone().name;
                write_requests.extend(vec![
                    WriteRequest::IncrementWorkflowIndex(IncrementWorkflowIndexDetails {
                        increment: true,
                        current_index: workflow_execution.index,
                    }),
                    WriteRequest::InitiateNodeExec(InitiateNodeExecDetails {
                        node_id: next_node_id.clone(),
                        state_id,
                        dept: vec![],
                    }),
                ]);
            } else {
                write_requests.push(WriteRequest::UpdateWorkflowStatus(Status::Success));
                if let ExecutionSource::Workflow(workflow_source) = &workflow_execution.source {
                    self.repository
                        .write_workflow_execution(
                            WriteWorkflowExecutionRequest::builder()
                                .workflow_id(
                                    workflow_source.execution_identifier.workflow_id.clone(),
                                )
                                .execution_id(
                                    workflow_source.execution_identifier.execution_id.clone(),
                                )
                                .write(WriteRequest::UpdateNodeStatus(UpdateNodeStatusDetails {
                                    node_id: workflow_source.caller_node_id.clone(),
                                    state_id: workflow_source.caller_node_state_id.clone(),
                                    status: Status::Success,
                                }))
                                .build(),
                        )
                        .await
                        .unwrap();
                }
                tracing::info!("Workflow[{}] executed successfully", graph.id);
            }
            self.repository
                .write_workflow_execution(
                    WriteWorkflowExecutionRequest::builder()
                        .workflow_id(state.workflow_id.clone())
                        .execution_id(state.execution_id.clone())
                        .writes(write_requests)
                        .build(),
                )
                .await
        } else {
            self.continue_parent_node_exec(&workflow_execution, state, true)
                .await
        }
    }

    async fn handle_queued_and_retry(
        &self,
        state: &NodeExecutionState,
        workflow_execution: &WorkflowExecution,
        graph: &Graph,
    ) -> Result<(), PersistenceError> {
        let state_id = state.state_id.clone();
        let retry_count = resolve_retry_count(state);
        self.repository
            .write_workflow_execution(
                WriteWorkflowExecutionRequest::builder()
                    .workflow_id(state.workflow_id.clone())
                    .execution_id(state.execution_id.clone())
                    .write(WriteRequest::LockNodeExec(LockNodeExecDetails {
                        node_id: state.node_id.clone(),
                        state_id: state_id.clone(),
                        retry_count,
                    }))
                    .build(),
            )
            .await
            .unwrap();
        let node = graph.nodes_by_id.get(&state.node_id).unwrap();
        let node_execution_state = self.execute_node(&workflow_execution, state, node).await;
        let node_status = &node_execution_state.status;
        tracing::info!(
            "Node[{}] executed with status: {:?}, details: {:?}",
            state.node_id.name,
            node_status,
            node_execution_state.execution
        );
        self.repository
            .write_workflow_execution(
                WriteWorkflowExecutionRequest::builder()
                    .workflow_id(state.workflow_id.clone())
                    .execution_id(state.execution_id.clone())
                    .write(WriteRequest::SaveNodeExec(node_execution_state.clone()))
                    .build(),
            )
            .await
    }

    async fn continue_parent_node_exec(
        &self,
        workflow_execution: &WorkflowExecution,
        state: &NodeExecutionState,
        is_triggerred_by_child: bool,
    ) -> Result<(), PersistenceError> {
        let graph = &workflow_execution.workflow;
        let parent_state = if is_triggerred_by_child {
            let parent_node_id = &state.depth.last().unwrap().clone();
            let parent_node_state_id = workflow_execution.state_id_of_node(parent_node_id);
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
                Execution::Loop(_) => {
                    loop_execution::continue_execution(self.repository.clone(), continue_command)
                        .await
                }
                Execution::Branch(_) => {
                    branch::continue_execution(self.repository.clone(), continue_command).await
                }
                Condition(_) => {
                    condition::continue_execution(self.repository.clone(), continue_command).await
                }
                Execution::Workflow(workflow_execution_identifier) => {
                    tracing::info!(
                        "Chain Workflow[{:?}] execution is in progress",
                        workflow_execution_identifier
                    );
                    Ok(())
                }
            }
        } else {
            Ok(())
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
            node_id.name,
            depth,
            workflow_execution.index
        );
        let final_context =
            build_context(self.repository.clone(), workflow_execution, config, state).await;
        let retry_count = resolve_retry_count(state);
        let (status, execution) = match config {
            NodeConfig::StepNode(step_target) => {
                step::initiate_execution(
                    self.step_executor.clone(),
                    retry_count,
                    &step_target,
                    &workflow_execution,
                    final_context,
                )
                .await
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
            NodeConfig::LoopNode(loop_config) => {
                loop_execution::initiate_execution(loop_config, &final_context).await
            }
            NodeConfig::WorkflowNode(workflow_node_config) => {
                let final_context =
                    build_context(self.repository.clone(), workflow_execution, config, state).await;
                let input = workflow_node_config.input.resolve(final_context.clone());
                let child_workflow_id = value_as_string(
                    workflow_node_config
                        .workflow_id
                        .resolve(final_context.clone()),
                );
                let child_execution_id = workflow_node_config.execution_id.clone().map_or_else(
                    || Uuid::new_v4().to_string(),
                    |exec_id| value_as_string(exec_id.resolve(final_context)),
                );
                let child_execution_identifier = WorkflowExecutionIdentifier {
                    workflow_id: child_workflow_id.clone(),
                    execution_id: child_execution_id.clone(),
                };
                let mut depth_so_far = workflow_execution.depth.clone();
                if depth_so_far.len() > self.execution_config.max_workflow_chain_depth {
                    (
                        Status::Failure,
                        Execution::Workflow(ChainExecution {
                            child_identifier: child_execution_identifier.clone(),
                            error: Some(format!(
                                "Max chain depth exceeded: {}",
                                depth_so_far.len()
                            )),
                        }),
                    )
                } else {
                    let parent_workflow_exec_identifier = WorkflowExecutionIdentifier {
                        workflow_id: workflow.id.clone(),
                        execution_id: workflow_execution.execution_id.clone(),
                    };
                    depth_so_far.push(parent_workflow_exec_identifier.clone());
                    tracing::info!("will start chain workflow[{}] execution", child_workflow_id);
                    let start_result = self
                        .start(
                            StartWorkflowCommand::builder()
                                .workflow_id(child_workflow_id)
                                .execution_id(child_execution_id)
                                .input(input)
                                .source(ExecutionSource::Workflow(WorkflowSource {
                                    execution_identifier: parent_workflow_exec_identifier,
                                    caller_node_id: state.node_id.clone(),
                                    caller_node_state_id: state.state_id.clone(),
                                }))
                                .dept_so_far(depth_so_far)
                                .build(),
                        )
                        .await;
                    match start_result {
                        Ok(_) => (
                            Status::InProgress,
                            Execution::Workflow(ChainExecution {
                                child_identifier: child_execution_identifier.clone(),
                                error: None,
                            }),
                        ),
                        Err(err) => (
                            Status::Failure,
                            Execution::Workflow(ChainExecution {
                                child_identifier: child_execution_identifier.clone(),
                                error: Some(err),
                            }),
                        ),
                    }
                }
            }
        };
        NodeExecutionState {
            workflow_id: workflow.id.clone(),
            execution_id: workflow_execution.execution_id.clone(),
            state_id: state.state_id.clone(),
            node_id: node_id.clone(),
            status,
            execution: Some(execution),
            depth: depth.clone(),
            created_at: 0,
            updated_at: None,
        }
    }
}

fn resolve_retry_count(state: &NodeExecutionState) -> usize {
    let retry_count = match &state.execution {
        None => 0,
        Some(exec) => match exec {
            Execution::Step(step_exec) => step_exec.retry_count,
            _ => 0,
        },
    };
    retry_count
}
