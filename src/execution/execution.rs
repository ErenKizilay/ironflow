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
                    self.continue_branch_exec(continue_command).await;
                }
                Condition(_) => {
                    self.continue_condition_exec(continue_command).await;
                }
            }
        }
    }

    async fn continue_condition_exec(&self, command: ContinueParentNodeExecutionCommand) {
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
                        self.repository
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
                        self.repository
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

    async fn execute_http_step(
        &self,
        context: &Value,
        http_config: &HttpConfig,
        retry_count: usize,
        workflow_execution: &WorkflowExecution,
    ) -> (Status, Execution) {
        let http_request = self.build_http_request(workflow_execution, http_config, context);
        let http_result = self.http_client.execute2(http_request).await;
        let attempt_number = retry_count + 1;
        match http_result {
            Ok(response) => {
                if response.status().is_success() {
                    let response_value = response.text().await.map_or_else(
                        |_| Value::Null,
                        |text| serde_json::from_str(text.as_str())
                            .map_or_else(|_| Value::Null, |val| val),
                    );
                    tracing::debug!("HTTP Execution successful: {:?}", response_value);
                    (
                        Status::Success,
                        Execution::Step(StepExecution {
                            retry_count: attempt_number,
                            result: Ok(response_value),
                        }),
                    )
                } else {
                    tracing::warn!("HTTP error response: {:?}", response);
                    (
                        Status::Failure,
                        Execution::Step(StepExecution {
                            retry_count: attempt_number,
                            result: Err(StepExecutionError::RunFailed(
                                response.text().await.unwrap(),
                            )),
                        }),
                    )
                }
            }
            Err(http_error) => {
                tracing::warn!("HTTP Execution error: {:?}", http_error);
                (
                    Status::Failure,
                    Execution::Step(StepExecution {
                        retry_count: attempt_number,
                        result: Err(StepExecutionError::RunFailed(http_error.to_string())),
                    }),
                )
            },
        }
    }

    fn build_http_request(&self, workflow_execution: &WorkflowExecution, config: &HttpConfig, context: &Value) -> HttpRequest {
        let mut headers = HashMap::new();
        let mut params = HashMap::new();
        let url = config.url.resolve(context.clone())
            .to_string()
            .as_str()
            .trim_matches('"')
            .to_string();
        workflow_execution.authentication_providers
            .iter()
            .filter(|authentication_provider|url.contains(&authentication_provider.auth.host))
            .for_each(|authentication_provider| {
                authentication_provider.auth.to_header()
                    .iter()
                    .next()
                    .inspect(|(key, value)| {
                        headers.insert(key.to_string(), value.to_string());
                    });
            });
        headers.insert("Content-Type".to_string(), config.content_type.clone());
        config.headers.iter()
            .for_each(|(key, dynamic_value)| {
                headers.insert(
                    key.clone(),
                    dynamic_value.resolve(context.clone()).to_string(),
                );
            });
        config.params.iter()
            .for_each(|(key, dynamic_value)| {
                params.insert(
                    key.clone(),
                    dynamic_value.resolve(context.clone()).to_string(),
                );
            });
        HttpRequest::builder()
            .url(url)
            .method(config.method.clone())
            .headers(headers)
            .params(params)
            .maybe_body(match &config.body {
                Some(body) => {
                    Some(body.resolve(context.clone()).to_string())
                }
                _ => { None }
            })
            .build()
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

        let (status, execution) = match config {
            NodeConfig::StepNode(step_target) => {
                let retry_count = resolve_retry_count(state);
                match step_target {
                    StepTarget::Lambda(lambda_config) => (
                        Status::Success,
                        Execution::Step(StepExecution {
                            retry_count: retry_count + 1,
                            result: Ok(Value::String("asddas".to_string())),
                        }),
                    ),
                    StepTarget::Http(http_config) => {
                        self.execute_http_step(&final_context, http_config, retry_count, &workflow_execution)
                            .await
                    }
                }
            }

            NodeConfig::ConditionNode(condition_config) => {
                let condition_result = condition_config.expression.evaluate(final_context);
                (
                    Status::InProgress,
                    Condition(ConditionExecution {
                        true_branch: condition_result.is_boolean() && condition_result.as_bool().unwrap(),
                        index: 0,
                    }),
                )

            }
            NodeConfig::BranchNode(branch_config) => {
                let branch_index: HashMap<String, usize> = branch_config
                    .branches
                    .iter()
                    .filter(|branch| {
                        match &branch.condition {
                            None => {true}
                            Some(expr) => {
                                let condition_result = expr.evaluate(final_context.clone());
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
            NodeConfig::AssertionNode(assertion_config) => {
                let errors: Vec<Option<String>> = assertion_config.assertions
                    .iter()
                    .map(|assertion| assertion.evaluate(&final_context))
                    .collect();
                let all_passed = errors.iter().all(|error| error.is_none());
                let assertion_exec = Execution::Assertion(AssertionExecution { passed: all_passed, errors });
                if all_passed {
                    (Status::Success, assertion_exec)
                } else {
                    (Status::Failure, assertion_exec)
                }
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

    async fn continue_branch_exec(&self, command: ContinueParentNodeExecutionCommand) {
        let parent_state = command.parent_state;
        let workflow_execution = command.workflow_execution;
        let workflow = workflow_execution.workflow.clone();
        let parent_state_id = workflow_execution.get_state_id_of_node(&parent_state.node_id);
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
                        let queued_executions: Vec<(String, NodeExecutionState)> = branch_exec
                            .branch_index
                            .iter()
                            .map(|(branch_name, branch_index)| {
                                let branch = branches_by_name.get(branch_name).unwrap();
                                let child_node_id: &NodeId =
                                    branch.nodes.get(branch_index.clone()).unwrap();
                                let child_state_id =
                                    format!("{}_{}", branch.name, child_node_id.name);
                                (
                                    child_state_id,
                                    NodeExecutionState {
                                        workflow_id: workflow.id.clone(),
                                        execution_id: workflow_execution.execution_id.clone(),
                                        node_id: child_node_id.clone(),
                                        status: Status::Queued,
                                        execution: None,
                                        depth: path_so_far.clone(),
                                    },
                                )
                            })
                            .collect();
                        self.repository
                            .initiate_node_executions(queued_executions)
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
                            self.repository
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
                                                execution_id: workflow_execution
                                                    .execution_id
                                                    .clone(),
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
                                self.repository
                                    .update_execution(
                                        UpdateWorkflowExecutionRequest::builder()
                                            .workflow_id(workflow.id.clone())
                                            .execution_id(workflow_execution.execution_id.clone())
                                            .status(Status::InProgress)
                                            .increment_index(false)
                                            .parent_execution_updates_by_state_id(HashMap::from([
                                                (
                                                    parent_state_id.clone(),
                                                    Execution::Branch(BranchExecution {
                                                        branch_index: branch_indexes,
                                                    }),
                                                ),
                                            ]))
                                            .node_states_by_id(vec![(
                                                next_state_id.clone(),
                                                NodeExecutionState {
                                                    workflow_id: workflow.id.clone(),
                                                    execution_id: workflow_execution
                                                        .execution_id
                                                        .clone(),
                                                    node_id: next_node_id.clone(),
                                                    status: Status::Queued,
                                                    execution: None,
                                                    depth: path_so_far,
                                                },
                                            )])
                                            .state_keys(vec![(next_node_id.clone(), next_state_id)])
                                            .build(),
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }
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
struct ContinueParentNodeExecutionCommand {
    workflow_execution: WorkflowExecution,
    parent_state: NodeExecutionState,
    child_state: Option<NodeExecutionState>,
}
