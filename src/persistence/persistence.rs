use crate::auth::http::AuthenticationProvider;
use crate::execution::model::{NodeExecutionState, Status, WorkflowExecution};
use crate::model::Graph;
use crate::persistence::model::UpdateWorkflowExecutionRequest;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct Repository {
    workflows: Mutex<HashMap<String, Graph>>,
    workflow_executions: Arc<Mutex<HashMap<String, WorkflowExecution>>>,
    node_executions: Arc<Mutex<HashMap<String, NodeExecutionState>>>,
    sender: Sender<NodeExecutionState>,
}

#[derive(Debug)]
pub enum PersistenceError {
    ConditionFailure(String),
    Internal(String),
}

impl Display for PersistenceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Repository {

    pub fn new(sender: Sender<NodeExecutionState>) -> Arc<Repository> {
        Arc::new(Repository {
            workflows: Mutex::new(Default::default()),
            workflow_executions: Arc::new(Mutex::new(Default::default())),
            node_executions: Arc::new(Mutex::new(Default::default())),
            sender,
        })
    }

    pub async fn close(&mut self) {
        println!("Closing repository");
        println!("Closed repository");
    }

    pub async fn initiate_node_execution(&self, state_id: String, state: NodeExecutionState) -> Result<NodeExecutionState, PersistenceError> {
        let node_execs = self.node_executions.clone();
        let existing = node_execs.lock().unwrap().get(&state_id).cloned();
        match existing {
            None => {
                node_execs.lock().unwrap().insert(state_id, state.clone());
                self.sender.send(state.clone()).await.expect("Cannot send message");
                Ok(state.clone())
            }
            Some(existing_state) => {
                match &existing_state.status {
                    Status::Success => {
                        tracing::warn!("Node execution already succeed!");
                        Ok(existing_state.clone())
                    }
                    _ => {
                        node_execs.lock().unwrap().insert(state_id, state.clone());
                        self.sender.send(state.clone()).await.expect("cannot send message");
                        Ok(state.clone())
                    }
                }
            }
        }
    }

    pub async fn initiate_node_executions(&self, executions: Vec<(String,NodeExecutionState)>) {
        for (state_id, state) in executions {
            self.initiate_node_execution(state_id, state).await.expect("TODO: cannot queue exec");
        }
    }

    pub async fn initiate_execution(&self, workflow: Graph, execution_id: String, input: Value, authentication_providers: Vec<AuthenticationProvider>) -> Result<WorkflowExecution, PersistenceError> {
        let execution = WorkflowExecution {
            execution_id: execution_id.clone(),
            input,
            index: 0,
            status: Status::Queued,
            result: None,
            state_keys_by_node_id: Default::default(),
            workflow,
            authentication_providers,
        };
        self.workflow_executions.clone().lock().unwrap().insert(execution_id, execution.clone());
        Ok(execution)
    }

    pub async fn get_workflow(&self, workflow_id: String) -> Result<Option<Graph>, PersistenceError> {
        Ok(self.workflows.lock()
            .unwrap()
            .get(&workflow_id).cloned())
    }

    pub async fn save_workflow(&self, graph: Graph) -> Result<Graph, PersistenceError> {
        self.workflows.lock().unwrap().insert(graph.id.clone(), graph.clone());
        Ok(graph.clone())
    }


    pub async fn get_workflow_execution(&self, workflow_id: &String, execution_id: &String) -> Result<Option<WorkflowExecution>, PersistenceError> {
        Ok(self.workflow_executions.clone().lock().unwrap().get(execution_id).cloned())
    }

    pub async fn get_node_execution(&self, workflow_id: &String, execution_id: &String, node_state_id: &String) -> Result<Option<NodeExecutionState>, PersistenceError> {
        Ok(self.node_executions.lock()
               .unwrap()
               .get(node_state_id)
               .cloned())
    }

    pub async fn get_node_executions(&self, workflow_id: &String, execution_id: &String, node_state_ids: Vec<String>) -> Result<Vec<NodeExecutionState>, PersistenceError> {
        let mut states : Vec<NodeExecutionState> = Vec::new();
        for node_state_id in node_state_ids {
            let result = self.get_node_execution(workflow_id, execution_id, &node_state_id).await.unwrap();
            if let state = Some(&result.unwrap()) {
                if state.is_some() {
                    states.push(state.unwrap().clone());
                }
            }
        }
        Ok(states)
    }

    pub async fn update_execution(&self, request: UpdateWorkflowExecutionRequest) {
        let workflow_execution = self.workflow_executions.clone()
            .lock().unwrap().get(&request.execution_id)
            .unwrap()
            .clone();
        let mut node_states_by_id = workflow_execution.clone().state_keys_by_node_id;
        request.state_keys.iter().for_each(|(node_id, state_id)| {
           node_states_by_id.insert(node_id.clone(), state_id.clone());
        });
        let updated_wf_exec = WorkflowExecution {
            workflow: workflow_execution.workflow.clone(),
            authentication_providers: workflow_execution.authentication_providers.clone(),
            execution_id: workflow_execution.execution_id.clone(),
            input: workflow_execution.input.clone(),
            index: workflow_execution.index + if request.increment_index { 1 } else { 0 },
            status: request.status.clone(),
            result: request.result.clone(),
            state_keys_by_node_id: node_states_by_id,
        };
        self.workflow_executions.clone().lock().unwrap().insert(request.execution_id.clone(), updated_wf_exec.clone());

        for (node_id, node_state) in request.node_states_by_id.iter() {
            self.initiate_node_execution(node_id.clone(), node_state.clone()).await.unwrap();
        }

        match request.parent_execution_updates_by_state_id {
            Some(parent_state_updates) => {
                parent_state_updates.iter().for_each(|(node_id, parent_exec)| {
                    let mut node_executions = self.node_executions.lock().unwrap();
                    let existing_parent_state = node_executions.get(node_id).unwrap();
                    let new_parent_state = NodeExecutionState {
                        workflow_id: existing_parent_state.workflow_id.clone(),
                        execution_id: existing_parent_state.execution_id.clone(),
                        node_id: existing_parent_state.node_id.clone(),
                        status: existing_parent_state.status.clone(),
                        execution: Some(parent_exec.clone()),
                        depth: existing_parent_state.depth.clone(),
                    };
                    node_executions.insert(node_id.clone(), new_parent_state);
                })
            }
            _ => {}
        }
    }
}