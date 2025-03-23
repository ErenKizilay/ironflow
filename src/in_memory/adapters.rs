use crate::execution::model::{Execution, ExecutionSource, NodeExecutionState, Status, WorkflowExecution};
use crate::listener::listener::Message;
use crate::listener::queue::QueuePort;
use crate::persistence::model::{WriteRequest, WriteWorkflowExecutionRequest};
use crate::persistence::persistence::{PersistenceError, PersistencePort};
use async_trait::async_trait;
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use aws_sdk_dynamodb::primitives::DateTime;
use tokio::sync::{mpsc, Mutex, RwLock};

pub struct InMemoryAdapter {
    queue_port: Arc<InMemoryQueueAdapter>,
    persistence_port: Arc<InMemoryRepositoryAdapter>
}
impl InMemoryAdapter {
    pub fn new() -> InMemoryAdapter {
        let (tx, rx) = mpsc::channel(32);
        InMemoryAdapter {
            queue_port: InMemoryQueueAdapter::new(rx),
            persistence_port: Arc::new(InMemoryRepositoryAdapter {
                sender: tx, node_table: Arc::new(Default::default()), workflow_execution_table: Arc::new(Default::default())
            }),
        }

    }
}

type InMemoryQueue = Arc<Mutex<LinkedList<Message>>>;
type NodeExecutionsTable = Arc<RwLock<HashMap<String, NodeExecutionState>>>;
type WorkflowExecutionsTable = Arc<RwLock<HashMap<String, WorkflowExecution>>>;

pub struct InMemoryQueueAdapter {
    receiver: Mutex<mpsc::Receiver<NodeExecutionState>>,
    queue: InMemoryQueue,
}


#[derive(Debug)]
pub struct InMemoryRepositoryAdapter  {
    sender: mpsc::Sender<NodeExecutionState>,
    node_table: NodeExecutionsTable,
    workflow_execution_table: WorkflowExecutionsTable
}

impl InMemoryQueueAdapter {
    pub fn new(receiver: mpsc::Receiver<NodeExecutionState>) -> Arc<InMemoryQueueAdapter> {
        let queue_adapter = Arc::new(InMemoryQueueAdapter {
            receiver: Mutex::new(receiver),
            queue: Arc::new(Default::default()),
        });
        let cloned = queue_adapter.clone();
        tokio::spawn(async move {
            while let Some(state) = cloned.receiver.lock().await.recv().await {
                cloned.queue.lock()
                    .await.push_back(Message {
                    node_execution_state: state.clone(),
                    id: build_uid_from_state(&state),
                })
            }
        });
        queue_adapter
    }
}

#[async_trait]
impl QueuePort for InMemoryQueueAdapter {
    async fn receive_messages(&self) -> Vec<Message> {
        self.queue.lock().await.pop_front()
            .map_or_else(Vec::new, |message| {vec![message]})
    }

    async fn delete_message(&self, message_id: &String) {
        //noop
    }
}


#[async_trait]
impl PersistencePort for  InMemoryRepositoryAdapter {
    async fn write_workflow_execution(&self, request: WriteWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        let execution_identifier = request.identifier;
        let workflow_execution_uid = build_workflow_execution_uid(&execution_identifier.workflow_id, &execution_identifier.execution_id);
        for write_request in request.write_requests {
            match write_request {
                WriteRequest::InitiateWorkflowExecution(initiate_workflow_exec_details) => {
                    self.workflow_execution_table.write()
                        .await.insert(workflow_execution_uid.clone(), WorkflowExecution {
                        execution_id: execution_identifier.execution_id.clone(),
                        source: initiate_workflow_exec_details.source,
                        input: initiate_workflow_exec_details.input,
                        index: 0,
                        status: Status::Queued,
                        state_keys_by_node_id: Default::default(),
                        last_executed_node_id: None,
                        workflow: initiate_workflow_exec_details.workflow,
                        depth: initiate_workflow_exec_details.depth,
                        started_at: now_i64(),
                        updated_at: None,
                    });
                }
                WriteRequest::IncrementWorkflowIndex(increment_workflow_index_details) => {
                    if increment_workflow_index_details.increment {
                        let mut existing = self.get_workflow_exec(&workflow_execution_uid).await;
                        existing.index = existing.index + 1;
                        self.update_workflow_exec(&existing).await;
                    }
                }
                WriteRequest::UpdateWorkflowStatus(status) => {
                    let mut existing = self.get_workflow_exec(&workflow_execution_uid).await;
                    if existing.status.is_complete() {
                        tracing::warn!("Workflow execution status is complete, will discard update..");
                    } else {
                        existing.status = status;
                        self.update_workflow_exec(&existing).await;
                    }
                }
                WriteRequest::InitiateNodeExec(initiate_node_exec_details) => {
                    let node_execution_state = NodeExecutionState {
                        workflow_id: execution_identifier.workflow_id.clone(),
                        execution_id: execution_identifier.execution_id.clone(),
                        state_id: initiate_node_exec_details.state_id,
                        node_id: initiate_node_exec_details.node_id,
                        status: Status::Queued,
                        execution: None,
                        depth: initiate_node_exec_details.dept,
                        created_at: now_i64(),
                        updated_at: None,
                    };
                    self.node_table.write()
                        .await.insert(build_uid_from_state(&node_execution_state), node_execution_state.clone());
                    self.sender.send(node_execution_state).await.unwrap();
                }
                WriteRequest::LockNodeExec(lock_node_exec_details) => {
                    let mut workflow_execution = self.get_workflow_exec(&workflow_execution_uid).await;
                    workflow_execution.state_keys_by_node_id.insert(lock_node_exec_details.node_id.clone(), lock_node_exec_details.state_id.clone());
                    workflow_execution.last_executed_node_id = Some(lock_node_exec_details.node_id);
                    self.update_workflow_exec(&workflow_execution).await;
                    let node_state_uid = build_node_state_uid(&execution_identifier.workflow_id, &execution_identifier.execution_id, &lock_node_exec_details.state_id);
                    let mut existing_node_exec = self.node_table.read()
                        .await.get(&node_state_uid)
                        .unwrap().clone();
                    existing_node_exec.updated_at = Some(now_i64());
                    existing_node_exec.status = Status::Running;
                    self.node_table.write().await.insert(node_state_uid, existing_node_exec.clone());
                    self.sender.send(existing_node_exec).await.unwrap();
                }
                WriteRequest::SaveNodeExec(node_exec_state) => {
                    let mut cloned = node_exec_state.clone();
                    cloned.updated_at = Some(now_i64());
                    self.node_table.write().await.insert(build_uid_from_state(&cloned), cloned.clone());
                    self.sender.send(cloned).await.unwrap();
                }
                WriteRequest::UpdateNodeStatus(update_node_status_details) => {
                    let node_state_uid = build_node_state_uid(&execution_identifier.workflow_id, &execution_identifier.execution_id, &update_node_status_details.state_id);
                    let mut existing_node_exec = self.node_table.read()
                        .await.get(&node_state_uid)
                        .unwrap().clone();
                    existing_node_exec.updated_at = Some(now_i64());
                    existing_node_exec.status = update_node_status_details.status;
                    self.node_table.write().await.insert(build_uid_from_state(&existing_node_exec), existing_node_exec.clone());
                    self.sender.send(existing_node_exec).await.unwrap();
                }
                WriteRequest::IncrementBranchIndex(increment_branch_index) => {
                    let node_state_uid = build_node_state_uid(&execution_identifier.workflow_id, &execution_identifier.execution_id, &increment_branch_index.state_id);
                    let mut existing_node_exec = self.node_table.read()
                        .await.get(&node_state_uid)
                        .unwrap().clone();
                    existing_node_exec.updated_at = Some(now_i64());
                    if let Some(Execution::Branch(branch_exec)) = existing_node_exec.execution {
                        let mut cloned = branch_exec.clone();
                        let current_index = cloned.branch_index.get(&increment_branch_index.branch_name).unwrap();
                        cloned.branch_index.insert(increment_branch_index.branch_name, current_index + 1);
                        existing_node_exec.updated_at = Some(now_i64());
                        existing_node_exec.execution = Some(Execution::Branch(cloned));
                        self.node_table.write().await.insert(build_uid_from_state(&existing_node_exec), existing_node_exec.clone());
                        self.sender.send(existing_node_exec).await.unwrap();
                    }
                }
                WriteRequest::IncrementConditionIndex(increment_condition_index) => {
                    let node_state_uid = build_node_state_uid(&execution_identifier.workflow_id, &execution_identifier.execution_id, &increment_condition_index.state_id);
                    let mut existing_node_exec = self.node_table.read()
                        .await.get(&node_state_uid)
                        .unwrap().clone();
                    if let Some(Execution::Condition(condition_exec)) = existing_node_exec.execution {
                        let mut cloned = condition_exec.clone();
                        cloned.index = condition_exec.index + 1;
                        existing_node_exec.updated_at = Some(now_i64());
                        existing_node_exec.execution = Some(Execution::Condition(cloned));
                        self.node_table.write().await.insert(build_uid_from_state(&existing_node_exec), existing_node_exec.clone());
                        self.sender.send(existing_node_exec).await.unwrap();
                    }
                }
                WriteRequest::IncrementLoopIndex(increment_loop_index) => {
                    let node_state_uid = build_node_state_uid(&execution_identifier.workflow_id, &execution_identifier.execution_id, &increment_loop_index.state_id);
                    let mut existing_node_exec = self.node_table.read()
                        .await.get(&node_state_uid)
                        .unwrap().clone();
                    if let Some(Execution::Loop(loop_exec)) = existing_node_exec.execution {
                        let mut cloned = loop_exec.clone();
                        cloned.index = increment_loop_index.next_index;
                        existing_node_exec.updated_at = Some(now_i64());
                        existing_node_exec.execution = Some(Execution::Loop(cloned));
                        self.node_table.write().await.insert(build_uid_from_state(&existing_node_exec), existing_node_exec.clone());
                        self.sender.send(existing_node_exec).await.unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_workflow_execution(&self, workflow_id: &String, execution_id: &String) -> Result<Option<WorkflowExecution>, PersistenceError> {
        let workflow_execution_uid = build_workflow_execution_uid(workflow_id, execution_id);
        Ok(self.workflow_execution_table.read().await.get(&workflow_execution_uid).cloned())
    }

    async fn get_node_execution(&self, workflow_id: &String, execution_id: &String, state_id: &String) -> Result<Option<NodeExecutionState>, PersistenceError> {
        let node_state_uid = build_node_state_uid(workflow_id, execution_id, state_id);
        Ok(self.node_table.read().await.get(&node_state_uid).cloned())
    }

    async fn get_node_executions(&self, workflow_id: &String, execution_id: &String, state_ids: Vec<String>) -> Vec<NodeExecutionState> {
        let mut states: Vec<NodeExecutionState> = vec![];
        for state_id in state_ids {
            let state_uid = build_node_state_uid(workflow_id, execution_id, &state_id);
            let optional_state = self.node_table.read().await.get(&state_uid).cloned();
            optional_state.inspect(|s|{
                states.push(s.clone());
            });
        }
        states
    }
}

impl InMemoryRepositoryAdapter {
    async fn get_workflow_exec(&self, workflow_execution_uid: &String) -> WorkflowExecution {
        let mut existing = self.workflow_execution_table.write()
            .await.get(workflow_execution_uid).unwrap().clone();
        existing
    }

    async fn update_workflow_exec(&self, workflow_execution: &WorkflowExecution) {
        let mut cloned = workflow_execution.clone();
        cloned.updated_at = Some(now_i64());
        self.workflow_execution_table.write()
            .await.insert(build_workflow_execution_uid(&workflow_execution.workflow.id, &workflow_execution.execution_id), cloned);
    }
}


static CONFIG: OnceLock<InMemoryAdapter> = OnceLock::new();

pub fn in_memory_persistence() -> &'static Arc<InMemoryRepositoryAdapter> {
    let in_memory_adapter = CONFIG.get_or_init(|| InMemoryAdapter::new());
    &in_memory_adapter.persistence_port
}

pub fn in_memory_queue() -> &'static Arc<InMemoryQueueAdapter> {
    let in_memory_adapter = CONFIG.get_or_init(|| InMemoryAdapter::new());
    &in_memory_adapter.queue_port
}

fn build_uid_from_state(state: &NodeExecutionState) -> String {
    build_node_state_uid(&state.workflow_id, &state.execution_id, &state.state_id)
}

fn build_workflow_execution_uid(workflow_id: &String, execution_id: &String) -> String {
    format!("{}_{}", workflow_id, execution_id)
}

fn build_node_state_uid(workflow_id: &String, execution_id: &String, state_id: &String) -> String {
    format!("{}_{}_{}", workflow_id, execution_id, state_id)
}

fn now_i64() -> i64 {
    DateTime::from(SystemTime::now()).to_millis().unwrap()
}