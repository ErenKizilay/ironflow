use crate::execution::model::{NodeExecutionState, Status, WorkflowExecution, WorkflowExecutionError, WorkflowExecutionIdentifier};
use crate::model::NodeId;
use crate::persistence::model::{WriteRequest, WriteWorkflowExecutionRequest};
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::config::BehaviorVersion;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::get_item::{GetItemError, GetItemOutput};
use aws_sdk_dynamodb::operation::transact_get_items::{TransactGetItemsError, TransactGetItemsOutput};
use aws_sdk_dynamodb::operation::transact_write_items::{TransactWriteItemsError, TransactWriteItemsOutput};
use aws_sdk_dynamodb::primitives::DateTime;
use aws_sdk_dynamodb::types::{AttributeValue, Get, Put, ReturnValuesOnConditionCheckFailure, TransactGetItem, TransactWriteItem, Update};
use aws_sdk_dynamodb::Client;
use clap::builder::{Str, TypedValueParser};
use reqwest::get;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_dynamo::aws_sdk_dynamodb_1::from_item;
use serde_dynamo::{to_attribute_value, to_item};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use async_trait::async_trait;
use tracing::error;
use tracing_subscriber::fmt::format;
use crate::persistence::persistence::{PersistenceError, PersistencePort};

pub struct DynamoDbRepository {
    client: Arc<Client>,
}

#[async_trait]
impl PersistencePort for DynamoDbRepository {
    async fn write_workflow_execution(&self, request: WriteWorkflowExecutionRequest) -> Result<(), PersistenceError> {
        let write_items: Vec<TransactWriteItem> = request.write_requests
            .iter()
            .flat_map(|write_request: &WriteRequest| write_request.clone().to_transact_write_item(&request.identifier))
            .collect();
        let aws_result = self.client.transact_write_items()
            .set_transact_items(Some(write_items))
            .send().await;

        match aws_result {
            Ok(_) => Ok(()),
            Err(err) => {
                Err(PersistenceError::ConditionFailure(err.to_string()))
            }
        }
    }

    async fn get_workflow_execution(&self, workflow_id: &String, execution_id: &String) -> Result<Option<WorkflowExecution>, PersistenceError> {
        Ok(self.get_item(get_workflow_executions_table_name(), build_workflow_execution_key(workflow_id, execution_id)).await)
    }

    async fn get_node_execution(&self, workflow_id: &String, execution_id: &String, state_id: &String) -> Result<Option<NodeExecutionState>, PersistenceError> {
        Ok(self.get_item(get_node_executions_table_name(), build_node_state_key(workflow_id, execution_id, state_id)).await)
    }

    async fn get_node_executions(&self, workflow_id: &String, execution_id: &String, state_ids: Vec<String>) -> Vec<NodeExecutionState> {
        let unique_state_ids: HashSet<String> = state_ids.iter().cloned().collect();
        if unique_state_ids.is_empty() {
            return vec![];
        }
        let gets = unique_state_ids.iter()
            .map(|state_id| TransactGetItem::builder()
                .get(Get::builder()
                    .table_name(get_node_executions_table_name())
                    .set_key(Some(build_node_state_key(workflow_id, execution_id, state_id)))
                    .build().unwrap())
                .build())
            .collect();
        let result = self.client.transact_get_items()
            .set_transact_items(Some(gets))
            .send().await;
        match result {
            Ok(get_output) => {
                get_output.responses
                    .map_or(vec![], |responses|{
                        responses.iter()
                            .map(|item_response|{
                                match &item_response.item {
                                    None => {None}
                                    Some(item) => {
                                        Some(from_item(item.clone()).unwrap())
                                    }
                                }
                            })
                            .filter(|item| item.is_some())
                            .map(|item| item.unwrap())
                            .collect()
                    })
            }
            Err(err) => {
                error!("{:?}", err);
                vec![]
            }
        }
    }
}


impl DynamoDbRepository {
    pub async fn new() -> Self {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        DynamoDbRepository {
            client: Arc::new(client),
        }
    }

    async fn get_item<T: Serialize + DeserializeOwned>(&self, table_name: impl Into<String>, key: HashMap<String, AttributeValue>) -> Option<T> {
        let result = self.client
            .get_item()
            .set_key(Some(key))
            .table_name(table_name)
            .consistent_read(true)
            .send().await;
        match result {
            Ok(output) => {
                match output.item {
                    None => {
                        tracing::warn!("No field returned");
                        None
                    }
                    Some(item) => {
                        Some(from_item(item).unwrap())
                    }
                }
            }
            Err(err) => {
                tracing::error!("get_item error: {:?}", err.to_string());
                None
            }
        }
    }
}

impl WriteRequest {

    fn to_transact_write_item(self, execution_identifier: &WorkflowExecutionIdentifier) -> Vec<TransactWriteItem> {
        let workflow_id = &execution_identifier.workflow_id;
        let execution_id = &execution_identifier.execution_id;
        match self {
            WriteRequest::InitiateWorkflowExecution(initiate_workflow_exec_details) => {
                let workflow_execution = WorkflowExecution {
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
                };
                vec![TransactWriteItem::builder()
                    .put(Put::builder()
                        .table_name(get_workflow_executions_table_name())
                        .set_item(Some(build_workflow_execution_item(workflow_execution)))
                        .condition_expression("attribute_not_exists(#pk)")
                        .expression_attribute_names("#pk", build_node_state_pk())
                        .build().unwrap())
                    .build()]
            }
            WriteRequest::IncrementWorkflowIndex(increment_workflow_index_details) => {
                if increment_workflow_index_details.increment {
                    vec![TransactWriteItem::builder()
                        .update(Update::builder()
                            .table_name(get_workflow_executions_table_name())
                            .set_key(Some(build_workflow_execution_key(workflow_id, execution_id)))
                            .update_expression("add #index :one set #updated_at = :updated_at")
                            .condition_expression("#index = :current_index")
                            .expression_attribute_names("#index", "index")
                            .expression_attribute_names("#updated_at", "updated_at")
                            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
                            .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                            .expression_attribute_values(":current_index", to_attribute_value(increment_workflow_index_details.current_index).unwrap())
                            .build().unwrap())
                        .build()]
                } else {
                    vec![]
                }
            }
            WriteRequest::UpdateWorkflowStatus(status) => {
                vec![TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_workflow_executions_table_name())
                        .set_key(Some(build_workflow_execution_key(workflow_id, execution_id)))
                        .update_expression("SET #status = :status, #updated_at = :updated_at")
                        .condition_expression("NOT #status in (:success, :failure)")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":success", to_attribute_value(Status::Success).unwrap())
                        .expression_attribute_values(":failure", to_attribute_value(Status::Failure).unwrap())
                        .expression_attribute_values(":status", to_attribute_value(status).unwrap())
                        .build().unwrap())
                    .build()]
            }
            WriteRequest::InitiateNodeExec(initiate_node_exec_details) => {
                vec![TransactWriteItem::builder()
                    .put(Put::builder()
                        .table_name(get_node_executions_table_name())
                        .set_item(Some(build_node_execution_item(NodeExecutionState {
                            workflow_id: workflow_id.clone(),
                            execution_id: execution_id.clone(),
                            state_id: initiate_node_exec_details.state_id.clone(),
                            node_id: initiate_node_exec_details.node_id.clone(),
                            status: Status::Queued,
                            execution: None,
                            depth: initiate_node_exec_details.dept.clone(),
                            created_at: now_i64(),
                            updated_at: None,
                        })))
                        .condition_expression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                        .expression_attribute_names("#pk", build_node_state_pk())
                        .expression_attribute_names("#sk", build_node_state_sk())
                        .build().unwrap()).build(),
                ]
            }
            WriteRequest::LockNodeExec(lock_node_exec_details) => {
                vec![TransactWriteItem::builder()
                    .update(Update::builder()
                        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &lock_node_exec_details.state_id)))
                        .update_expression("SET #status = :status, #updated_at = :updated_at")
                        .condition_expression("(#status IN (:queued, :in_progress)) AND (attribute_not_exists(#execution) OR attribute_not_exists(#execution.Step) OR attribute_not_exists(#execution.Step.#retry_count) OR #execution.Step.#retry_count = :retry_count)")
                        .expression_attribute_names("#execution", "execution")
                        .expression_attribute_names("#retry_count", "retry_count")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":retry_count", to_attribute_value(lock_node_exec_details.retry_count).unwrap())
                        .expression_attribute_values(":status", to_attribute_value(Status::Running).unwrap())
                        .expression_attribute_values(":queued", to_attribute_value(Status::Queued).unwrap())
                        .expression_attribute_values(":in_progress", to_attribute_value(Status::InProgress).unwrap())
                        .build().unwrap())
                    .build(),
                     TransactWriteItem::builder()
                         .update(Update::builder()
                             .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
                             .table_name(get_workflow_executions_table_name())
                             .set_key(Some(build_workflow_execution_key(workflow_id, execution_id)))
                             .update_expression("SET #state_keys_by_node_id.#node_id = :state_id, #last_executed_node_id = :last_executed_node_id, #updated_at = :updated_at")
                             .expression_attribute_names("#state_keys_by_node_id", "state_keys_by_node_id")
                             .expression_attribute_names("#updated_at", "updated_at")
                             .expression_attribute_names("#last_executed_node_id", "last_executed_node_id")
                             .expression_attribute_names("#node_id", lock_node_exec_details.node_id.name.clone())
                             .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                             .expression_attribute_values(":state_id", to_attribute_value(lock_node_exec_details.state_id).unwrap())
                             .expression_attribute_values(":last_executed_node_id", to_attribute_value(lock_node_exec_details.node_id).unwrap())
                             .build()
                             .unwrap())
                         .build()
                ]
            }
            WriteRequest::SaveNodeExec(node_execution_state) => {
                vec![TransactWriteItem::builder()
                    .put(Put::builder()
                        .table_name(get_node_executions_table_name())
                        .set_item(Some(build_node_execution_item(NodeExecutionState {
                            updated_at: Some(now_i64()),
                            ..node_execution_state
                        })))
                        .condition_expression("#status = :status")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_values(":status", to_attribute_value(Status::Running).unwrap())
                        .build().unwrap())
                    .build()]
            }
            WriteRequest::UpdateNodeStatus(update_node_status_details) => {
                vec![TransactWriteItem::builder()
                    .update(Update::builder()
                        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &update_node_status_details.state_id)))
                        .update_expression("SET #status = :status, #updated_at = :updated_at")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":status", to_attribute_value(update_node_status_details.status).unwrap())
                        .build().unwrap())
                    .build()]
            }
            WriteRequest::IncrementBranchIndex(increment_branch_index) => {
                vec![TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &increment_branch_index.state_id)))
                        .update_expression("ADD #execution.#branch.#branch_index.#branch_name :one SET #updated_at = :updated_at")
                        .expression_attribute_names("#execution", "execution")
                        .expression_attribute_names("#branch", "Branch")
                        .expression_attribute_names("#branch_index", "branch_index")
                        .expression_attribute_names("#branch_name", "branch_name")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_names("#branch_index", "branch_index")
                        .expression_attribute_names("#branch_name", increment_branch_index.branch_name)
                        .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
                        .build().unwrap())
                    .build()]
            }
            WriteRequest::IncrementConditionIndex(increment_condition_index_details ) => {
                vec![TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &increment_condition_index_details.state_id)))
                        .update_expression("ADD #execution.#condition.#index :one SET #updated_at = :updated_at")
                        .expression_attribute_names("#execution", "execution")
                        .expression_attribute_names("#condition", "Condition")
                        .expression_attribute_names("#index", "index")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_names("#index", "index")
                        .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
                        .build().unwrap())
                    .build()]
            }
            WriteRequest::IncrementLoopIndex(increment_loop_index_details) => {
                vec![TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &increment_loop_index_details.state_id)))
                        .update_expression("SET #execution.#loop.#index = :next_index, #execution.#loop.#iteration_count = :iteration_count, #updated_at = :updated_at")
                        .expression_attribute_names("#execution", "execution")
                        .expression_attribute_names("#loop", "Loop")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_names("#index", "index")
                        .expression_attribute_names("#iteration_count", "iteration_count")
                        .expression_attribute_values(":updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":next_index", to_attribute_value(increment_loop_index_details.next_index).unwrap())
                        .expression_attribute_values(":iteration_count", to_attribute_value(increment_loop_index_details.iteration_count).unwrap())
                        .build().unwrap())
                    .build()]
            }
        }
    }
}

fn get_node_executions_table_name() -> &'static str {
    "ironflow_node_executions"
}

fn get_workflow_executions_table_name() -> &'static str {
    "ironflow_workflow_executions"
}

fn build_workflow_execution_item(workflow_execution: WorkflowExecution) -> HashMap<String,  AttributeValue> {
    let workflow_id = &workflow_execution.workflow.clone().id;
    let execution_id = &workflow_execution.execution_id.clone();
    let mut item: HashMap<String, AttributeValue> = to_item(workflow_execution).unwrap();
    let key = build_workflow_execution_key(workflow_id, execution_id);
    item.extend(key.into_iter());
    item
}

fn build_node_execution_item(node_execution_state: NodeExecutionState) -> HashMap<String,  AttributeValue> {
    let workflow_id = &node_execution_state.workflow_id;
    let execution_id = &node_execution_state.execution_id;
    let state_id = &node_execution_state.state_id;
    let mut item: HashMap<String, AttributeValue> = to_item(node_execution_state.clone()).unwrap();
    let pk = build_workflow_execution_key(workflow_id, execution_id);
    let sk = build_node_execution_sort_key(state_id);
    item.extend(pk.into_iter());
    item.extend(sk.into_iter());
    item
}

fn build_node_state_key(workflow_id: &String, execution_id: &String, state_id: &String) -> HashMap<String,  AttributeValue> {
    merge_pk_and_sk(build_workflow_execution_key(workflow_id, execution_id), build_node_execution_sort_key(&state_id))
}

fn merge_pk_and_sk(pk: HashMap<String,  AttributeValue>, sk: HashMap<String,  AttributeValue>) -> HashMap<String,  AttributeValue> {
    let mut key: HashMap<String, AttributeValue> = HashMap::new();
    key.extend(pk.into_iter());
    key.extend(sk.into_iter());
    key
}

fn build_workflow_execution_key(workflow_id: &String, execution_id: &String) -> HashMap<String, AttributeValue> {
    HashMap::from([(build_node_state_pk(),
                    to_attribute_value(format!("{}#{}", workflow_id, execution_id)).unwrap())])
}

fn build_node_execution_sort_key(state_id: &String) -> HashMap<String, AttributeValue> {
    HashMap::from([(build_node_state_sk(),
                    to_attribute_value(state_id.clone()).unwrap())])
}

fn build_node_state_pk() -> String {
    "workflow_id#execution_id".to_string()
}

fn build_node_state_sk() -> String {
    "state_id".to_string()
}

fn now_i64() -> i64 {
    DateTime::from(SystemTime::now()).to_millis().unwrap()
}