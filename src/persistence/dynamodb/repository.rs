use crate::execution::model::{NodeExecutionState, Status, WorkflowExecution};
use crate::model::NodeId;
use crate::persistence::model::{WorkflowExecutionIdentifier, WriteRequest, WriteWorkflowExecutionRequest};
use aws_sdk_dynamodb::config::BehaviorVersion;
use aws_sdk_dynamodb::primitives::DateTime;
use aws_sdk_dynamodb::types::{AttributeValue, Get, Put, TransactGetItem, TransactWriteItem, Update};
use aws_sdk_dynamodb::Client;
use serde_dynamo::{to_attribute_value, to_item};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::get_item::{GetItemError, GetItemOutput};
use aws_sdk_dynamodb::operation::transact_get_items::{TransactGetItemsError, TransactGetItemsOutput};
use aws_sdk_dynamodb::operation::transact_write_items::{TransactWriteItemsError, TransactWriteItemsOutput};
use clap::builder::{Str, TypedValueParser};
use reqwest::get;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_dynamo::aws_sdk_dynamodb_1::from_item;
use tracing::error;

pub struct DynamoDbRepository {
    client: Arc<Client>,
}


impl DynamoDbRepository {
    pub async fn new() -> Self {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        DynamoDbRepository {
            client: Arc::new(client),
        }
    }

    pub async fn transact_write_items(&self, request: WriteWorkflowExecutionRequest) -> Result<TransactWriteItemsOutput, SdkError<TransactWriteItemsError, HttpResponse>> {
        let write_items: Vec<TransactWriteItem> = request.write_requests
            .iter()
            .map(|write_request: &WriteRequest| write_request.clone().to_transact_write_item(&request.identifier))
            .filter(|write_item| write_item.is_some())
            .map(|write_item| write_item.unwrap())
            .collect();
        self.client.transact_write_items()
            .set_transact_items(Some(write_items))
            .send().await
    }

    pub async fn get_workflow_execution(&self, workflow_id: &String, execution_id: &String) -> Option<WorkflowExecution> {
        self.get_item(get_workflow_executions_table_name(), build_workflow_execution_key(workflow_id, execution_id)).await
    }

    pub async fn get_node_execution(&self, workflow_id: &String, execution_id: &String, state_id: &String) -> Option<NodeExecutionState> {
        self.get_item(get_node_executions_table_name(), build_node_state_key(workflow_id, execution_id, state_id)).await
    }

    pub async fn get_node_executions(&self, workflow_id: &String, execution_id: &String, state_ids: Vec<String>) -> Vec<NodeExecutionState> {
        let gets = state_ids.iter()
            .map(|state_id| TransactGetItem::builder()
                .get(Get::builder()
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

    fn to_transact_write_item(self, execution_identifier: &WorkflowExecutionIdentifier) -> Option<TransactWriteItem> {
        let workflow_id = &execution_identifier.workflow_id;
        let execution_id = &execution_identifier.execution_id;
        match self {
            WriteRequest::InitiateWorkflowExecution(initiate_workflow_exec_details) => {
                let workflow_execution = WorkflowExecution {
                    execution_id: execution_identifier.execution_id.clone(),
                    input: initiate_workflow_exec_details.input,
                    index: 0,
                    status: Status::Queued,
                    result: None,
                    state_keys_by_node_id: Default::default(),
                    workflow: initiate_workflow_exec_details.workflow,
                    authentication_providers: initiate_workflow_exec_details.authentication_providers,
                    started_at: DateTime::from(SystemTime::now()).to_millis().unwrap(),
                };
                Some(TransactWriteItem::builder()
                    .put(Put::builder()
                        .table_name(get_workflow_executions_table_name())
                        .set_item(Some(build_workflow_execution_item(workflow_execution)))
                        .condition_expression("attribute_not_exists(workflow_id#execution_id)")
                        .build().unwrap())
                    .build())
            }
            WriteRequest::IncrementWorkflowIndex(increment_workflow_index_details) => {
                if increment_workflow_index_details.increment {
                    Some(TransactWriteItem::builder()
                        .update(Update::builder()
                            .table_name(get_workflow_executions_table_name())
                            .set_key(Some(build_workflow_execution_key(workflow_id, execution_id)))
                            .update_expression("add #index 1")
                            .condition_expression(format!("#index = {}", increment_workflow_index_details.current_index))
                            .expression_attribute_names("#index", "index")
                            .build().unwrap())
                        .build())
                } else {
                    None
                }
            }
            WriteRequest::UpdateWorkflowStatus(status) => {
                Some(TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_workflow_executions_table_name())
                        .set_key(Some(build_workflow_execution_key(workflow_id, execution_id)))
                        .update_expression("SET #status = :status")
                        .condition_expression("#status != :status")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_values(":status", to_attribute_value(status).unwrap())
                        .build().unwrap())
                    .build())
            }
            WriteRequest::InitiateNodeExec(initiate_node_exec_details) => {
                Some(TransactWriteItem::builder()
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
                        .build().unwrap())
                    .build())
            }
            WriteRequest::LockNodeExec(lock_node_exec_details) => {
                Some(TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &lock_node_exec_details.state_id)))
                        .update_expression("SET #status = :status, #updated_at = :updated_at")
                        .condition_expression("(#status in (:queued, :in_progress)) AND (attribute_not_exists(#retry_count) OR #retry_count = :retry_count)")
                        .expression_attribute_names("#retry_count", "execution.retry_count")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_values("updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":retry_count", to_attribute_value(lock_node_exec_details.retry_count).unwrap())
                        .expression_attribute_values(":status", to_attribute_value(Status::Running).unwrap())
                        .build().unwrap())
                    .build())
            }
            WriteRequest::SaveNodeExec(node_execution_state) => {
                Some(TransactWriteItem::builder()
                    .put(Put::builder()
                        .table_name(get_node_executions_table_name())
                        .set_item(Some(build_node_execution_item(node_execution_state)))
                        .build().unwrap())
                    .build())
            }
            WriteRequest::UpdateNodeStatus(update_node_status_details) => {
                Some(TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &update_node_status_details.state_id)))
                        .update_expression("SET #status = :status, #updated_at = :updated_at")
                        .condition_expression("(#status != :status)")
                        .expression_attribute_names("#status", "status")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_values("updated_at", to_attribute_value(now_i64()).unwrap())
                        .expression_attribute_values(":status", to_attribute_value(update_node_status_details.status).unwrap())
                        .build().unwrap())
                    .build())
            }
            WriteRequest::IncrementBranchIndex(increment_branch_index) => {
                Some(TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &increment_branch_index.state_id)))
                        .update_expression("add #index 1, #updated_at = :updated_at")
                        .expression_attribute_names("#index", "execution.#branch_index.#branch_name")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_names("#branch_index", "branch_index")
                        .expression_attribute_names("#branch_name", increment_branch_index.branch_name)
                        .expression_attribute_values("updated_at", to_attribute_value(now_i64()).unwrap())
                        .build().unwrap())
                    .build())
            }
            WriteRequest::IncrementConditionIndex(increment_condition_index_details ) => {
                Some(TransactWriteItem::builder()
                    .update(Update::builder()
                        .table_name(get_node_executions_table_name())
                        .set_key(Some(build_node_state_key(workflow_id, execution_id, &increment_condition_index_details.state_id)))
                        .update_expression("add #index 1, #updated_at = :updated_at")
                        .expression_attribute_names("#index", "execution.#index.#branch_name")
                        .expression_attribute_names("#updated_at", "updated_at")
                        .expression_attribute_names("#index", "index")
                        .expression_attribute_values("updated_at", to_attribute_value(now_i64()).unwrap())
                        .build().unwrap())
                    .build())
            }
            WriteRequest::PutNodeStateId(_, _) => {todo!()}
        }
    }
}

fn get_node_executions_table_name() -> &'static str {
    "node_executions"
}

fn get_workflow_executions_table_name() -> &'static str {
    "workflow_executions"
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
    let node_id = &node_execution_state.node_id;
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
    HashMap::from([(build_node_state_pk(),
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