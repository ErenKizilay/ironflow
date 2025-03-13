use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use serde_json::{Map, Value};
use crate::execution::model::{Execution, NodeExecutionState, WorkflowExecution};
use crate::model::{LoopConfig, NodeConfig, NodeId};
use crate::persistence::persistence::Repository;

async fn load_env_variables() -> Value {
    let mut variables = Map::new();
    env::vars()
        .for_each(|(name, value)| {
            variables.insert(name, Value::String(value));
        });
    Value::Object(variables)
}

pub async fn build_context(repository: Arc<Repository>, workflow_execution: &WorkflowExecution, config: &NodeConfig, node_state: &NodeExecutionState) -> Value {
    let workflow = &workflow_execution.workflow;
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
    tracing::debug!("will build context with following state ids: {:?}", referred_state_ids);
    let referred_node_states = repository.port
        .get_node_executions(
            &workflow.id.clone(),
            &workflow_execution.execution_id,
            referred_state_ids,
        )
        .await;

    let mut context: Map<String, Value> = referred_node_states
        .iter()
        .filter(|state| state.execution.is_some())
        .map(|state| (state.node_id.clone().name, state.get_context()))
        .collect();

    if !node_state.depth.is_empty() {
        let parent_node_id = node_state.depth.last().unwrap();
        let parent_state_id = workflow_execution.get_state_id_of_node(parent_node_id);
        let parent_node = workflow.get_node(parent_node_id).unwrap();
        if let NodeConfig::LoopNode(loop_config) = parent_node {
            let loop_state = repository.port.get_node_execution(&workflow.id, &workflow_execution.execution_id, &parent_state_id)
                .await
                .unwrap()
                .unwrap();
            if let Execution::Loop(loop_exec) = loop_state.execution.unwrap() {
                context.insert(loop_config.for_each.clone(), loop_exec.get_item());
            }
        }
    }

    context.insert("input".to_string(), workflow_execution.input.clone());
    context.insert("env".to_string(), load_env_variables().await);
    Value::Object(context)
}