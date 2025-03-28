use crate::config::configuration::ConfigurationManager;
use crate::execution::execution::WorkflowExecutor;
use crate::execution::model::{ExecutionSource, StartWorkflowCommand, WorkflowExecution};
use crate::persistence::persistence::Repository;
use crate::yaml::yaml::{from_yaml, from_yaml_to_auth};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use axum_macros::debug_handler;
use bon::Builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tracing::Level;

#[derive(Clone)]
pub struct ApiState {
    pub(crate) workflow_executor: Arc<WorkflowExecutor>,
    pub(crate) configuration_manager: Arc<ConfigurationManager>,
    pub(crate) repository: Arc<Repository>,
}

pub async fn build_routes(app_state: ApiState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/workflows/{workflow_id}/executions/{execution_id}", post(run_workflow_with_execution_id))
        .route("/workflows/{workflow_id}/executions", post(run_workflow))
        .route("/workflows/{workflow_id}/executions/{execution_id}", get(get_workflow_exec))
        .route("/workflows", put(put_workflow))
        .route("/auth-providers", put(put_auth_provider))
        .layer(cors)
        .layer(DefaultBodyLimit::max(90003944))
        .layer(TraceLayer::new_for_http()
            .make_span_with(
                DefaultMakeSpan::new().include_headers(true))
            .on_request(
                DefaultOnRequest::new()
                    .level(Level::INFO))
            .on_response(
                DefaultOnResponse::new()
                    .level(Level::INFO)
                    .latency_unit(LatencyUnit::Millis)
            ))
        .with_state(app_state)
}

#[debug_handler]
async fn run_workflow_with_execution_id(
    Path((workflow_id, execution_id)): Path<(String, String)>,
    State(app_state): State<ApiState>,
    Json(input): Json<Value>,
) -> Result<Json<RunWorkflowResponse>, String> {
    let result = app_state.workflow_executor.start(StartWorkflowCommand::builder()
        .workflow_id(workflow_id)
        .execution_id(execution_id)
        .input(input)
        .source(ExecutionSource::Manual)
        .dept_so_far(vec![])
        .build()).await;
    match result {
        Ok(exec_id) => {
            Ok(Json(RunWorkflowResponse::builder()
                .execution_id(exec_id.clone())
                .build()))
        }
        Err(err) => {
            Err(err.to_string())
        }
    }
}

#[debug_handler]
async fn run_workflow(
    Path((workflow_id)): Path<(String)>,
    State(app_state): State<ApiState>,
    Json(input): Json<Value>,
) -> Result<Json<RunWorkflowResponse>, String>{
    let result = app_state.workflow_executor.start(StartWorkflowCommand::builder()
        .workflow_id(workflow_id)
        .input(input)
        .source(ExecutionSource::Manual)
        .dept_so_far(vec![])
        .build()).await;
    match result {
        Ok(exec_id) => {
            Ok(Json(RunWorkflowResponse::builder()
                .execution_id(exec_id.clone())
                .build()))
        }
        Err(err) => {
            Err(err.to_string())
        }
    }
}

#[debug_handler]
async fn get_workflow_exec(
    Path((workflow_id, execution_id)): Path<(String, String)>,
    State(app_state): State<ApiState>,
    Json(input): Json<Value>,
) -> Result<Json<WorkflowExecution>, String>{
    let result = app_state.repository.get_workflow_execution(&workflow_id, &execution_id).await;
    match result {
        Ok(workflow_opt) => {
            match workflow_opt {
                None => {
                    Err(format!("workflow execution {}#{} not found", workflow_id, execution_id))
                }
                Some(workflow_exec) => {
                    Ok(Json(workflow_exec))
                }
            }

        }
        Err(err) => {
            Err("Internal server error!".to_string())
        }
    }
}

#[debug_handler]
async fn put_workflow(
    State(app_state): State<ApiState>,
    yaml: String,
) -> Result<Json<PutConfigResponse>, String> {
    let result = from_yaml(&yaml);
    match result {
        Ok(workflow) => {
            let workflow_id = &workflow.id.clone();
            app_state.configuration_manager.register_workflow(workflow).await;
            Ok(Json(PutConfigResponse {
                message: format!("workflow[{}] successfully registered", workflow_id),
            }))
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
}

#[debug_handler]
async fn put_auth_provider(
    State(app_state): State<ApiState>,
    yaml: String,
) -> Result<Json<PutConfigResponse>, String> {
    let result = from_yaml_to_auth(&yaml);
    match result {
        Ok(auth_provider_details) => {
            for auth_provider in auth_provider_details.providers {
                app_state.configuration_manager.register_auth_provider(auth_provider).await;
            }
            Ok(Json(PutConfigResponse {
                message: "Auth providers successfully registered".to_string(),
            }))
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Debug, Builder)]
pub struct RunWorkflowResponse {
    execution_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Builder)]
pub struct PutConfigResponse {
    message: String,
}