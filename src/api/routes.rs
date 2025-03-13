use std::sync::Arc;
use aws_sdk_lambda::primitives::event_stream::HeaderValue::Uuid;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::{Json, Router};
use axum::http::{Response, StatusCode};
use axum::routing::{get, post};
use axum_macros::debug_handler;
use bon::Builder;
use clap::builder::Str;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower_http::cors::{Any, CorsLayer};
use tower_http::LatencyUnit;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;
use crate::engine::Engine;
use crate::execution::model::{NodeExecutionState, WorkflowExecution};
use crate::model::Graph;
use crate::persistence::persistence::{PersistenceError, Repository};

#[derive(Clone)]
pub struct AppState {
    pub(crate) engine: Arc<Engine>,
}

pub async fn build_routes(app_state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/workflows/{workflow_id}/executions/{execution_id}", post(run_workflow_with_execution_id))
        .route("/workflows/{workflow_id}/executions/", post(run_workflow))
        .route("/workflows/{workflow_id}/executions/{execution_id}", get(get_workflow_exec))
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
    State(app_state): State<AppState>,
    Json(input): Json<Value>,
) -> Json<RunWorkflowResponse>{
    app_state.engine.run_workflow(workflow_id, execution_id.clone(), input).await;
    Json(RunWorkflowResponse::builder()
        .execution_id(execution_id.clone())
        .build())
}

#[debug_handler]
async fn run_workflow(
    Path((workflow_id)): Path<(String)>,
    State(app_state): State<AppState>,
    Json(input): Json<Value>,
) -> Json<RunWorkflowResponse>{
    let execution_id = uuid::Uuid::new_v4().to_string();
    app_state.engine.run_workflow(workflow_id, execution_id.clone(), input).await;
    Json(RunWorkflowResponse::builder()
        .execution_id(execution_id.clone())
        .build())
}

#[debug_handler]
async fn get_workflow_exec(
    Path((workflow_id, execution_id)): Path<(String, String)>,
    State(app_state): State<AppState>,
    Json(input): Json<Value>,
) -> Result<Json<WorkflowExecution>, String>{
    let result = app_state.engine.repository.port.get_workflow_execution(&workflow_id, &execution_id).await;
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


#[derive(Serialize, Deserialize, Clone, Debug, Builder)]
pub struct RunWorkflowResponse {
    execution_id: String,
}