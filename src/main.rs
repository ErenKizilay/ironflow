use uuid::Uuid;
use crate::engine::Engine;

mod execution;
mod expression;
mod http;
mod model;
mod persistence;
mod yaml;
mod auth;
mod engine;
mod listener;
mod aws_lambda;
mod api;

#[tokio::main]
async fn main() {
    setup_logging();
    let mut engine = Engine::new()
        .await
        .load_auth_providers_locally("resources/auth.yaml")
        .load_workflows_locally("resources/workflows")
        .await;
    engine.start().await;
}

fn setup_logging() {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}
