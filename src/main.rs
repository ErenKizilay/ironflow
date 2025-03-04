use crate::engine::Engine;

mod execution;
mod expression;
mod http;
mod json;
mod model;
mod persistence;
mod yaml;
mod auth;
mod engine;

#[tokio::main]
async fn main() {
    let engine = Engine::new()
        .load_auth_providers_locally("resources/auth.yaml")
        .load_workflows_locally("resources/workflows")
        .await;
    engine.start().await;
    let input = r#"
        {
            "message": "omg message",
            "description": "desc description"
        }"#;
    engine.run_workflow("opsgenie".to_string(), "my_exec_1".to_string(), serde_json::from_str(input).unwrap())
        .await;
    engine.stop().await;
}
