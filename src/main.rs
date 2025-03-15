use crate::config::configuration::{load_iron_flow_config, ConfigOptions, ConfigSource, IronFlowConfig};
use crate::engine::IronFlow;

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
mod config;
mod secret;

#[tokio::main]
async fn main() {
    setup_logging();
    let iron_flow_config = load_iron_flow_config();
    IronFlow::run(iron_flow_config).await;
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
