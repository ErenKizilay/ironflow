use std::collections::HashMap;
use std::sync::Arc;
use serde_dynamo::AttributeValue::S;
use serde_json::Value;
use crate::aws_lambda::client::{InvokeLambdaRequest, LambdaClient};
use crate::execution::model::{Execution, Status, StepExecution, StepExecutionError, WorkflowExecution};
use crate::http::http::{HttpClient, HttpRequest};
use crate::model::{HttpConfig, LambdaConfig, StepTarget};


pub struct StepExecutor {
    http_client: Arc<HttpClient>,
    lambda_client: Arc<LambdaClient>,
}

impl StepExecutor {
    pub async fn new() -> Arc<StepExecutor> {
        Arc::new(Self {
            http_client: HttpClient::new(),
            lambda_client: LambdaClient::new().await,
        })
    }
}


pub async fn initiate_execution(step_executor: Arc<StepExecutor>, retry_count: usize, step_target: &StepTarget, workflow_execution: &WorkflowExecution, context: Value) -> (Status, Execution) {
    match step_target {
        StepTarget::Lambda(lambda_config) => {
            let lambda_client = step_executor.lambda_client.clone();
            execute_lambda_step(retry_count, &context, lambda_config, lambda_client).await
        }
        StepTarget::Http(http_config) => {
            let http_client = step_executor.http_client.clone();
            execute_http_step(http_client, &context, http_config, retry_count, &workflow_execution)
                .await
        }
    }
}

async fn execute_lambda_step(retry_count: usize, context: &Value, lambda_config: &LambdaConfig, lambda_client: Arc<LambdaClient>) -> (Status, Execution) {
    let lambda_result = lambda_client.invoke(InvokeLambdaRequest::builder()
        .function_name(lambda_config.function_name.clone())
        .payload(lambda_config.payload.resolve(context.clone()))
        .build())
        .await;

    match lambda_result {
        Ok(response) => {
            (Status::Success, Execution::Step(StepExecution {
                retry_count: retry_count + 1,
                result: Ok(response),
            }))
        }
        Err(err) => {
            (Status::Failure, Execution::Step(StepExecution {
                retry_count: retry_count + 1,
                result: Err(StepExecutionError::RunFailed(err)),
            }))
        }
    }
}

async fn execute_http_step(
    http_client: Arc<HttpClient>,
    context: &Value,
    http_config: &HttpConfig,
    retry_count: usize,
    workflow_execution: &WorkflowExecution,
) -> (Status, Execution) {
    let http_request = build_http_request(workflow_execution, http_config, context);
    let http_result = http_client.execute2(http_request).await;
    let attempt_number = retry_count + 1;
    match http_result {
        Ok(response) => {
            if response.status().is_success() {
                let response_value = response.text().await.map_or_else(
                    |_| Value::Null,
                    |text| serde_json::from_str(text.as_str())
                        .map_or_else(|_| Value::Null, |val| val),
                );
                tracing::debug!("HTTP Execution successful: {:?}", response_value);
                (
                    Status::Success,
                    Execution::Step(StepExecution {
                        retry_count: attempt_number,
                        result: Ok(response_value),
                    }),
                )
            } else {
                tracing::warn!("HTTP error response: {:?}", response);
                (
                    Status::Failure,
                    Execution::Step(StepExecution {
                        retry_count: attempt_number,
                        result: Err(StepExecutionError::RunFailed(
                            response.text().await.unwrap(),
                        )),
                    }),
                )
            }
        }
        Err(http_error) => {
            tracing::warn!("HTTP Execution error: {:?}", http_error);
            (
                Status::Failure,
                Execution::Step(StepExecution {
                    retry_count: attempt_number,
                    result: Err(StepExecutionError::RunFailed(http_error.to_string())),
                }),
            )
        },
    }
}

fn build_http_request(workflow_execution: &WorkflowExecution, config: &HttpConfig, context: &Value) -> HttpRequest {
    let mut headers = HashMap::new();
    let mut params = HashMap::new();
    let url = config.url.resolve(context.clone())
        .to_string()
        .as_str()
        .trim_matches('"')
        .to_string();
    workflow_execution.authentication_providers
        .iter()
        .filter(|authentication_provider|url.contains(&authentication_provider.auth.host))
        .for_each(|authentication_provider| {
            authentication_provider.auth.to_header()
                .iter()
                .next()
                .inspect(|(key, value)| {
                    headers.insert(key.to_string(), value.to_string());
                });
        });
    headers.insert("Content-Type".to_string(), config.content_type.clone());
    config.headers.iter()
        .for_each(|(key, dynamic_value)| {
            headers.insert(
                key.clone(),
                dynamic_value.resolve(context.clone()).to_string(),
            );
        });
    config.params.iter()
        .for_each(|(key, dynamic_value)| {
            params.insert(
                key.clone(),
                dynamic_value.resolve(context.clone()).to_string(),
            );
        });
    HttpRequest::builder()
        .url(url)
        .method(config.method.clone())
        .headers(headers)
        .params(params)
        .maybe_body(match &config.body {
            Some(body) => {
                Some(body.resolve(context.clone()).to_string())
            }
            _ => { None }
        })
        .build()
}