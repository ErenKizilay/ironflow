use crate::auth::provider::AuthProvider;
use crate::aws_lambda::client::{InvokeLambdaRequest, LambdaClient};
use crate::config::configuration::ConfigurationManager;
use crate::execution::model::{Execution, Status, StepExecution, StepExecutionError, WorkflowExecution};
use crate::expression::expression::value_as_string;
use crate::http::http::{HttpClient, HttpRequest};
use crate::model::{HttpConfig, LambdaConfig, StepTarget};
use serde_dynamo::AttributeValue::S;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct StepExecutor {
    http_client: Arc<HttpClient>,
    lambda_client: Arc<LambdaClient>,
    auth_provider: Arc<AuthProvider>,
}

impl StepExecutor {
    pub async fn new(auth_provider: Arc<AuthProvider>) -> Arc<StepExecutor> {
        Arc::new(Self {
            http_client: HttpClient::new(),
            lambda_client: LambdaClient::new().await,
            auth_provider,
        })
    }
    async fn execute_lambda_step(&self, retry_count: usize, context: &Value, lambda_config: &LambdaConfig, lambda_client: Arc<LambdaClient>) -> (Status, Execution) {
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
                    result: Err(StepExecutionError::RunFailed(Value::from(err))),
                }))
            }
        }
    }

    async fn execute_http_step(
        &self,
        http_client: Arc<HttpClient>,
        context: &Value,
        http_config: &HttpConfig,
        retry_count: usize,
        workflow_execution: &WorkflowExecution,
    ) -> (Status, Execution) {
        let http_request = self.build_http_request(workflow_execution, http_config, context)
            .await;
        let http_result = http_client.execute2(http_request).await;
        let attempt_number = retry_count + 1;
        match http_result {
            Ok(response) => {
                let status_code = &response.status();
                let response_body = &response.text().await.map_or_else(
                    |_| Value::Null,
                    |text| serde_json::from_str(text.as_str())
                        .map_or_else(|err| Value::String(text.as_str().to_string()), |val| val),
                );
                let mut response_context: Map<String, Value> = Map::new();
                response_context.insert("body".to_string(), response_body.clone());
                //todo add headers
                response_context.insert("status_code".to_string(), Value::from(status_code.as_u16()));
                if status_code.is_success() || !http_config.execution.fail_on_non_2xx {
                    (
                        Status::Success,
                        Execution::Step(StepExecution {
                            retry_count: attempt_number,
                            result: Ok(Value::Object(response_context)),
                        }),
                    )
                } else {
                    let retry_config = &http_config.execution.retry;
                    if retry_config.enabled &&
                        retry_count < retry_config.max_count + 1 &&
                        retry_config.on_status_codes.contains(&status_code.as_u16()) &&
                        retry_config.on_methods.contains(&http_config.method)  {
                        (
                            Status::WillRetried,
                            Execution::Step(StepExecution {
                                retry_count: attempt_number,
                                result: Err(StepExecutionError::RunFailed(
                                    Value::Object(response_context),
                                )),
                            }),
                        )
                    } else {
                        (
                            Status::Failure,
                            Execution::Step(StepExecution {
                                retry_count: attempt_number,
                                result: Err(StepExecutionError::RunFailed(
                                    Value::Object(response_context),
                                )),
                            }),
                        )
                    }
                }
            }
            Err(http_error) => {
                tracing::warn!("HTTP Execution error: {:?}", http_error);
                (
                    Status::Failure,
                    Execution::Step(StepExecution {
                        retry_count: attempt_number,
                        result: Err(StepExecutionError::RunFailed(Value::from(http_error.to_string()))),
                    }),
                )
            },
        }
    }

    async fn build_http_request(&self, workflow_execution: &WorkflowExecution, config: &HttpConfig, context: &Value) -> HttpRequest {
        let mut headers = HashMap::new();
        let mut params = HashMap::new();
        let url = value_as_string(config.url.resolve(context.clone()));
        self.auth_provider.resolve_auth_header(&url, &workflow_execution.workflow.config.auth_providers)
            .await
            .inspect(|(key, value)| {
                headers.insert(key.to_string(), value.to_string());
            });

        headers.insert("Content-Type".to_string(), config.content_type.clone());
        config.headers.iter()
            .for_each(|(key, dynamic_value)| {
                headers.insert(
                    key.clone(),
                    value_as_string(dynamic_value.resolve(context.clone())),
                );
            });
        config.params.iter()
            .for_each(|(key, dynamic_value)| {
                params.insert(
                    key.clone(),
                    value_as_string(dynamic_value.resolve(context.clone())),
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
}


pub async fn initiate_execution(step_executor: Arc<StepExecutor>, retry_count: usize, step_target: &StepTarget, workflow_execution: &WorkflowExecution, context: Value) -> (Status, Execution) {
    match step_target {
        StepTarget::Lambda(lambda_config) => {
            let lambda_client = step_executor.lambda_client.clone();
            step_executor.execute_lambda_step(retry_count, &context, lambda_config, lambda_client).await
        }
        StepTarget::Http(http_config) => {
            let http_client = step_executor.http_client.clone();
            step_executor.execute_http_step(http_client, &context, http_config, retry_count, &workflow_execution)
                .await
        }
    }
}