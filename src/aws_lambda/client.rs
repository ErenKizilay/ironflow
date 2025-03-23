use crate::aws_lambda;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_lambda::error::ProvideErrorMetadata;
use aws_sdk_lambda::operation::invoke::{InvokeError, InvokeOutput};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{InvocationType, LogType};
use aws_sdk_lambda::Client;
use bon::Builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::string::FromUtf8Error;
use std::sync::Arc;

#[derive(Debug)]
pub struct LambdaClient {
    client: Client,
}

impl LambdaClient {
    pub async fn new() -> Arc<Self> {
        let config = aws_config::load_from_env().await;
        Arc::new(LambdaClient {
            client: Client::new(&config),
        })
    }

    pub async fn invoke(&self, request: InvokeLambdaRequest) -> Result<Value, String> {
        let invocation_result = self
            .client
            .invoke()
            .function_name(request.function_name)
            .payload(Blob::from(request.payload.to_string().as_str()
                .as_bytes()))
            .invocation_type(InvocationType::RequestResponse)
            .log_type(LogType::Tail)
            .send()
            .await;
        match invocation_result {
            Ok(invoke_output) => {
                tracing::info!(
                    "invocation log result: {:?}",
                    invoke_output.log_result.unwrap_or_default()
                );
                match invoke_output.payload {
                    None => Ok(Value::Null),
                    Some(blob) => {
                        let from_utf8 = String::from_utf8(blob.into_inner());
                        match from_utf8 {
                            Ok(payload_string) => serde_json::from_str(&payload_string)
                                .unwrap_or_else(|_| {
                                    Err(format!(
                                        "could not parse payload from UTF-8 {}",
                                        payload_string
                                    ))
                                }),
                            Err(err) => Err(format!("could not parse payload in UTF-8 {}", err)),
                        }
                    }
                }
            }
            Err(err) => Err(err.message()
                .unwrap_or(err.to_string().as_str())
                .to_string()),
        }
    }
}

#[derive(Clone, Debug, Builder)]
pub struct InvokeLambdaRequest {
    pub function_name: String,
    pub payload: Value,
}