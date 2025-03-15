use bon::Builder;
use jmespath::functions::Function;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Body, Client, Error, Method, Response, Url};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub fn new() -> Arc<Self> {
        Arc::new(HttpClient {
            client: Default::default(),
        })
    }

    pub async fn execute2(&self, http_request: HttpRequest) -> Result<Response, Error> {
        let mut headers = HeaderMap::new();
        http_request.headers.iter()
            .for_each(|(key, header_value)| {
                headers.insert(
                    HeaderName::from_bytes(&key.as_bytes()).unwrap(),
                    HeaderValue::from_bytes(header_value.as_str().as_bytes()).unwrap(),
                );
            });
        let query_params: Vec<(&str, String)> = http_request.params.iter()
            .map(|(name, param)| (name.as_str(), param.clone()))
            .collect();
        let mut request_builder = self.client.request(Method::try_from(http_request.method.as_str()).unwrap(),
                                                      Url::parse(http_request.url.as_str()
                                                          .trim_matches('"'))
                                                          .unwrap())
            .headers(headers)
            .query(&query_params);
        match http_request.body {
            Some(body) => {
                request_builder = request_builder.body(Body::from(body))
            }
            _ => {}
        }
        let reqwest_request = request_builder.build()?;
        self.client.execute(reqwest_request).await
    }

}

#[derive(Builder)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub params: HashMap<String, String>,
    pub body: Option<String>,
}