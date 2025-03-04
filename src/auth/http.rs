use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use serde::Deserialize;
use std::iter::Map;

pub struct AuthenticationProviders {
    pub providers_by_name: Map<String, AuthenticationProvider>
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthenticationProvider {
    pub name: String,
    pub auth: HttpAuth,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpAuth {
    pub host: String,
    pub strategy: AuthStrategy,
}

#[derive(Debug, Deserialize, Clone)]
pub enum AuthStrategy {
    Plain,
    Basic(BasicAuth),
    Token(TokenBasedAuth),
}

#[derive(Debug, Deserialize, Clone)]
pub struct TokenBasedAuth {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BasicAuth {
    pub name: String,
    pub password: String,
}

impl HttpAuth {
    pub fn to_header(&self) -> Option<(String, String)> {
        match &self.strategy {
            AuthStrategy::Plain => {None}
            AuthStrategy::Basic(basic) => {
                let secret = BASE64_STANDARD.encode(format!("{}:{}", basic.name, basic.password));
                Some(("Authorization".to_owned(), format!("Basic {}", secret)))
            }
            AuthStrategy::Token(token) => {
                Some((token.name.clone(), token.value.clone()))
            }
        }
    }
}