use crate::config::configuration::Secret;
use base64::Engine;
use serde::Deserialize;

#[derive(Debug,  Deserialize, Clone, PartialEq)]
pub struct HttpAuthentication {
    pub name: String,
    pub auth: HttpAuth,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct HttpAuth {
    pub host: String,
    pub strategy: AuthStrategy,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum AuthStrategy {
    Plain,
    Basic(BasicAuth),
    Token(TokenBasedAuth),
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct TokenBasedAuth {
    pub name: String,
    pub value: Secret,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct BasicAuth {
    pub name: String,
    pub password: Secret,
}