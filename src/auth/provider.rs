use crate::auth::http::{AuthStrategy, HttpAuthentication};
use crate::config::configuration::ConfigurationManager;
use crate::secret::secrets::SecretManager;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use std::sync::Arc;

#[derive(Debug)]
pub struct AuthProvider {
    secret_manager: Arc<SecretManager>,
    configuration_manager: Arc<ConfigurationManager>,
}

impl AuthProvider {
    pub fn new(secret_manager: Arc<SecretManager>, configuration_manager: Arc<ConfigurationManager>) -> Self {
        Self { secret_manager, configuration_manager }
    }

    pub async fn resolve_auth_header(&self, url: &String, provider_names: &Vec<String>) -> Option<(String, String)> {
        let http_auth = self.configuration_manager.auth_providers_by_name(provider_names).await
            .iter()
            .filter(|http_authentication| url.contains(&http_authentication.auth.host))
            .next()
            .cloned();
        match http_auth {
            None => {
                None
            }
            Some(auth) => {
                self.resolve_auth_secret(&auth).await
            }
        }
    }

    async fn resolve_auth_secret(&self, http_authentication: &HttpAuthentication) -> Option<(String, String)> {
        match &http_authentication.auth.strategy {
            AuthStrategy::Plain => {None}
            AuthStrategy::Basic(basic) => {
                let password = &basic.password;
                let reveal_result = self.secret_manager.reveal(password).await;
                match reveal_result {
                    Ok(password_value) => {
                        let secret = BASE64_STANDARD.encode(format!("{}:{}", basic.name, password_value));
                        Some(("Authorization".to_owned(), format!("Basic {}", secret)))
                    }
                    Err(_) => {
                        None
                    }
                }
            }
            AuthStrategy::Token(token) => {
                let reveal_result = self.secret_manager.reveal(&token.value).await;
                match reveal_result {
                    Ok(token_value) => {
                        Some((token.name.clone(), token_value))
                    }
                    Err(_) => {None}
                }
            }
        }
    }
}