use crate::config::configuration::Secret;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct SecretManager {
    secret_values : RwLock<HashMap<Secret, String>>,
}

impl SecretManager {

    pub fn new() -> Arc<Self> {
        Arc::new(SecretManager {
            secret_values: Default::default(),
        })
    }

    pub async fn reveal(&self, secret: &Secret) -> Result<String, String> {
        let exising = self.secret_values.read()
            .await
            .get(&secret)
            .cloned();
        let revealed_value = match exising {
            None => {
                match &secret {
                    Secret::Env(var_name) => {
                        Ok(env::vars()
                            .find(|(key, _)| key.eq(var_name))
                            .map(|(_, value)| value.clone())
                            .iter()
                            .next()
                            .cloned()
                            .unwrap_or_else(|| "".to_string()))
                    }
                    Secret::Plain(plain_val) => {
                        Ok(plain_val.clone())
                    }
                }
            }
            Some(val) => {
                Ok(val.clone())
            }
        };
        if revealed_value.is_ok() {
            self.secret_values.write().await.insert(secret.clone(), revealed_value.clone()?);
        }
        revealed_value
    }
}