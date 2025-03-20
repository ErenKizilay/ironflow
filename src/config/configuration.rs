use crate::auth::http::HttpAuthentication;
use crate::auth::provider::AuthProvider;
use crate::config::git_adapters;
use crate::model::Graph;
use crate::secret::secrets::SecretManager;
use crate::yaml::yaml::{from_yaml, from_yaml_file, from_yaml_file_to_auth, from_yaml_to_auth, AuthProviderDetails};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bon::Builder;
use clap::builder::Str;
use reqwest::Client;
use serde::Deserialize;
use std::ascii::AsciiExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};
use tokio::sync::RwLock;
use tokio_util::task::TaskTracker;
use tower_http::follow_redirect::policy::PolicyExt;

#[derive(Debug, Deserialize, Clone)]
pub struct IronFlowConfig {
    pub config_options: ConfigOptions,
    pub lister_config: ListenerConfig,
    pub persistence_config: PersistenceConfig,
    pub execution_config: ExecutionConfig,
    pub api_config: ApiConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    pub port: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionConfig {
    pub max_workflow_chain_depth: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PersistenceConfig {
    pub provider : PersistenceProvider,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ListenerConfig {
    pub poll_interval_ms: u64,
    pub queue_provider: QueueProvider,
    pub message_visibility_timeout_sec: usize,
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub enum QueueProvider {
    SQS,
    InMemory
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub enum PersistenceProvider {
    DynamoDb,
    InMemory
}

impl Default for ApiConfig {
    fn default() -> Self {
        ApiConfig { port: 3000 }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig {
            max_workflow_chain_depth: 10,
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        PersistenceConfig {
            provider: PersistenceProvider::DynamoDb,
        }
    }
}

impl Default for ListenerConfig {
    fn default() -> Self {
        ListenerConfig {
            poll_interval_ms: 300,
            queue_provider: QueueProvider::SQS,
            message_visibility_timeout_sec: 30,
        }
    }
}

impl IronFlowConfig {
    pub fn default(config_options: ConfigOptions) -> IronFlowConfig {
        IronFlowConfig {
            config_options,
            lister_config: Default::default(),
            persistence_config: Default::default(),
            execution_config: Default::default(),
            api_config: Default::default(),
        }
    }

    pub fn of_in_memory(config_options: ConfigOptions) -> IronFlowConfig {
        let mut default = Self::default(config_options);
        default.lister_config.queue_provider = QueueProvider::InMemory;
        default.persistence_config.provider = PersistenceProvider::InMemory;
        default
    }

}

#[derive(Debug)]
pub struct ConfigurationManager {
    iron_flow_config: IronFlowConfig,
    auth_providers: RwLock<HashMap<String, HttpAuthentication>>,
    workflows_by_id: RwLock<HashMap<String, Graph>>,
    secret_manager: Arc<SecretManager>,
    task_tracker: TaskTracker,
    http_client: Arc<Client>,
}

impl ConfigurationManager {
    pub async fn new(iron_flow_config: IronFlowConfig, secret_manager: Arc<SecretManager>) -> Arc<ConfigurationManager> {
        let arc = Arc::new(ConfigurationManager {
            iron_flow_config,
            auth_providers: RwLock::new(HashMap::new()),
            workflows_by_id: Default::default(),
            secret_manager,
            task_tracker: TaskTracker::new(),
            http_client: Arc::new(Client::builder()
                .build().unwrap()),
        });
        arc.load().await;
        let cloned = arc.clone();
        arc.task_tracker.spawn(async move {
            loop {
                tracing::info!("Will load configurations");
                cloned.load().await;
                tokio::time::sleep(Duration::from_secs(cloned.iron_flow_config.config_options.refresh_interval_secs)).await;
            }
        });
        arc
    }

    pub async fn load(&self) {
        self.load_auth_providers().await;
        self.load_workflows().await;
    }

    pub async fn get_workflow(&self, workflow_id: &String) -> Option<Graph> {
        self.workflows_by_id.read().await.get(workflow_id).cloned()
    }

    pub async fn register_workflow(&self, workflow: Graph) {
        self.workflows_by_id.write().await.insert(workflow.id.clone(), workflow);
    }

    pub async fn register_auth_provider(&self, auth_provider: HttpAuthentication) {
        self.auth_providers.write().await.insert(auth_provider.name.clone(), auth_provider);
    }

    pub async fn auth_providers_by_name(&self, provider_names: &Vec<String>) -> Vec<HttpAuthentication> {
        let mut result: Vec<HttpAuthentication> = Vec::new();
        for provider_name in provider_names {
            if let Some(auth) = self.auth_providers.read().await.get(provider_name) {
                result.push(auth.clone())
            }
        }
        result
    }

    async fn load_auth_providers(&self) {
        tracing::info!("Loading auth providers");
        if let Some(config_source) = &self.iron_flow_config.config_options.auth_provider_source {
            match config_source {
                ConfigSource::Local(path) => {
                    let mut provider_details = from_yaml_file_to_auth(path.as_str()).unwrap();
                    for provider in provider_details.providers {
                        self.auth_providers.write().await.insert(provider.name.clone(), provider);
                    }
                }
                ConfigSource::GitHub(source_details) => {
                    let token = self.secret_manager.reveal(&source_details.token).await.expect(format!("Failed to reveal {:?}", source_details.token).as_str());
                    let contents = git_adapters::load_contents_from_github(self.http_client.clone(), &token, &source_details).await
                        .expect("Error loading workflows from GitHub");
                    for content in contents {
                        let decoded = String::from_utf8(BASE64_STANDARD.decode(content.as_bytes())
                            .expect(format!("Error loading workflow {:?}", content).as_str())).unwrap();
                        let auth = from_yaml_to_auth(&decoded).expect("Error loading auth config from yaml");
                        for provider  in auth.providers {
                            self.auth_providers.write().await.insert(provider.name.clone(), provider);
                        }
                    }
                }
            }
        }
    }

    async fn load_workflows(&self) {
        tracing::info!("Loading workflows");
        if let Some(config_source) = &self.iron_flow_config.config_options.workflow_source {
            match config_source {
                ConfigSource::Local(path) => {
                    if let Ok(entries) = fs::read_dir(path.as_str()) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.is_file() {
                                if let Some(path_str) = path.to_str() {
                                    let workflow_result = from_yaml_file(path_str);
                                    match workflow_result {
                                        Ok(workflow) => {
                                            self.workflows_by_id.write().await.insert(workflow.id.clone(), workflow);
                                        }
                                        Err(err) => {
                                            panic!("Error loading workflow: {}", err);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                ConfigSource::GitHub(source_details) => {
                    let token = self.secret_manager.reveal(&source_details.token).await.expect(format!("Failed to reveal {:?}", source_details.token).as_str());
                    let contents = git_adapters::load_contents_from_github(self.http_client.clone(), &token, &source_details).await
                        .expect("Error loading workflows from GitHub");
                    for content in contents {
                        let decoded = String::from_utf8(BASE64_STANDARD.decode(content.as_bytes())
                            .expect(format!("Error loading workflow {:?}", content).as_str())).unwrap();
                        let workflow = from_yaml(&decoded).expect("Error loading workflow from yaml");
                        self.workflows_by_id.write().await.insert(workflow.id.clone(), workflow);
                    }
                }
            }
        }
        tracing::info!("Loaded #{} workflows", self.workflows_by_id.read().await.len());
    }
}

#[derive(Clone, Deserialize, Debug)]
pub enum ConfigSource {
    Local(String),
    GitHub(GitSourceDetails),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Eq, Hash)]
pub enum Secret {
    Env(String),
    Plain(String),
}

#[derive(Builder, Clone, Deserialize, Debug)]
pub struct GitSourceDetails {
    pub url: String,
    pub directory: String,
    pub token: Secret,
}

#[derive(Builder, Clone, Deserialize, Debug)]
pub struct ConfigOptions {
    pub auth_provider_source: Option<ConfigSource>,
    pub workflow_source: Option<ConfigSource>,
    pub refresh_interval_secs: u64,
}

pub fn load_iron_flow_config() -> IronFlowConfig {
    let in_memory_enabled = env::vars()
        .filter(|(key, value)| key.eq_ignore_ascii_case("IN_MEMORY_MODE") && value.eq_ignore_ascii_case("true"))
        .next()
        .map_or_else(|| false, |item| true);
    let default_config_options = ConfigOptions::builder()
        .auth_provider_source(ConfigSource::Local("./resources/auth.yaml".to_string()))
        .workflow_source(ConfigSource::Local("./resources/workflows".to_string()))
        .refresh_interval_secs(300)
        .build();
    if in_memory_enabled {
        tracing::info!("Will use in memory providers..");
        return IronFlowConfig::of_in_memory(default_config_options)
    }
    let mut file_result = File::open("/opt/ironflow/config.yaml");
    let iron_flow_config = match file_result {
        Ok(mut file) => {
            tracing::info!("Will load configuration from config.yaml");
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            serde_yaml::from_str::<IronFlowConfig>(&contents).unwrap()
        }
        Err(err) => {
            tracing::info!("Will use default configuration..");
            IronFlowConfig::default(default_config_options)
        }
    };
    tracing::info!("Loaded configuration: {:?}", iron_flow_config);
    if QueueProvider::InMemory.eq(&iron_flow_config.lister_config.queue_provider) && PersistenceProvider::InMemory.ne(&iron_flow_config.persistence_config.provider) {
        panic!("InMemory Queue can only be used via InMemory Persistence provider");
    }
    iron_flow_config
}
