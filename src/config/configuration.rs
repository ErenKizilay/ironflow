use crate::auth::http::HttpAuthentication;
use crate::model::Graph;
use crate::yaml::yaml::{from_yaml, from_yaml_to_auth};
use bon::Builder;
use clap::builder::Str;
use serde::Deserialize;
use std::ascii::AsciiExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};
use std::fs::File;
use std::io::Read;
use tokio::sync::RwLock;
use tokio_util::task::TaskTracker;

#[derive(Debug, Deserialize, Clone)]
pub struct IronFlowConfig {
    pub config_options: ConfigOptions,
    pub lister_config: ListenerConfig,
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
pub struct ListenerConfig {
    pub poll_interval: Duration,
    pub message_visibility_timeout: Duration,
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

impl Default for ListenerConfig {
    fn default() -> Self {
        ListenerConfig {
            poll_interval: Duration::from_millis(300),
            message_visibility_timeout: Duration::from_secs(30),
        }
    }
}

impl IronFlowConfig {
    pub fn default(config_options: ConfigOptions) -> IronFlowConfig {
        IronFlowConfig {
            config_options,
            lister_config: Default::default(),
            execution_config: Default::default(),
            api_config: Default::default(),
        }
    }
}

pub struct ConfigurationManager {
    iron_flow_config: IronFlowConfig,
    auth_providers: Vec<HttpAuthentication>,
    workflows_by_id: HashMap<String, Graph>,
    task_tracker: TaskTracker,
}

impl ConfigurationManager {
    pub fn new(iron_flow_config: IronFlowConfig) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(ConfigurationManager {
            iron_flow_config,
            auth_providers: vec![],
            workflows_by_id: Default::default(),
            task_tracker: TaskTracker::new(),
        }))
    }

    pub async fn load(&mut self) {
        self.load_auth_providers();
        self.load_workflows();
    }

    pub fn get_workflow(&self, workflow_id: &String) -> Option<Graph> {
        self.workflows_by_id.get(workflow_id).cloned()
    }

    pub fn auth_providers_by_name(&self, provider_names: &Vec<String>) -> Vec<HttpAuthentication> {
        self.auth_providers
            .iter()
            .filter(|authentication_provider| {
                provider_names.contains(&authentication_provider.name)
            })
            .cloned()
            .collect::<Vec<HttpAuthentication>>()
    }

    fn load_auth_providers(&mut self) {
        tracing::info!("Loading auth providers");
        match &self.iron_flow_config.config_options.auth_provider_source {
            ConfigSource::Local(path) => {
                let mut provider_details = from_yaml_to_auth(path.as_str());
                self.auth_providers.extend(provider_details.providers);
            }
            ConfigSource::Git(git_details) => {
                todo!("implement load from git")
            }
        }
    }

    fn load_workflows(&mut self) {
        tracing::info!("Loading workflows");
        match &self.iron_flow_config.config_options.workflow_source {
            ConfigSource::Local(path) => {
                if let Ok(entries) = fs::read_dir(path.as_str()) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_file() {
                            if let Some(path_str) = path.to_str() {
                                let workflow_result = from_yaml(path_str);
                                match workflow_result {
                                    Ok(workflow) => {
                                        self.workflows_by_id.insert(workflow.id.clone(), workflow);
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
            ConfigSource::Git(_) => {
                todo!("implement load from git")
            }
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub enum ConfigSource {
    Local(String),
    Git(GitSourceDetails),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Eq, Hash)]
pub enum Secret {
    Env(String),
    Plain(String),
}

#[derive(Builder, Clone, Deserialize, Debug)]
pub struct GitSourceDetails {
    host: String,
    directory: String,
    token: Secret,
}

#[derive(Builder, Clone, Deserialize, Debug)]
pub struct ConfigOptions {
    pub auth_provider_source: ConfigSource,
    pub workflow_source: ConfigSource,
}

pub fn load_iron_flow_config() -> IronFlowConfig {
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
            let config_options = ConfigOptions::builder()
                .auth_provider_source(ConfigSource::Local("resources/auth.yaml".to_string()))
                .workflow_source(ConfigSource::Local("resources/workflows".to_string()))
                .build();
            IronFlowConfig::default(config_options)
        }
    };
    tracing::info!("Loaded configuration: {:?}", iron_flow_config);
    iron_flow_config
}
