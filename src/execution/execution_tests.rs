
#[cfg(test)]
mod tests {
    use crate::auth::provider::AuthProvider;
    use crate::config::configuration::{ConfigOptions, ConfigSource, ConfigurationManager, ExecutionConfig, IronFlowConfig, PersistenceConfig, PersistenceProvider};
    use crate::execution::execution::WorkflowExecutor;
    use crate::execution::model::{ExecutionSource, StartWorkflowCommand, Status};
    use crate::in_memory::adapters::in_memory_queue;
    use crate::listener::queue::QueuePort;
    use crate::persistence::persistence::Repository;
    use crate::secret::secrets::SecretManager;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::sync::watch;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_workflow_execution() {
        let configuration_manager = ConfigurationManager::new(IronFlowConfig::of_in_memory(
            ConfigOptions::builder()
                .workflow_source(ConfigSource::Local("resources/workflows".to_string()))
                .auth_provider_source(ConfigSource::Local("resources/auth.yaml".to_string()))
                .build(),
        ));

        configuration_manager.write().await.load().await;
        let (tx, rx) = watch::channel(SystemTime::now());
        let repository = Arc::new(Repository::new(PersistenceConfig {
            provider: PersistenceProvider::InMemory,
        }).await);
        let workflow_executor = WorkflowExecutor::new(
            ExecutionConfig::default(),
            configuration_manager.clone(),
            repository.clone(),
            Arc::new(AuthProvider::new(
                SecretManager::new(),
                configuration_manager,
            )),
            tx,
        ).await;
        let in_memory_queue = in_memory_queue().clone();

        let workflow_id = "opsgenie".to_string();
        let execution_id = Uuid::new_v4().to_string();
        let start_result = workflow_executor.start(StartWorkflowCommand::builder()
            .workflow_id(workflow_id.clone())
            .execution_id(execution_id.clone())
            .input(json!(HashMap::from([("message".to_string(), "a message".to_string()), ("description".to_string(), "a desc".to_string())])))
            .source(ExecutionSource::Manual)
            .dept_so_far(vec![])
            .build()).await;
        assert!(start_result.is_ok());

        let workflow_execution = repository.get_workflow_execution(&workflow_id, &execution_id).await
            .unwrap().unwrap();
        assert_eq!(Status::Queued, workflow_execution.status);

        let started_at = SystemTime::now();
        loop {
            if SystemTime::now().duration_since(started_at).unwrap().as_secs() > 10 {
                panic!("Execution timed out");
            }
            let workflow_execution = repository.get_workflow_execution(&workflow_id, &execution_id).await
                .unwrap().unwrap();
            if workflow_execution.status.is_complete() {
                println!("workflow execution complete: {:?}", workflow_execution);
                let state_ids: Vec<String> = workflow_execution.state_keys_by_node_id.iter()
                    .map(|(_, val)|val.clone())
                    .collect();
                let node_execs = repository.get_node_executions(&workflow_id, &execution_id, state_ids)
                    .await;

                for node_exec in node_execs {
                    println!("node_execs: {:?}", node_exec);
                    println!("----------");
                }
                assert_eq!(Status::Success, workflow_execution.status);
                break;
            }
            let messages = in_memory_queue.receive_messages().await;
            for message in messages {
                workflow_executor.continue_execution(&message.node_execution_state).await
                    .unwrap();
            }
        }
    }
}