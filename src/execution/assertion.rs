use crate::execution::model::{AssertionExecution, Execution, Status};
use crate::model::AssertionConfig;
use serde_json::Value;

pub async fn initiate_execution(assertion_config: &AssertionConfig, context: Value) -> (Status, Execution) {
    let errors: Vec<Option<String>> = assertion_config.assertions
        .iter()
        .map(|assertion| assertion.evaluate(&context))
        .collect();
    let all_passed = errors.iter().all(|error| error.is_none());
    let assertion_exec = Execution::Assertion(AssertionExecution { passed: all_passed, errors });
    if all_passed {
        (Status::Success, assertion_exec)
    } else {
        (Status::Failure, assertion_exec)
    }
}