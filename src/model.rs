use crate::expression::expression::{DynamicValue, Expression};
use crate::model::NodeConfig::StepNode;
use bon::Builder;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LambdaConfig {
    pub function_name: String,
    pub payload: DynamicValue
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConditionConfig {
    pub expression: Expression,
    pub true_branch: Vec<NodeId>,
    pub false_branch: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LoopConfig {
    pub array: DynamicValue,
    pub for_each: String,
    pub nodes: Vec<NodeId>,
}

#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId {
    pub name: String,
}

impl NodeId {
    pub(crate) fn of(name: String) -> NodeId {
        NodeId { name }
    }
}

#[derive(Clone, Debug, Builder, PartialEq, Serialize, Deserialize)]
pub struct HttpConfig {
    pub url: DynamicValue,
    pub headers: HashMap<String, DynamicValue>,
    pub method: String,
    pub body: Option<DynamicValue>,
    pub params: HashMap<String, DynamicValue>,
    pub content_type: String,
    pub execution: HttpExecutionConfig,
}

#[derive(Clone, Debug, Builder, PartialEq, Serialize, Deserialize)]
pub struct HttpRetryConfig {
    pub enabled: bool,
    pub max_count: usize,
    pub on_methods: Vec<String>,
    pub on_status_codes: Vec<u16>,
}

#[derive(Clone, Debug, Builder, PartialEq, Serialize, Deserialize)]
pub struct HttpExecutionConfig {
    pub retry: HttpRetryConfig,
    pub fail_on_non_2xx: bool,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        HttpRetryConfig {
            enabled: true,
            max_count: 5,
            on_methods: vec!["GET".to_string(), "HEAD".to_string(), "PUT".to_string(), "DELETE".to_string(), "OPTIONS".to_string(), "TRACE".to_string()],
            on_status_codes: vec![], //429 and 408 status code will be retried automatically
        }
    }
}

impl Default for HttpExecutionConfig {
    fn default() -> Self {
        HttpExecutionConfig {
            retry: Default::default(),
            fail_on_non_2xx: true,
        }
    }
}

#[derive(Clone, Debug, Builder, PartialEq, Serialize, Deserialize)]
pub struct BranchConfig {
    pub branches: Vec<Branch>,
}

#[derive(Clone, Debug, Builder, PartialEq, Serialize, Deserialize)]
pub struct AssertionConfig {
    pub assertions: Vec<AssertionItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Branch {
    pub name: String,
    pub condition: Option<Expression>,
    pub nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EqualToComparison {
    pub left: DynamicValue,
    pub right: DynamicValue,
    pub negate: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AssertionItem {
    Equal(EqualToComparison),
    NotEqual(EqualToComparison),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StepTarget {
    Lambda(LambdaConfig),
    Http(HttpConfig),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeConfig {
    StepNode(StepTarget),
    ConditionNode(ConditionConfig),
    LoopNode(LoopConfig),
    BranchNode(BranchConfig),
    AssertionNode(AssertionConfig),
    WorkflowNode(WorkflowNodeConfig)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub config: NodeConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Graph {
    pub nodes_by_id: HashMap<NodeId, NodeConfig>,
    pub node_ids: Vec<NodeId>,
    pub id: String,
    pub config: WorkflowConfiguration
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkflowNodeConfig {
    pub workflow_id: DynamicValue,
    pub execution_id: Option<DynamicValue>,
    pub input: DynamicValue
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct  WorkflowConfiguration {
    pub auth_providers: Vec<String>,
    pub max_retry_count: Option<usize>,
}

impl Default for WorkflowConfiguration {
    fn default() -> Self {
        WorkflowConfiguration {
            auth_providers: vec![],
            max_retry_count: Some(5),
        }
    }
}

impl WorkflowConfiguration {

    pub(crate) fn merge(&self) -> WorkflowConfiguration {
        let default = WorkflowConfiguration::default();
        WorkflowConfiguration {
            auth_providers: self.auth_providers.clone(),
            max_retry_count: self.max_retry_count.or(default.max_retry_count),
        }
    }
}

impl Node {
    pub(crate) fn lambda(id: NodeId, config: LambdaConfig) -> Node {
        Node {
            id,
            config: StepNode(StepTarget::Lambda(config)),
        }
    }

    pub fn http(id: NodeId, config: HttpConfig) -> Node {
        Node {
            id,
            config: StepNode(StepTarget::Http(config)),
        }
    }

    pub fn of(id: NodeId, config: NodeConfig) -> Node {
        Node { id, config }
    }
}

impl Graph {

    pub fn get_node(&self, id: &NodeId) -> Option<NodeConfig> {
        self.nodes_by_id.get(id).cloned()
    }

    pub fn get_branch_node(&self, id: &NodeId) -> Option<BranchConfig> {
        match self.nodes_by_id.get(id) {
            None => {None}
            Some(config) => {
                match config {
                    NodeConfig::BranchNode(branch_config) => {
                        Some(branch_config.clone())
                    }
                    _ => {
                        None
                    }
                }
            }
        }
    }
}
impl NodeConfig {
    pub fn get_expressions(&self) -> Vec<Expression> {
        match self {
            StepNode(step_target) => match step_target {
                StepTarget::Lambda(lambda_config) => {
                    vec![]
                }
                StepTarget::Http(http_config) => {
                    http_config.get_expressions()
                }
            },
            NodeConfig::ConditionNode(condition_config) => {
                vec![condition_config.expression.clone()]
            }
            NodeConfig::BranchNode(branch_config) => {
                let expressions: Vec<Expression> = branch_config
                    .branches
                    .iter()
                    .filter(|branch| branch.condition.is_some())
                    .map(|branch| branch.condition.clone().unwrap())
                    .collect();
                expressions
            }
            NodeConfig::AssertionNode(assertion_config) => {
                assertion_config.assertions
                    .iter()
                    .flat_map(|assertion|assertion.get_expressions())
                    .collect()
            }
            NodeConfig::LoopNode(loop_config) => {
                loop_config.array.get_expressions()
            }
            NodeConfig::WorkflowNode(workflow_node_config) => {
                let mut workflow_expressions = vec![];
                workflow_expressions.extend(workflow_node_config.input.get_expressions());
                workflow_expressions.extend(workflow_node_config.workflow_id.get_expressions());
                let execution_id = workflow_node_config.execution_id.clone();
                workflow_expressions.extend(execution_id.map_or_else(Vec::new, |dv|dv.get_expressions()));
                workflow_expressions
            }
        }
    }
}

impl HttpConfig {
    pub fn get_expressions(&self) -> Vec<Expression> {
        let mut result: Vec<Expression> = vec![];
        result.extend(self.url.get_expressions());
        self.headers.iter().for_each(|header| {
            result.extend(header.1.get_expressions());
        });
        self.params.iter().for_each(|param| {
            result.extend(param.1.get_expressions());
        });
        if let Some(body) = &self.body {
            result.extend(body.get_expressions());
        }
        result
    }
}

impl AssertionItem {
    pub fn get_expressions(&self) -> Vec<Expression> {
        match self {
            AssertionItem::Equal(equal) => {
                let mut result = vec![];
                result.extend(equal.left.get_expressions());
                result.extend(equal.right.get_expressions());
                result
            }
            AssertionItem::NotEqual(not_equal) => {
                let mut result = vec![];
                result.extend(not_equal.left.get_expressions());
                result.extend(not_equal.right.get_expressions());
                result
            }
        }
    }

    pub fn evaluate(&self, context: &Value) -> Option<String> {
        match self {
            AssertionItem::Equal(equal_comp) => {
                evaluate_equal_to_comparison(equal_comp, context)
            }
            AssertionItem::NotEqual(not_equal_comp) => {
                evaluate_equal_to_comparison(not_equal_comp, context)
            }
        }
    }
}

fn evaluate_equal_to_comparison(equal_to_comparison: &EqualToComparison, context: &Value) -> Option<String> {
    let left = equal_to_comparison.left.resolve(context.clone());
    let right = equal_to_comparison.right.resolve(context.clone());
    let is_equal = left == right;
    if equal_to_comparison.negate {
        if is_equal {
            Some(format!("expected not equal but `{}` == `{}`", left, right))
        } else {
            None
        }
    } else {
        if is_equal {
            None
        } else {
            Some(format!("expected equal but `{}` != `{}`", left, right))
        }
    }
}

impl DynamicValue {
    pub fn get_expressions(&self) -> Vec<Expression> {
        match self {
            DynamicValue::Simple(expression) => {
                vec![expression.clone()]
            }
            DynamicValue::Collection(dynamic_vals) => {
                dynamic_vals.iter()
                    .flat_map(|dynamic_value| dynamic_value.get_expressions())
                    .collect()
            }
            DynamicValue::Map(dynamic_map) => {
                dynamic_map.iter()
                    .flat_map(|(key, value)| {value.get_expressions()})
                    .collect()
            }
        }
    }
}

