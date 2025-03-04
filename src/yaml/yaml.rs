use crate::auth::http::AuthenticationProvider;
use crate::expression::expression::{DynamicValue, Expression};
use crate::model::NodeConfig::{AssertionNode, BranchNode};
use crate::model::{AssertionConfig, AssertionItem, Branch, BranchConfig, ConditionConfig, EqualToComparison, Graph, HttpConfig, Node, NodeConfig, NodeId, WorkflowConfiguration};
use serde::Deserialize;
use serde_json::{Value as JsonValue, Value};
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct Workflow {
    pub name: String,
    pub nodes: Vec<NodeValue>,
    pub config: Option<WorkflowConfiguration>,
}

#[derive(Debug, Deserialize, Clone)]
pub enum NodeValue {
    Http(HttpDetails),
    Condition(ConditionDetails),
    Branch(BranchDetails),
    Assertion(AssertionDetails),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Http {
    pub http: HttpDetails,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpDetails {
    pub id: String,
    pub url: YamlValue,
    pub method: String,
    pub content_type: Option<String>,
    pub headers: Option<HashMap<String, YamlValue>>,
    pub params: Option<HashMap<String, YamlValue>>,
    pub body: Option<YamlValue>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Condition {
    pub condition: ConditionDetails,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConditionDetails {
    pub id: String,
    pub expression: String,
    pub true_branch: Option<Vec<NodeValue>>,
    pub false_branch: Option<Vec<NodeValue>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BranchDetails {
    pub id: String,
    pub condition: Option<String>,
    pub branches: HashMap<String, Vec<NodeValue>>,
}

#[derive(Debug, Deserialize, Clone)]
pub enum AssertionDetail {
    Equals(EqualityCheck),
    NotEquals(EqualityCheck),
}

#[derive(Debug, Deserialize, Clone)]
pub struct EqualityCheck {
    pub left: YamlValue,
    pub right: YamlValue,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AssertionDetails {
    pub id: String,
    pub assertions: Vec<AssertionDetail>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthProviderDetails {
    pub providers: Vec<AuthenticationProvider>
}

impl NodeValue {
    fn node_id(&self) -> NodeId {
        match self {
            NodeValue::Http(http_details) => NodeId::of(http_details.id.clone()),
            NodeValue::Condition(condition_details) => NodeId::of(condition_details.id.clone()),
            NodeValue::Branch(branch_details) => {NodeId::of(branch_details.id.clone())}
            NodeValue::Assertion(assertion_details) => {NodeId::of(assertion_details.id.clone())}
        }
    }
}

fn traverse(node: NodeValue, configs: &mut Vec<Node>) {
    match &node {
        NodeValue::Http(http) => {
            configs.push(Node::http(
                node.node_id(),
                HttpConfig {
                    url: resolve_dynamic_from_yaml_value(http.url.clone()),
                    headers: match &http.headers {
                        None => {HashMap::new()}
                        Some(headers) => {
                            headers.iter()
                                .map(|(k, v)| (k.clone(), resolve_dynamic_from_yaml_value(v.clone())))
                                .collect()
                        }
                    },
                    params: match &http.params {
                        None => {HashMap::new()}
                        Some(params) => {
                            params.iter()
                                .map(|(k, v)| (k.clone(), resolve_dynamic_from_yaml_value(v.clone())))
                                .collect()
                        }
                    },
                    method: http.method.clone(),
                    body: match http.body.clone() {
                        None => {
                            None
                        }
                        Some(body_val) => {
                            Some(resolve_dynamic_from_yaml_value(body_val))
                        }
                    },
                    content_type: match &http.content_type {
                        None => {"application/json".to_string()}
                        Some(content_type) => {
                            content_type.to_string()
                        }
                    },
                },
            ));
        }
        NodeValue::Condition(condition) => {
            configs.push(Node::of(node.node_id(), NodeConfig::ConditionNode(ConditionConfig {
                expression: resolve_expression(YamlValue::String(condition.expression.clone())),
                true_branch: match &condition.true_branch {
                    None => {vec![]}
                    Some(true_nodes) => {
                        true_nodes.iter()
                            .map(|node|node.node_id())
                            .collect()
                    }
                },
                false_branch: match &condition.false_branch {
                    None => {vec![]}
                    Some(false_nodes) => {
                        false_nodes.iter()
                            .map(|node|node.node_id())
                            .collect()
                    }
                },
            })));
            if let Some(true_nodes) = condition.true_branch.clone() {
                true_nodes.iter().for_each(|node| {
                    traverse(node.clone(), configs);
                });
            }
            if let Some(false_nodes) = condition.false_branch.clone() {
                false_nodes.iter().for_each(|node| {
                    traverse(node.clone(), configs);
                });
            }
        }
        NodeValue::Branch(branch_details) => {
            let branches = branch_details.branches
                .iter()
                .map(|(branch_name, childs)| Branch {
                    name: branch_name.clone(),
                    condition: match &branch_details.condition {
                        None => {None}
                        Some(expression) => {
                            Some(Expression::of_path(expression.clone()))
                        }
                    },
                    nodes: childs.iter()
                        .map(|child|child.node_id())
                        .collect(),
                })
                .collect();
            configs.push(Node::of(node.node_id(), BranchNode(BranchConfig{
                branches
            })));
            branch_details.branches
                .iter()
                .for_each(|(branch_name, childs)| {
                    childs.iter()
                        .for_each(|child| traverse(child.clone(), configs));
                })
        }
        NodeValue::Assertion(assertion_details) => {
            configs.push(Node::of(node.node_id(), AssertionNode(AssertionConfig {
                assertions: assertion_details.assertions
                    .iter()
                    .map(|assertion_item|{
                        match assertion_item {
                            AssertionDetail::Equals(eq_check) => {
                                AssertionItem::Equal(EqualToComparison{
                                    left: resolve_dynamic_from_yaml_value(eq_check.left.clone()),
                                    right: resolve_dynamic_from_yaml_value(eq_check.right.clone()),
                                    negate: false,
                                })
                            }
                            AssertionDetail::NotEquals(ne_check) => {
                                AssertionItem::Equal(EqualToComparison{
                                    left: resolve_dynamic_from_yaml_value(ne_check.left.clone()),
                                    right: resolve_dynamic_from_yaml_value(ne_check.right.clone()),
                                    negate: true,
                                })
                            }
                        }
                    })
                    .collect(),
            })))
        }
    }
}

pub fn from_yaml_to_auth(file_name: &str) -> AuthProviderDetails {
    let mut file = File::open(file_name).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    serde_yaml::from_str::<AuthProviderDetails>(&contents).unwrap()
}


pub fn from_yaml(file_name: &str) -> Graph {
    let mut file = File::open(file_name).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let workflow = serde_yaml::from_str::<Workflow>(&contents).unwrap();
    let mut configs: Vec<Node> = vec![];
    workflow.nodes.iter().for_each(|node| {
        traverse(node.clone(), &mut configs);
        println!("{:?}", configs);
    });
    println!("{:#?}", configs);
    let graph: Graph = Graph {
        nodes_by_id: configs
            .iter()
            .map(|config| (config.id.clone(), config.config.clone()))
            .collect(),
        node_ids: workflow.nodes.iter().map(|node| node.node_id()).collect(),
        id: workflow.name,
        config: match &workflow.config {
            None => {Default::default()}
            Some(workflow_config) => {
                workflow_config.clone()
            }
        },
    };
    println!("{:?}", graph);
    graph
}

fn resolve_expression(from: YamlValue) -> Expression {
    // Convert `serde_yaml::Value` to `serde_json::Value`
    println!("yaml val: {:?}", from);
    let json_value: JsonValue = serde_json::to_value(from).unwrap();

    match json_value {
        Value::String(string) => {
            if string.starts_with("{{") {
                let unwrapped = string.strip_prefix("{{").unwrap()
                    .to_string()
                    .replace("}}", "")
                    .trim()
                    .to_string();
                println!("unwrapped: {}", unwrapped);
                Expression::of_path(unwrapped)
            } else {
                Expression::of_value(Value::String(string))
            }
        }
        _ => Expression::of_value(json_value),
    }
}

fn resolve_dynamic_from_yaml_value(from: YamlValue) -> DynamicValue {
    println!("yaml val: {:?}", from);
    let json_value: JsonValue = serde_json::to_value(from).unwrap();
    resolve_dynamic_json_value(json_value)
}

fn resolve_dynamic_json_value(json_value: JsonValue) -> DynamicValue {

    match json_value {
        Value::String(string) => {
            if string.starts_with("{{") {
                let unwrapped = string.strip_prefix("{{").unwrap()
                    .to_string()
                    .replace("}}", "")
                    .trim()
                    .to_string();
                DynamicValue::Simple(Expression::of_path(unwrapped))
            } else {
                DynamicValue::Simple(Expression::of_value(Value::String(string)))
            }
        }
        Value::Array(items) => {
            let dynamic_values = items.iter()
                .map(|item| resolve_dynamic_json_value(item.clone()))
                .collect();
            DynamicValue::Collection(dynamic_values)
        }
        Value::Object(map) => {
            let dynamic_value_map: HashMap<String, DynamicValue> = map.iter()
                .map(|(k, v)| (k.clone(), resolve_dynamic_json_value(v.clone())))
                .collect();
            DynamicValue::Map(dynamic_value_map)
        }
        _ => {DynamicValue::Simple(Expression::of_value(json_value))}
    }
}

fn serialize() {
    let yaml_str = r#"
workflow: test
nodes:
  - http:
      id: create_todo
      url: https://blabla.sdasd.com/todos
      method: POST
      body: "{{input}}"
  - condition:
      id: if_a_condition
      expression: "{{input.asdasd}}"
      true_branch:
        - http:
            id: get_todo
            url: "{{https://blabla.sdasd.com/todos/create_todo.id}}"
            method: GET
    "#;

    let workflow: Workflow = serde_yaml::from_str(yaml_str).unwrap();
    println!("{:#?}", workflow);
}

#[cfg(test)]
mod tests {
    use crate::yaml::yaml::{from_yaml, serialize};

    #[test]
    fn test_to_json_value() {
        serialize();
    }

    #[test]
    fn test_from_yaml() {
        from_yaml("resources/workflows/workflow.yaml");
    }
    #[test]
    fn test_from_yaml_og() {
        from_yaml("resources/workflows/opsgenie.yaml");
    }
}
