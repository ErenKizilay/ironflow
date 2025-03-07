use std::collections::{BTreeMap, HashMap};
use crate::model::NodeId;
use jmespath::{ Rcvar, Variable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::rc::Rc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Expression {
    pub path: Option<String>,
    pub value: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DynamicValue {
    Simple(Expression),
    Collection(Vec<DynamicValue>),
    Map(HashMap<String, DynamicValue>),
}

impl DynamicValue {

    pub fn resolve(&self, context: Value) -> Value {
        match self {
            DynamicValue::Simple(expression) => {
                expression.evaluate(context.clone())
            }
            DynamicValue::Collection(expressions) => {
                let evaluated_values = expressions.iter()
                    .map(|dynamic_val|dynamic_val.resolve(context.clone()))
                    .collect();
                Value::Array(evaluated_values)
            }
            DynamicValue::Map(expression_map) => {
                let evaluated_value_map: HashMap<String, Value> = expression_map.iter()
                    .map(|(key, dynamic_val)| { (key.clone(), dynamic_val.resolve(context.clone())) })
                    .collect();
                serde_json::to_value(&evaluated_value_map).unwrap()
            }
        }
    }
}


impl Expression {
    pub fn of_path(path: String) -> Expression {
        Expression {
            path: Some(path.clone()), value: None,
        }
    }

    pub fn of_value(v: Value) -> Expression {
        Expression { path: None, value: Some(v) }
    }

    pub fn of_str(str: &str) -> Expression {
        Expression { path: Some(str.to_string()), value: None }
    }

    pub fn get_referred_nodes(&self, candidates: Vec<NodeId>) -> Vec<NodeId> {
        match &self.path {
            None => {
                vec![]
            }
            Some(jmespath) => {
                candidates.iter()
                    .filter(|node_id|jmespath.contains(&node_id.name))
                    .cloned()
                    .collect()
            }
        }
    }

    pub fn evaluate(&self, context: Value) -> Value {
        match &self.value {
            None => {
                let path = self.path.clone();
                let compilation_result = jmespath::compile(path.unwrap().as_str());
                match compilation_result {
                    Ok(expr) => {
                        let context_json = serde_json::to_string(&context).unwrap();
                        let evaluation_result = Variable::from_json(context_json.as_str());
                        match evaluation_result {
                            Ok(data) => {
                                let result = expr.search(data).unwrap();
                                let json_value = to_json_value(result);
                                json_value
                            },
                            Err(err) => {
                                tracing::warn!("evaluation error[{:?}] occurred. expr: {:?} context: {:?}", err, expr, context);
                                Value::Null
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!("expression: {:?} compilation error: {:?}", self.path, err);
                        Value::Null
                    }
                }
            }
            Some(val) => {
                val.clone()
            }
        }
    }
}

fn to_json_value(rc: Rcvar) -> Value {
    match Rc::try_unwrap(rc).unwrap() {
        Variable::Null => {
            Value::Null
        }
        Variable::String(string) => {
            Value::String(string)
        }
        Variable::Bool(boolean) => {
            Value::Bool(boolean)
        }
        Variable::Number(number) => {
            Value::Number(number)
        }
        Variable::Array(array) => {
            Value::Array(array.into_iter().map(to_json_value).collect())
        }
        Variable::Object(object) => {
            Value::Object(object.into_iter().map(|(k, v)| (k, to_json_value(v))).collect())
        }
        Variable::Expref(ast) => {
            Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use jmespath::{compile, Variable};
    use crate::expression::expression::{DynamicValue, Expression};
    use crate::model::NodeId;
    use serde_json::{json, Map};
    use serde_yaml::Value;

    #[test]
    fn test_to_json_value() {
        let context = json!({"foo": {"bar": {"baz": true}}});
        let expression = Expression::of_path(String::from("foo.bar | baz"));
        let actual = expression.evaluate(context);
        assert_eq!(actual, true);
    }

    #[test]
    fn test_to_referred_nodes() {
        let expression = Expression::of_path(String::from("{ \"fields\": input, \"id\": get_uuid.uuid }"));
        let actual = expression.get_referred_nodes(vec![NodeId::of("get_uuid".to_string())]);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual.get(0).unwrap(), &NodeId::of("get_uuid".to_string()));
    }

    #[test]
    fn test_resolve() {
        let context = json!({"foo": {"bar": {"baz": true}}});
        let dynamic_value = DynamicValue::Map(HashMap::from([
            ("k1".to_string(), DynamicValue::Simple(Expression::of_value(serde_json::Value::String("hardcoded".to_string())))),
            ("k2".to_string(), DynamicValue::Simple(Expression::of_str("foo.bar"))),
            ("k3".to_string(), DynamicValue::Collection(vec![DynamicValue::Simple(Expression::of_str("foo.bar.baz"))])),
        ]));
        let result = dynamic_value.resolve(context);

        println!("{}", result);
        let expected = r#"{"k1":"hardcoded","k2":{"baz":true},"k3":[true]}"#;
        assert_eq!(expected, result.to_string());
        assert!(result.is_object())
    }

}