use crate::expression::custom_functions::{base64_decode, base64_encode, capitalize, concat, now, split, uuid};
use crate::model::NodeId;
use jmespath::functions::{ArgumentType, CustomFunction, Signature};
use jmespath::{JmespathError, Rcvar, Runtime, Variable, DEFAULT_RUNTIME};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::collections::{BTreeMap, HashMap};
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



lazy_static! {
    pub static ref CUSTOM_RUNTIME: Runtime = {
        let mut runtime = Runtime::new();
        runtime.register_builtin_functions();
        runtime.register_function("base64_encode", Box::new(base64_encode()));
        runtime.register_function("base64_decode", Box::new(base64_decode()));
        runtime.register_function("capitalize", Box::new(capitalize()));
        runtime.register_function("now", Box::new(now()));
        runtime.register_function("uuid", Box::new(uuid()));
        runtime.register_function("concat", Box::new(concat()));
        runtime.register_function("split", Box::new(split()));
        runtime
    };
}




impl DynamicValue {

    //todo solve double "" issue on string results
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
                let compilation_result = compile(path.unwrap().as_str());
                match compilation_result {
                    Ok(expr) => {
                        let context_json = serde_json::to_string(&context).unwrap();
                        let evaluation_result = Variable::from_json(context_json.as_str());
                        match evaluation_result {
                            Ok(data) => {
                                let result = expr.search(data);
                                match result {
                                    Ok(search_result) => {
                                        let json_value = to_json_value(search_result);
                                        json_value
                                    }
                                    Err(err) => {
                                        tracing::warn!("search error[{:?}] occurred. expr: {:?} context: {:?}", err, expr, context);
                                        Value::Null
                                    }
                                }
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

pub fn value_as_string(value: Value) -> String {
    value.as_str().unwrap().to_string()
        .trim_matches('"')
        .to_string()
}

fn compile(expression: &str) -> Result<jmespath::Expression<'static>, JmespathError> {
    CUSTOM_RUNTIME.compile(expression)
}

#[cfg(test)]
mod tests {
    use crate::expression::expression::{DynamicValue, Expression};
    use crate::model::NodeId;
    use aws_sdk_dynamodb::primitives::DateTime;
    use serde_json::json;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::SystemTime;

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

    #[test]
    fn test_encode() {
        let context = json!({"foo": {"bar": {"baz": "hardcoded"}}});
        let encode_expr = DynamicValue::Simple(Expression::of_str("base64_encode(foo.bar.baz)"));
        let decode_expr = DynamicValue::Simple(Expression::of_str("base64_decode(base64_encode(foo.bar.baz))"));
        let result = decode_expr.resolve(context);

        println!("result: {}", result);
        assert_eq!("hardcoded", result.as_str().unwrap());
    }

    #[test]
    fn test_capitalize() {
        let context = json!({"foo": {"bar": {"baz": "hardcoded"}}});
        let expr = DynamicValue::Simple(Expression::of_str("capitalize(foo.bar.baz)"));
        let result = expr.resolve(context);

        println!("result: {}", result);
        assert_eq!("HARDCODED", result.as_str().unwrap());
    }

    #[test]
    fn test_now() {
        let context = json!({"foo": {"bar": {"baz": "hardcoded"}}});
        let expr = DynamicValue::Simple(Expression::of_str("now()"));
        let result = expr.resolve(context);

        println!("result: {}", result);
        let now_millis = DateTime::from(SystemTime::now()).to_millis().unwrap();
        let diff = now_millis - result.as_number().unwrap().as_i64().unwrap();
        assert!(diff < 1000);
    }

    #[test]
    fn test_uuid() {
        let context = json!({"foo": {"bar": {"baz": "hardcoded"}}});
        let expr = DynamicValue::Simple(Expression::of_str("uuid()"));
        let result = expr.resolve(context);

        println!("result: {}", result);
        let uuid = uuid::Uuid::from_str(result.to_string().trim_matches('"').to_string().as_str()).unwrap();
        println!("uuid: {}", uuid);
    }

}