use aws_sdk_dynamodb::primitives::DateTime;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use jmespath::functions::{ArgumentType, CustomFunction, Signature};
use jmespath::{ErrorReason, JmespathError, Rcvar, Variable};
use serde_json::Number;
use std::time::SystemTime;

pub fn base64_encode() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::String], None),
        Box::new(|args, ctx| {
            let subject = args[0].as_string().ok_or_else(|| {
                JmespathError::new(
                    "",
                    0,
                    ErrorReason::Parse("Expected args[0] to be a valid string".to_owned()),
                )
            })?;
            let result = BASE64_STANDARD.encode(subject);
            Ok(Rcvar::new(Variable::String(String::from(result))))
        }),
    )
}

pub fn split() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::String, ArgumentType::String], None),
        Box::new(|args, ctx| {
            let subject = args[0].as_string().ok_or_else(|| {
                JmespathError::new(
                    "",
                    0,
                    ErrorReason::Parse("Expected args[0] to be a valid string".to_owned()),
                )
            })?;
            let delimiter = args[0].as_string().ok_or_else(|| {
                JmespathError::new(
                    "",
                    0,
                    ErrorReason::Parse("Expected args[1] to be a valid string".to_owned()),
                )
            })?;
            let result = subject.split(delimiter).collect::<Vec<&str>>();
            Ok(Rcvar::new(Variable::Array(result.iter()
                .map(|i|Rcvar::new(Variable::String(i.to_string())))
                .collect())))
        }),
    )
}

pub fn concat() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::Array], None),
        Box::new(|args, ctx| {
            let result = args.into_iter()
                .filter(|arg| arg.is_string())
                .map(|arg| arg.as_string().unwrap().clone())
                .collect::<Vec<String>>().join("").to_string();
            Ok(Rcvar::new(Variable::String(String::from(result))))
        }),
    )
}

pub fn now() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![], None),
        Box::new(|args, ctx| {
            let now_millis = DateTime::from(SystemTime::now()).to_millis().unwrap();
            Ok(Rcvar::new(Variable::Number(Number::from(now_millis))))
        }),
    )
}

pub fn uuid() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![], None),
        Box::new(|args, ctx| {
            Ok(Rcvar::new(Variable::String(uuid::Uuid::new_v4().to_string())))
        }),
    )
}

pub fn capitalize() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::String], None),
        Box::new(|args, ctx| {
            let subject = args[0].as_string().ok_or_else(|| {
                JmespathError::new(
                    "",
                    0,
                    ErrorReason::Parse("Expected args[0] to be a valid string".to_owned()),
                )
            })?;
            let result = subject.to_uppercase();
            Ok(Rcvar::new(Variable::String(String::from(result))))
        }),
    )
}

pub fn base64_decode() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::String], None),
        Box::new(|args, ctx| {
            let subject = args[0].as_string().ok_or_else(|| {
                JmespathError::new(
                    "",
                    0,
                    ErrorReason::Parse("Expected args[0] to be a valid string".to_owned()),
                )
            })?;
            let decode_result = BASE64_STANDARD.decode(subject);
            match decode_result {
                Ok(decoded) => {
                    Ok(Rcvar::new(Variable::String(String::from_utf8(decoded).unwrap())))
                }
                Err(err) => {
                    return Err(JmespathError::new(
                        "",
                        0,
                        ErrorReason::Parse("Unable to decode str".to_owned()),
                    ))
                }
            }
        }),
    )
}
