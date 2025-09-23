use std::str::FromStr;

use serde_json::Value;


pub fn validate_string(value: Value) -> Result<Value, Value> {
    match value {
        Value::String(value) => Ok(Value::String(value)),
        Value::Number(number) => Ok(Value::String(number.to_string())),
        value => Err(value)
    }
}

pub fn validate_uppercase(value: Value) -> Result<Value, Value> {
    match value {
        Value::String(value) => Ok(Value::String(value.to_uppercase())),
        Value::Number(number) => Ok(Value::String(number.to_string())),
        value => Err(value)
    }
}

pub fn validate_lowercase(value: Value) -> Result<Value, Value> {
    match value {
        Value::String(value) => Ok(Value::String(value.to_lowercase())),
        Value::Number(number) => Ok(Value::String(number.to_string())),
        value => Err(value)
    }
}

pub fn validate_string_with<Filter: Fn(&str) -> bool>(value: Value, filter: Filter) -> Result<Value, Value> {
    let transformed = match &value {
        Value::String(value) => value,
        Value::Number(number) => &number.to_string(),
        _ => return Err(value)
    };

    if filter(transformed) {
        Ok(Value::String(transformed.to_owned()))
    } else {
        Err(value)
    }
}

pub fn transform_string_with<Filter: Fn(&str) -> Option<String>>(value: Value, filter: Filter) -> Result<Value, Value> {
    let transformed = match &value {
        Value::String(value) => value,
        Value::Number(number) => &number.to_string(),
        _ => return Err(value)
    };

    if let Some(transformed) = filter(transformed) {
        Ok(Value::String(transformed))
    } else {
        Err(value)
    }
}

pub fn validate_uppercase_with<Filter: Fn(&str) -> bool>(value: Value, filter: Filter) -> Result<Value, Value> {
    let transformed = match &value {
        Value::String(value) => value.to_uppercase(),
        Value::Number(number) => number.to_string(),
        _ => return Err(value)
    };

    if filter(&transformed) {
        Ok(Value::String(transformed))
    } else {
        Err(value)
    }
}


pub fn validate_lowercase_with<Filter: Fn(&str) -> bool>(value: Value, filter: Filter) -> Result<Value, Value> {
    let transformed = match &value {
        Value::String(value) => value.to_lowercase(),
        Value::Number(number) => number.to_string(),
        _ => return Err(value)
    };

    if filter(&transformed) {
        Ok(Value::String(transformed))
    } else {
        Err(value)
    }
}


pub fn validate_number<TYPE: FromStr + Into<serde_json::Number>>(value: Value) -> Result<Value, Value> {
    match value {
        Value::String(value) => {
            let value: TYPE = match value.parse() {
                Ok(value) => value,
                Err(_) => return Err(Value::String(value))
            };
            Ok(Value::Number(value.into()))
        },
        Value::Bool(true) => Ok(Value::Number(1.into())),
        Value::Bool(false) => Ok(Value::Number(0.into())),
        Value::Number(number) => {
            // TODO this is less efficient than it could be if we didn't go round trip to string
            let value: TYPE = match number.to_string().parse() {
                Ok(value) => value,
                Err(_) => return Err(Value::String(number.to_string()))
            };
            Ok(Value::Number(value.into()))
        },
        value => Err(value)
    }
}

// HashMap<String, Vec<String>>
pub fn validate_rule_mapping(root: Value) -> Result<Value, Value> {
    match root { 
        Value::Object(mut obj) => {
            for (_, value) in &mut obj {
                if let Value::Array(items) = value {
                    for item in items {
                        if !item.is_string() {
                            *item = Value::String(item.to_string())
                        }
                    }
                } else {
                    return Err(Value::Object(obj))
                }
            }
            Ok(Value::Object(obj))
        },
        value => Err(value)
    }
}
