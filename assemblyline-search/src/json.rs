use std::borrow::Cow;

use anyhow::Result;
use chrono::{DateTime, Utc};
use crate::lucene::{FieldQuery, NumberQuery, ParsingError, PrefixOperator, Query, RangeBound, RangeQuery, RangeTerm, StringQuery};

#[derive(Debug, thiserror::Error)]
#[error("A nested query targeted a value that wasn't a list")]
pub struct NestedFieldError;

pub trait JsonFilter {
    fn get_full_text_fields(data: &serde_json::Value) -> Vec<&serde_json::Value>;

    fn get_field<'a>(data: &'a serde_json::Value, fields: &[String]) -> Vec<&'a serde_json::Value> {
        if let Some(data) = data.as_array() && !fields.is_empty() {
            let mut out = vec![];
            for item in data {
                out.extend(Self::get_field(item, fields))
            }
            return out;
        }

        if let Some(field) = fields.first() {
            if let Some(data) = data.as_object() {
                match data.get(field) {
                    Some(field_data) => {
                        return Self::get_field(field_data, &fields[1..])
                    },
                    None => return vec![],
                }
            }
            return vec![]
        } else {
            return vec![data]
        }
    }

    fn test(query: &Query, data: &serde_json::Value) -> Result<bool> {
        match query {
            Query::And(parts) => {
                for part in parts {
                    if !Self::test(part, data)? {
                        return Ok(false)
                    }
                }
                return Ok(true)
            },
            Query::Or(parts) => {
                for part in parts {
                    if Self::test(part, data)? {
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::Not(part) => Ok(!Self::test(part, data)?),
            Query::MatchAny(query) => {
                for field in Self::get_full_text_fields(data) {
                    if Self::test_string(query, field)? {
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::RegexAny(query) => {
                for field in Self::get_full_text_fields(data) {
                    let temp: String;
                    let data = match field.as_str() {
                        Some(field) => field.trim(),
                        None => {
                            temp = field.to_string();
                            &temp
                        }
                    };
                    if query.is_match(data) {
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::MatchField(field, query) => {
                let fields = Self::get_field(data, field);
                // println!("Field list: {field:?} ");
                // println!("\t{:?}", data);
                // println!("\t{:?}", fields);
                if fields.is_empty() {
                    return Ok(false)
                }
                for field in fields {
                    if Self::test_field(query, field)? {
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::FieldExists(field) => {
                Ok(!Self::get_field(data, field).is_empty())
            },
        }
    }

    fn test_string(query: &StringQuery, data: &serde_json::Value) -> Result<bool> {
        let data: Cow<str> = make_string(data);

        Ok(match query.operator {
            Some(PrefixOperator::Require) => data.contains(&query.value),
            Some(PrefixOperator::Forbid) => !data.contains(&query.value),
            Some(PrefixOperator::GreaterThanOrEqual) => *data >= query.value[..],
            Some(PrefixOperator::LessThanOrEqual) => *data <= query.value[..],
            Some(PrefixOperator::GreaterThan) => *data > query.value[..],
            Some(PrefixOperator::LessThan) => *data < query.value[..],
            None => data.contains(&query.value),
        })
    }

    fn test_field(field: &FieldQuery, data: &serde_json::Value) -> Result<bool> {
        match field {
            FieldQuery::Regex(regex) => {
                let temp: String;
                let data = match data.as_str() {
                    Some(data) => data.trim(),
                    None => {
                        temp = data.to_string();
                        &temp
                    }
                };
                // println!("Regex {regex} on {data}, {}", regex.is_match(data));
                Ok(regex.is_match(data))
            },
            FieldQuery::Match(query) => Self::test_string(query, data),
            FieldQuery::Number(query) => Self::test_number(query, data),
            FieldQuery::Range(query) => Self::test_range(query, data),
            FieldQuery::Or(parts) => {
                for part in parts {
                    if Self::test_field(part, data)? {
                        return Ok(true)
                    }
                }
                Ok(false)
            },
            FieldQuery::And(parts) => {
                for part in parts {
                    if !Self::test_field(part, data)? {
                        return Ok(false)
                    }
                }
                Ok(true)
            },
            FieldQuery::Not(part) => Ok(!Self::test_field(part, data)?),
            FieldQuery::Nested(query) => {
                if let Some(data) = data.as_array() {
                    for row in data {
                        if Self::test(query, row)? {
                            return Ok(true)
                        }
                    }
                    return Ok(false)
                } else {
                    Err(NestedFieldError.into())
                }
            },
        }
    }

    fn test_number(query: &NumberQuery, data: &serde_json::Value) -> Result<bool> {
        if let Some(data) = data.as_f64() {
            return Ok(match query.operator {
                Some(PrefixOperator::Require) => data == query.value,
                Some(PrefixOperator::Forbid) => data != query.value,
                Some(PrefixOperator::GreaterThanOrEqual) => data >= query.value,
                Some(PrefixOperator::LessThanOrEqual) => data <= query.value,
                Some(PrefixOperator::GreaterThan) => data > query.value,
                Some(PrefixOperator::LessThan) => data < query.value,
                None => data == query.value,
            })
        }
        Ok(false)
    }

    fn test_range(query: &RangeQuery, data: &serde_json::Value) -> Result<bool> {
        match &query.start {
            RangeTerm::Wildcard => {},
            RangeTerm::Date(start) => {
                let data: DateTime<Utc> = make_date(data)?;
                let bound = start.resolve().ok_or(ParsingError::InvalidDate(start.to_string()))?;
                match query.start_bound {
                    RangeBound::Inclusive => if data < bound {
                        return Ok(false)
                    },
                    RangeBound::Exclusive => if data <= bound {
                        return Ok(false)
                    },
                }
            },
            RangeTerm::Value(start) => {
                let data = make_string(data);
                // println!("{data} <~ {start}");
                match query.start_bound {
                    RangeBound::Inclusive => if *data < start[..] {
                        return Ok(false)
                    },
                    RangeBound::Exclusive => if *data <= start[..] {
                        return Ok(false)
                    },
                }
            },
            RangeTerm::Numeric(start) => {
                match data.as_f64() {
                    Some(value) => {
                        match query.start_bound {
                            RangeBound::Inclusive => if value < *start {
                                return Ok(false)
                            },
                            RangeBound::Exclusive => if value <= *start {
                                return Ok(false)
                            },
                        }
                    },
                    None => {
                        return Ok(false)
                    }
                }
            },
        }
        match &query.end {
            RangeTerm::Wildcard => {},
            RangeTerm::Date(end) => {
                let data: DateTime<Utc> = make_date(data)?;
                let bound = end.resolve().ok_or(ParsingError::InvalidDate(end.to_string()))?;
                match query.end_bound {
                    RangeBound::Inclusive => if bound < data {
                        return Ok(false)
                    },
                    RangeBound::Exclusive => if bound <= data {
                        return Ok(false)
                    },
                }
            },
            RangeTerm::Value(end) => {
                let data = make_string(data);
                // println!("{end} <~ {data}");
                match query.end_bound {
                    RangeBound::Inclusive => if end[..] < *data {
                        return Ok(false)
                    },
                    RangeBound::Exclusive => if end[..] <= *data {
                        return Ok(false)
                    },
                }
            },
            RangeTerm::Numeric(end) => {
                match data.as_f64() {
                    Some(value) => {
                        match query.end_bound {
                            RangeBound::Inclusive => if *end < value {
                                return Ok(false)
                            },
                            RangeBound::Exclusive => if *end <= value {
                                return Ok(false)
                            },
                        }
                    },
                    None => {
                        return Ok(false)
                    }
                }
            },
        }
        return Ok(true)
    }
}


fn make_string(data: &'_ serde_json::Value) -> Cow<'_, str> {
    match data.as_str() {
        Some(data) => data.trim().into(),
        None => data.to_string().into()
    }
}

fn make_date(data: &serde_json::Value) -> Result<DateTime<Utc>> {
    if let Some(data) = data.as_str() {
        if let Ok(date) = DateTime::parse_from_rfc3339(data) {
            return Ok(date.into())
        }

        if let Ok(date) = DateTime::parse_from_rfc2822(data) {
            return Ok(date.into())
        }

        return Err(ParsingError::InvalidDate(data.to_owned()).into())
    }
    return Err(ParsingError::InvalidDate(data.to_string()).into())
}



pub struct AllFields;
impl JsonFilter for AllFields {
    fn get_full_text_fields(data: &serde_json::Value) -> Vec<&serde_json::Value> {
        let mut output = vec![];
        let mut input = vec![data];
        while let Some(current) = input.pop() {
            match current {
                serde_json::Value::Array(values) => {
                    for value in values {
                        output.extend(Self::get_full_text_fields(value))
                    }
                },
                serde_json::Value::Object(map) => {
                    for value in map.values() {
                        output.extend(Self::get_full_text_fields(value))
                    }
                },
                serde_json::Value::Null => {},
                value => output.push(value)
            }
        }
        output
    }
}


// fn get_full_text_fields(data: &serde_json::Value) -> Vec<&serde_json::Value> {
//     let mut output = vec![];
//     for field in get_full_text_field_names() {
//         output.extend(get_field(data, field));
//     }
//     if let Some(map) = data.as_object() {
//         if let Some(tags) = map.get("tags") {
//             output.extend(get_all_fields(tags));
//         }
//     }
//     return output;
// }


