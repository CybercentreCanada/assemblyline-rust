use std::borrow::Cow;
use std::str::FromStr;
use std::sync::OnceLock;
use assemblyline_models::ElasticMeta;
use chrono::{DateTime, Datelike, Duration, Months, SubsecRound, Timelike, Utc};
use itertools::Itertools;
use struct_metadata::Described;
use super::ParsingError;

type Result<T, E=ParsingError> = std::result::Result<T, E>;

const CACHE_RESTRICTED_FIELDS: [&str; 5] = ["sid", "max_score", "files", "metadata", "params"];

fn get_full_text_field_names() -> &'static Vec<Vec<String>> {
    static CACHE: OnceLock<Vec<Vec<String>>> = OnceLock::new();

    CACHE.get_or_init(|| {
        // get the fields
        let data = assemblyline_models::datastore::Submission::metadata();
        let struct_metadata::Kind::Struct { children, .. } = data.kind else { panic!() };

        // seed collection
        let mut sections: Vec<(Vec<String>, struct_metadata::Entry<ElasticMeta>)> = children.into_iter()
            .map(|child|(vec![child.label.to_string()], child))
            .collect_vec();

        // collect all fields that are copyied to __text__
        let mut coppied = vec![];
        while let Some((field, data)) = sections.pop() {
            if data.metadata.copyto == Some("__text__") {
                coppied.push(field);
                continue
            }

            // if our type is complex or aliased loop inward until we find the original
            let mut field_kind = data.type_info.kind;
            loop {
                match field_kind {
                    struct_metadata::Kind::Struct { children, .. } => {
                        for child in children {
                            let mut label = field.clone();
                            label.push(child.label.to_string());
                            sections.push((label, child));
                        }
                    },
                    struct_metadata::Kind::Aliased { kind, .. } |
                    struct_metadata::Kind::Sequence(kind) |
                    struct_metadata::Kind::Option(kind) => {
                        field_kind = kind.kind;
                        continue
                    },
                    struct_metadata::Kind::Mapping(_, _) => {},
                    _ => {}
                }
                break 
            }
        }

        return coppied
    })
}


#[derive(Debug, PartialEq, Eq)]
pub enum CacheAvailabilityStatus {
    Ok,
    ErrorUsesForbiddenFields(Vec<String>),
    WarningUsesUnqualifiedSearch
}

impl CacheAvailabilityStatus {

    pub fn can_run(&self) -> bool {
        match self {
            CacheAvailabilityStatus::Ok => true,
            CacheAvailabilityStatus::ErrorUsesForbiddenFields(_) => false,
            CacheAvailabilityStatus::WarningUsesUnqualifiedSearch => true,
        }
    }

    fn merge(self, other: Self) -> Self {
        match self {
            Self::Ok => other,
            Self::ErrorUsesForbiddenFields(mut fields) => match other {
                Self::Ok => Self::ErrorUsesForbiddenFields(fields),
                Self::ErrorUsesForbiddenFields(other_fields) => {
                    fields.extend(other_fields);
                    fields.sort_unstable();
                    fields.dedup();
                    return Self::ErrorUsesForbiddenFields(fields)
                },
                Self::WarningUsesUnqualifiedSearch => Self::ErrorUsesForbiddenFields(fields),
            },
            Self::WarningUsesUnqualifiedSearch => match other {
                Self::Ok => Self::Ok,
                Self::ErrorUsesForbiddenFields(fields) => Self::ErrorUsesForbiddenFields(fields),
                Self::WarningUsesUnqualifiedSearch => Self::WarningUsesUnqualifiedSearch,
            },
        }
    }
}

#[derive(Debug)]
pub enum PrefixOperator {
    Require,
    Forbid,
    GreaterThanOrEqual,
    LessThanOrEqual,
    GreaterThan,
    LessThan,
}

// PREFIX_OPERATOR: "-" | "+" | ">=" | "<=" | ">" | "<"
impl FromStr for PrefixOperator {
    type Err = ParsingError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "-" => PrefixOperator::Forbid,
            "+" => PrefixOperator::Require,
            ">=" => PrefixOperator::GreaterThanOrEqual,
            "<=" => PrefixOperator::LessThanOrEqual,
            ">" => PrefixOperator::GreaterThan,
            "<" => PrefixOperator::LessThan,
            _ => return Err(ParsingError::UnknownPrefixOperator(s.to_owned()))
        })
    }
}

#[derive(Debug)]
pub enum Query {
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
    MatchAny(StringQuery),
    RegexAny(regex::Regex),
    MatchField(Vec<String>, FieldQuery),
    FieldExists(Vec<String>),
}

impl Query {
    pub fn cache_safe(&self) -> CacheAvailabilityStatus {
        match self {
            Query::And(parts) => parts.iter().map(|part|part.cache_safe()).fold(CacheAvailabilityStatus::Ok, |a, b|a.merge(b)),
            Query::Or(parts) => parts.iter().map(|part|part.cache_safe()).fold(CacheAvailabilityStatus::Ok, |a, b|a.merge(b)),
            Query::Not(part) => part.cache_safe(),
            Query::MatchAny(_) => CacheAvailabilityStatus::WarningUsesUnqualifiedSearch,
            Query::RegexAny(_) => CacheAvailabilityStatus::WarningUsesUnqualifiedSearch,
            Query::FieldExists(name) |
            Query::MatchField(name, _) => {
                if let Some(first) = name.first() {
                    if CACHE_RESTRICTED_FIELDS.contains(&&first[..]) {
                        CacheAvailabilityStatus::Ok
                    } else {
                        CacheAvailabilityStatus::ErrorUsesForbiddenFields(vec![name.join(".")])
                    }
                } else {
                    CacheAvailabilityStatus::Ok
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn test(&self, data: &serde_json::Value) -> Result<bool> {
        match self {
            Query::And(parts) => {
                for part in parts {
                    if !part.test(data)? {
                        return Ok(false)
                    }
                }
                return Ok(true)
            },
            Query::Or(parts) => {
                for part in parts {
                    if part.test(data)? {
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::Not(part) => Ok(!part.test(data)?),
            Query::MatchAny(query) => {
                for field in get_full_text_fields(data) {
                    if query.test(field)? {
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::RegexAny(query) => {
                for field in get_full_text_fields(data) {
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
                let fields = get_field(data, field);
                // println!("Field list: {field:?} ");
                // println!("\t{:?}", data);
                // println!("\t{:?}", fields);
                if fields.is_empty() {
                    return Ok(false)
                }
                for field in fields {
                    if query.test(field)? {
                        // println!("hit");
                        return Ok(true)
                    }
                }
                return Ok(false)
            },
            Query::FieldExists(field) => {
                Ok(!get_field(data, field).is_empty())
            },
        }
    }

    fn list_fields(&self) -> Vec<Vec<String>> {
        let mut fields = vec![];
        match self {
            Query::And(parts) => for part in parts {
                fields.extend(part.list_fields().into_iter());
            },
            Query::Or(parts) => for part in parts {
                fields.extend(part.list_fields().into_iter());
            },
            Query::Not(part) => fields.extend(part.list_fields()),
            Query::MatchAny(_) => {},
            Query::RegexAny(_) => {},
            Query::FieldExists(field) |
            Query::MatchField(field, _) => fields.push(field.clone()),
        }
        fields.sort_unstable();
        fields.dedup();
        return fields;
    }
}

fn get_all_fields(data: &serde_json::Value) -> Vec<&serde_json::Value> {
    match data {
        serde_json::Value::Null => vec![],
        serde_json::Value::Array(array) => {
            let mut output = vec![];
            for item in array {
                output.extend(get_all_fields(item));
            }
            output
        },
        serde_json::Value::Object(object) => {
            let mut output = vec![];
            for item in object.values() {
                output.extend(get_all_fields(item));
            }
            output
        },
        _ => vec![data]
    }
}

fn get_full_text_fields(data: &serde_json::Value) -> Vec<&serde_json::Value> {
    let mut output = vec![];
    for field in get_full_text_field_names() {
        output.extend(get_field(data, field));
    }
    if let Some(map) = data.as_object() {
        if let Some(tags) = map.get("tags") {
            output.extend(get_all_fields(tags));
        }
    }
    return output;
}

fn get_field<'a>(data: &'a serde_json::Value, fields: &[String]) -> Vec<&'a serde_json::Value> {
    if let Some(data) = data.as_array() {
        let mut out = vec![];
        for item in data {
            out.extend(get_field(item, fields))
        }
        return out;
    }
    if let Some(field) = fields.first() {
        if let Some(data) = data.as_object() {
            match data.get(field) {
                Some(field_data) => {
                    return get_field(field_data, &fields[1..])
                },
                None => return vec![],
            }
        }
        return vec![]
    } else {
        return vec![data]
    }
}

#[derive(Debug)]
pub enum FieldQuery {
    Regex(regex::Regex),
    Number(NumberQuery),
    Match(StringQuery),
    Range(RangeQuery),
    Or(Vec<FieldQuery>),
    And(Vec<FieldQuery>),
    Not(Box<FieldQuery>)
}

impl FieldQuery {
    fn test(&self, data: &serde_json::Value) -> Result<bool> {
        match self {
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
            FieldQuery::Match(query) => query.test(data),
            FieldQuery::Number(query) => query.test(data),
            FieldQuery::Range(query) => query.test(data),
            FieldQuery::Or(parts) => {
                for part in parts {
                    if part.test(data)? {
                        return Ok(true)
                    }
                }
                Ok(false)
            },
            FieldQuery::And(parts) => {
                for part in parts {
                    if !part.test(data)? {
                        return Ok(false)
                    }
                }
                Ok(true)
            },
            FieldQuery::Not(part) => Ok(!part.test(data)?),
        }
    }
}

#[derive(Debug)]
pub struct StringQuery {
    pub operator: Option<PrefixOperator>,
    pub value: String,
}

fn make_string(data: &serde_json::Value) -> Cow<str> {
    match data.as_str() {
        Some(data) => data.trim().into(),
        None => data.to_string().into()
    }
}

impl StringQuery {
    fn test(&self, data: &serde_json::Value) -> Result<bool> {
        let data: Cow<str> = make_string(data);

        Ok(match self.operator {
            Some(PrefixOperator::Require) => data.contains(&self.value),
            Some(PrefixOperator::Forbid) => !data.contains(&self.value),
            Some(PrefixOperator::GreaterThanOrEqual) => *data >= self.value[..],
            Some(PrefixOperator::LessThanOrEqual) => *data <= self.value[..],
            Some(PrefixOperator::GreaterThan) => *data > self.value[..],
            Some(PrefixOperator::LessThan) => *data < self.value[..],
            None => data.contains(&self.value),
        })
    }
}

#[derive(Debug)]
pub struct NumberQuery {
    pub operator: Option<PrefixOperator>,
    pub value: f64,
}

impl NumberQuery {
    fn test(&self, data: &serde_json::Value) -> Result<bool> {
        if let Some(data) = data.as_f64() {
            return Ok(match self.operator {
                Some(PrefixOperator::Require) => data == self.value,
                Some(PrefixOperator::Forbid) => data != self.value,
                Some(PrefixOperator::GreaterThanOrEqual) => data >= self.value,
                Some(PrefixOperator::LessThanOrEqual) => data <= self.value,
                Some(PrefixOperator::GreaterThan) => data > self.value,
                Some(PrefixOperator::LessThan) => data < self.value,
                None => data == self.value,
            })
        }
        Ok(false)
    }
}


#[derive(Debug, Clone, Copy)]
pub enum RangeBound {
    Inclusive,
    Exclusive
}

#[derive(Debug)]
pub struct RangeQuery {
    pub start: RangeTerm,
    pub end: RangeTerm,
    pub start_bound: RangeBound,
    pub end_bound: RangeBound,
}

#[derive(Debug, Clone)]
pub enum RangeTerm {
    Wildcard,
    Date(DateExpression),
    Numeric(f64),
    Value(String)
}

fn make_date(data: &serde_json::Value) -> Result<DateTime<Utc>> {
    if let Some(data) = data.as_str() {
        if let Ok(date) = chrono::DateTime::parse_from_rfc3339(data) {
            return Ok(date.into())
        }

        if let Ok(date) = chrono::DateTime::parse_from_rfc2822(data) {
            return Ok(date.into())
        }

        return Err(ParsingError::InvalidDate(data.to_owned()))
    }
    return Err(ParsingError::InvalidDate(data.to_string()))
}


impl RangeQuery {
    fn test(&self, data: &serde_json::Value) -> Result<bool> {
        match &self.start {
            RangeTerm::Wildcard => {},
            RangeTerm::Date(start) => {
                let data: DateTime<Utc> = make_date(data)?;
                let bound = start.resolve().ok_or(ParsingError::InvalidDate(start.to_string()))?;
                match self.start_bound {
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
                match self.start_bound {
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
                        match self.start_bound {
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
        match &self.end {
            RangeTerm::Wildcard => {},
            RangeTerm::Date(end) => {
                let data: DateTime<Utc> = make_date(data)?;
                let bound = end.resolve().ok_or(ParsingError::InvalidDate(end.to_string()))?;
                match self.end_bound {
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
                match self.end_bound {
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
                        match self.end_bound {
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

#[derive(Debug, Clone)]
pub enum DateExpression {
    Fixed(chrono::DateTime<chrono::Utc>),
    Relative{changes: Vec<(i64, DateUnit)>, truncation: Option<DateUnit>}
}

impl std::fmt::Display for DateExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DateExpression::Fixed(date) => write!(f, "{date}"),
            DateExpression::Relative { changes, truncation } => {
                write!(f, "NOW")?;
                for (quantity, unit) in changes {
                    write!(f, ":+{quantity}{unit}")?;
                }
                if let Some(trunc) = truncation {
                    write!(f, "/{trunc}")?;
                }
                Ok(())
            },
        }
    }
}

impl DateExpression {
    pub fn apply_changes(mut base: DateTime<Utc>, changes: &Vec<(i64, DateUnit)>) -> Option<DateTime<Utc>> {
        for (quantity, unit) in changes {
            base = match unit {
                DateUnit::Year => if *quantity > 0 {
                    base.checked_add_months(Months::new(12 * *quantity as u32))?
                } else {
                    base.checked_sub_months(Months::new(12 * -*quantity as u32))?
                },
                DateUnit::Month => if *quantity > 0 {
                    base.checked_add_months(Months::new(*quantity as u32))?
                } else {
                    base.checked_sub_months(Months::new(-*quantity as u32))?
                },
                DateUnit::Week => base.checked_add_signed(Duration::try_weeks(*quantity)?)?,
                DateUnit::Day => base.checked_add_signed(Duration::try_days(*quantity)?)?,
                DateUnit::Hour => base.checked_add_signed(Duration::try_hours(*quantity)?)?,
                DateUnit::Minute => base.checked_add_signed(Duration::try_minutes(*quantity)?)?,
                DateUnit::Second => base.checked_add_signed(Duration::try_seconds(*quantity)?)?,
                DateUnit::Millis => base.checked_add_signed(Duration::try_milliseconds(*quantity)?)?,
            }
        }
        Some(base)
    }

    pub fn apply_truncation(base: DateTime<Utc>, trunc: DateUnit) -> DateTime<Utc> {
        use chrono::{NaiveDate, NaiveTime};
        let naive = base.naive_utc();
        let naive_date = naive.date();
        let naive_time = naive.time();
        let offset = base.offset();
        match trunc {
            DateUnit::Year => DateTime::from_naive_utc_and_offset(NaiveDate::from_ymd_opt(naive_date.year(), 1, 1).unwrap().into(), *offset),
            DateUnit::Month => DateTime::from_naive_utc_and_offset(NaiveDate::from_ymd_opt(naive_date.year(), naive_date.month(), 1).unwrap().into(), *offset),
            DateUnit::Week => DateTime::from_naive_utc_and_offset(NaiveDate::from_isoywd_opt(naive_date.year(), naive_date.iso_week().week(), chrono::Weekday::Mon).unwrap().into(), *offset),
            DateUnit::Day => DateTime::from_naive_utc_and_offset(naive_date.into(), *offset),
            DateUnit::Hour => DateTime::from_naive_utc_and_offset(naive_date.and_time(NaiveTime::from_hms_opt(naive_time.hour(), 0, 0).unwrap()), *offset),
            DateUnit::Minute => DateTime::from_naive_utc_and_offset(naive_date.and_time(NaiveTime::from_hms_opt(naive_time.hour(), naive_time.minute(), 0).unwrap()), *offset),
            DateUnit::Second => DateTime::from_naive_utc_and_offset(naive_date.and_time(NaiveTime::from_hms_opt(naive_time.hour(), naive_time.minute(), naive_time.second()).unwrap()), *offset),
            DateUnit::Millis => base.trunc_subsecs(3),
        }
    }

    pub fn resolve(&self) -> Option<DateTime<Utc>> {
        match self {
            DateExpression::Fixed(value) => Some(*value),
            DateExpression::Relative { changes, truncation } => {
                let time = Self::apply_changes(Utc::now(), changes)?;
                if let Some(truncation) = truncation {
                    return Some(Self::apply_truncation(time, *truncation));
                }
                return Some(time)
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DateUnit {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millis
}

impl std::fmt::Display for DateUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DateUnit::Year => "y",
            DateUnit::Month => "M",
            DateUnit::Week => "w",
            DateUnit::Day => "d",
            DateUnit::Hour => "h",
            DateUnit::Minute => "m",
            DateUnit::Second => "s",
            DateUnit::Millis => "milli",
        })
    }
}

fn submission_fields() -> &'static Vec<struct_metadata::Entry<ElasticMeta>> {
    static CACHE: OnceLock<Vec<struct_metadata::Entry<ElasticMeta>>> = OnceLock::new();

    CACHE.get_or_init(|| {
        let data = assemblyline_models::datastore::Submission::metadata();

        let struct_metadata::Kind::Struct { children, .. } = data.kind else { panic!() };

        return children
    })
}

fn check_field_type(_root: &str, tail: &[String], kind: &struct_metadata::Kind<ElasticMeta>) -> bool {
    match kind {
        // if its a struct search within it's children
        struct_metadata::Kind::Struct { children, .. } => {
            check_field(tail, children)
        }

        // if its a mapping consume one name token for the address into the mapping
        struct_metadata::Kind::Mapping(_, inner) => {
            match tail.len() {
                0 => false,
                1 => true,
                _ => check_field_type(&tail[1], &tail[2..], &inner.kind)
            }            
        },

        // Recurse into the inner type for these
        struct_metadata::Kind::Sequence(kind) |
        struct_metadata::Kind::Option(kind) |
        struct_metadata::Kind::Aliased { kind, .. } => {
            check_field_type(_root, tail, &kind.kind)
        }

        // all remaining types are scalar, accept the field so long as its not trying 
        // to select a subfield of a scalar
        _ => {
            tail.is_empty()
        }
    }
}

fn check_field(field: &[String], options: &[struct_metadata::Entry<ElasticMeta>]) -> bool {
    let root = match field.first() {
        Some(name) => name,
        None => return false,
    };

    for child in options {
        if child.label != root { continue }
        return check_field_type(root, &field[1..], &child.type_info.kind);
    }

    return false
}


pub fn parse(query: &str) -> Result<Query, ParsingError> {
    // jsut make sure we can parse the query at all
    let (remain, query) = match super::parsing::expression(query) {
        Ok(row) => row,
        Err(err) => return Err(ParsingError::CouldNotParseSubmissionFilter(err.to_string()))
    };
    if !remain.is_empty() {
        return Err(ParsingError::CouldNotParseSubmissionFilterTrailing(remain.to_string()))
    }

    // compare the query against our field maps to see if its valid
    let mut extra_fields = vec![];
    let submission_fields = submission_fields();
    'fields: for field in query.list_fields() {
        // check if its in a special field, TODO replace this with actual tag layout when available
        if let Some(root) = field.first() {
            if root == "tags" {
                continue 'fields
            }
        }

        // check against submission structure
        if check_field(&field, submission_fields) {
            continue 'fields
        }

        // we hit a field that definately doesn't seem to exist, record it for the error message
        extra_fields.push(field.join("."));
    }
    if !extra_fields.is_empty() {
        return Err(ParsingError::SubmissionFilterUsesUnknownFields(extra_fields))
    }
    return Ok(query)
}


