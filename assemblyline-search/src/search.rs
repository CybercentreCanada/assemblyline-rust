use std::any::Any;
use std::fmt::Write;

use anyhow::{Result, bail};
use assemblyline_markings::classification::AccessControlValues;
use itertools::Itertools;
use log::warn;
use nom::AsChar;
use postgres_types::ToSql;
use crate::lucene::{self, FieldQuery, Location, PrefixOperator};
use crate::tables::{ANALYSIS_METADATA_TABLE, ANALYSIS_RESULTS_TABLE, ANALYSIS_SUBMISSIONS_TABLE, Field, Table, init_metadata_table, init_result_table, init_submission_table};
use crate::yugabyte::{self, ParameterValue, Parameters, SelectCommand};



/// A query on an sql table including constraints including across inner joins
#[derive(Debug, Clone)]
pub struct SqlQuery {
    table: String,
    constraints: Constraints,
}

/// A constrained join on an sql query
#[derive(Debug, Clone)]
pub struct Join {
    table: String,
    access_control: bool,
    on: (String, String),
    constraints: Box<Constraints>,
}

///
#[derive(Debug, Clone)]
enum Constraints {
    And(Vec<Constraints>),
    Or(Vec<Constraints>),
    Field {
        name: String,
        operation: Op,
        value: ParameterValue
    },
    Join(Join),
    Not(Box<Constraints>),
}

impl Constraints {
    fn build(self, context: &str, tables: &mut Vec<(String, String, bool)>, params: &mut Parameters) -> (Vec<String>, Option<String>) {
        match self {
            Constraints::And(items) => {
                let mut joins = vec![];
                let mut where_components = vec![];
                for item in items {
                    let (j, w) = item.build(context, tables, params);
                    joins.extend(j);
                    if let Some(w) = w {
                        where_components.push(w);
                    }
                }
                (joins, Some(format!("({})", where_components.join(" AND "))))
            }
            Constraints::Or(items) => {
                let mut joins = vec![];
                let mut where_components = vec![];
                for item in items {
                    let (j, w) = item.build(context, tables, params);
                    joins.extend(j);
                    if let Some(w) = w {
                        where_components.push(w);
                    }
                }
                (joins, Some(format!("({})", where_components.join(" OR "))))
            },
            Constraints::Field { name, operation, value } => {
                let label = params.push(value);
                if matches!(operation, Op::NotEqual) {
                    (vec![], Some(format!("({context}.{name} {operation} {label} OR {context}.{name} IS NULL)")))
                } else {
                    (vec![], Some(format!("({context}.{name} {operation} {label})")))
                }
            },
            Constraints::Join(join) => {
                let label = to_alpha_code(tables.len());
                tables.push((label.clone(), join.table.clone(), join.access_control));
                // if join.constraints.negative_test() {
                //     todo!()
                // } else {
                let join_string = format!("LEFT JOIN {} as {label} ON {context}.{} = {label}.{}", join.table, join.on.0, join.on.1);
                let (mut joins, where_terms) = join.constraints.build(&label, tables, params);
                joins.push(join_string);
                (joins, where_terms)
            },
            Constraints::Not(constraints) => {
                constraints.invert().build(context, tables, params)
            },
        }
    }

    fn invert(self) -> Self {
        match self {
            Constraints::And(items) => {
                Constraints::Or(items.into_iter().map(Self::invert).collect())
            },
            Constraints::Or(items) => {
                Constraints::And(items.into_iter().map(Self::invert).collect())
            },
            Constraints::Field { name, operation, value } => {
                Constraints::Field { name, operation: operation.invert(), value }
            },
            Constraints::Join(mut join) => {
                join.constraints = Box::new(join.constraints.invert());
                Constraints::Join(join)
            },
            Constraints::Not(constraints) => constraints.invert(),
        }
    }
}

static ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

fn to_alpha_code(index: usize) -> String {
    let upper = index / ALPHABET.len();
    let lower = index % ALPHABET.len();
    if upper > 0 {
        to_alpha_code(upper - 1) + &ALPHABET[lower].as_char().to_string()
    } else {
        ALPHABET[lower].as_char().to_string()
    }
}

#[test]
fn test_alpha_code() {
    assert_eq!(to_alpha_code(0), "a");
    assert_eq!(to_alpha_code(25), "z");
    assert_eq!(to_alpha_code(26), "aa");
    assert_eq!(to_alpha_code(51), "az");

    let mut values = std::collections::HashSet::new();
    for index in 0..500 {
        values.insert(to_alpha_code(index));
    }
    assert_eq!(values.len(), 500);
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Equal,
    NotEqual,
    Like,
    NotLike,
    SimilarTo,
    NotSimilarTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl Op {
    fn invert(self) -> Self {
        match self {
            Op::Equal => Op::NotEqual,
            Op::NotEqual => Op::Equal,
            Op::Like => Op::NotLike,
            Op::NotLike => Op::Like,
            Op::SimilarTo => Op::NotSimilarTo,
            Op::NotSimilarTo => Op::SimilarTo,
            Op::GreaterThan => Op::LessThanOrEqual,
            Op::GreaterThanOrEqual => Op::LessThan,
            Op::LessThan => Op::GreaterThanOrEqual,
            Op::LessThanOrEqual => Op::GreaterThan,
        }
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Equal => f.write_str("="),
            Op::NotEqual => f.write_str("<>"),
            Op::GreaterThan => f.write_str(">"),
            Op::GreaterThanOrEqual => f.write_str(">="),
            Op::LessThan => f.write_str("<"),
            Op::LessThanOrEqual => f.write_str("<="),
            Op::Like => f.write_str("LIKE"),
            Op::NotLike => f.write_str("NOT LIKE"),
            Op::SimilarTo => f.write_str("SIMILAR TO"),
            Op::NotSimilarTo => f.write_str("NOT SIMILAR TO"),
        }
    }
}

impl SqlQuery {
    fn parse(table: &Table, ast: lucene::Query) -> Result<Self> {
        Ok(Self {
            table: table.name.to_owned(),
            constraints: Self::parse_contstraint(table, ast)?,
        })
    }

    fn parse_contstraint(table: &Table, ast: lucene::Query) -> Result<Constraints> {
        match ast {
            lucene::Query::And(items, _location) => {
                let mut out = vec![];
                for item in items {
                    out.push(Self::parse_contstraint(table, item)?);
                }
                Ok(Constraints::And(out))
            },
            lucene::Query::Or(items, _location) => {
                let mut out = vec![];
                for item in items {
                    out.push(Self::parse_contstraint(table, item)?);
                }
                Ok(Constraints::Or(out))
            },
            lucene::Query::Not(query, _location) => {
                Ok(Constraints::Not(Box::new(Self::parse_contstraint(table, *query)?)))
            },
            lucene::Query::MatchAny(string_query, location) => todo!(),
            lucene::Query::RegexAny(regex, location) => todo!(),
            lucene::Query::WildcardAny(wildcard, location) => todo!(),
            lucene::Query::MatchField(items, field_query, location) => {
                limit_term(table, &items, field_query)
            },
            lucene::Query::FieldExists(items, location) => todo!(),
        }
    }

    pub fn apply_classification(self, access: AccessControlValues) -> SelectCommand {
        let mut tables = vec![("a".to_string(), self.table.clone(), true)];
        let mut parameters = Parameters::default();
        let level_label = parameters.push(access.level.into());
        let req_label = parameters.push(access.required.into());
        let g1_label = parameters.push(access.groups1.into());
        let g2_label = parameters.push(access.groups2.into());

        let (join_clauses, where_clause) = self.constraints.build("a", &mut tables, &mut parameters);

        let mut where_clauses = vec![];
        if let Some(w) = where_clause {
            where_clauses.push(w)
        }

        let mut joins = String::new();
        for component in join_clauses {
            joins = joins + &component + " ";
        }

        for (label, table, access_control) in tables {
            if access_control {
                where_clauses.push(format!("({label}.__access_lvl__ <= {level_label} AND {label}.__access_req__ <@ {req_label} AND {label}.__access_grp1__ && {g1_label} AND {label}.__access_grp2__ && {g2_label})"))
            }
        }

        let statement = format!("SELECT a.raw FROM {} as a {joins}WHERE {}", self.table, where_clauses.join(" AND "));

        SelectCommand {
            statement,
            parameters
        }
    }

    pub fn system_level_query(self) -> SelectCommand {
        let mut tables = vec![("a".to_string(), self.table.clone(), false)];
        let mut parameters = Parameters::default();

        let (join_clauses, where_clause) = self.constraints.build("a", &mut tables, &mut parameters);

        let mut where_clauses = vec![];
        if let Some(w) = where_clause {
            where_clauses.push(w)
        }

        let mut joins = String::new();
        for component in join_clauses {
            joins = joins + &component + " ";
        }

        let statement = format!("SELECT a.raw FROM {} as a {joins}WHERE {}", self.table, where_clauses.join(" AND "));

        SelectCommand {
            statement,
            parameters
        }
    }

}

fn limit_term(table: &Table, name: &[String], query: FieldQuery) -> Result<Constraints> {
    match table.name {
        ANALYSIS_SUBMISSIONS_TABLE => {
            // handle static fields
            let joined_name = name.join("_");
            if let Some(field) = table.get_field(&joined_name) {
                return Ok(field.query(query)?)
            }

            let root = match name.first() {
                Some(value) => value,
                None => bail!("Anonymous fields not permitted"),
            };

            // Add a pivot into metadata
            if root == "metadata" {
                return Ok(Constraints::Join(Join {
                    table: ANALYSIS_METADATA_TABLE.to_owned(),
                    access_control: false,
                    on: ("sid".to_string(), "sid".to_string()),
                    constraints: Box::new(Constraints::And(vec![
                        Constraints::Field {
                            name: "key".to_string(),
                            operation: Op::Equal,
                            value: ParameterValue::String(name[1..].join("_"))
                        },
                        limit_term(&init_metadata_table(), &["value".to_string()], query)?
                    ])),
                }))
            }

            if root == "results" {
                return Ok(Constraints::Join(Join {
                    table: ANALYSIS_RESULTS_TABLE.to_owned(),
                    access_control: true,
                    on: ("sid".to_string(), "sid".to_string()),
                    constraints: Box::new(limit_term(&init_result_table(), &name[1..], query)?),
                }))
            }

            todo!("virtual field reference: {name:?}");
        },
        ANALYSIS_METADATA_TABLE => {
            let joined_name = name.join("_");
            if let Some(field) = table.get_field(&joined_name) {
                return Ok(field.query(query)?)
            }

            todo!("virtual field reference: {name:?}");
        }
        ANALYSIS_RESULTS_TABLE => {
            let joined_name = name.join("_");
            if let Some(field) = table.get_field(&joined_name) {
                return Ok(field.query(query)?)
            }

            if let FieldQuery::Nested(query, _) = query {
                return SqlQuery::parse_contstraint(table, *query)
            }

            todo!("virtual field reference: {name:?}");
        }
        _ => {
            bail!("Unknown table: {}", table.name)
        }
    }
}

// fn to_sql_regex(ast: regex_syntax::ast::Ast) -> String {
//     match ast {
//         regex_syntax::ast::Ast::Dot(span) => todo!(),
//         regex_syntax::ast::Ast::Repetition(repetition) => todo!(),
//         regex_syntax::ast::Ast::Group(group) => todo!(),
//         regex_syntax::ast::Ast::Alternation(alternation) => todo!(),
//         regex_syntax::ast::Ast::Concat(concat) => todo!(),
//         other => other.to_string()
//     }
// }

impl Field {
    fn query(&self, query: FieldQuery) -> Result<Constraints, QueryError> {
        match query {
            FieldQuery::Regex(regex, location) => match self.kind {
                crate::tables::PostgresTypes::Timestamp | crate::tables::PostgresTypes::Boolean |
                crate::tables::PostgresTypes::SmallInt | crate::tables::PostgresTypes::Int |
                crate::tables::PostgresTypes::BigInt | crate::tables::PostgresTypes::RandomUuid |
                crate::tables::PostgresTypes::Uuid |
                crate::tables::PostgresTypes::Float | crate::tables::PostgresTypes::Double |
                crate::tables::PostgresTypes::Enum(_) =>
                    Err(QueryError::incompatable_operation("regex", &self.kind.type_string(), location)),
                crate::tables::PostgresTypes::Char(_) |
                crate::tables::PostgresTypes::Text |
                crate::tables::PostgresTypes::TextArrayInvert |
                crate::tables::PostgresTypes::TextInvert |
                crate::tables::PostgresTypes::TextTrigram => {
                    // let ast = regex_syntax::ast::parse::Parser::new().parse(regex.as_str())?;
                    warn!("Using regex expression directly: {}", regex);
                    Ok(Constraints::Field { name: self.name.clone(), operation: Op::SimilarTo, value: regex.as_str().to_owned().into() })
                },
            },
            FieldQuery::Wildcard(query, location) => match self.kind {
                crate::tables::PostgresTypes::Timestamp | crate::tables::PostgresTypes::Boolean |
                crate::tables::PostgresTypes::SmallInt | crate::tables::PostgresTypes::Int |
                crate::tables::PostgresTypes::BigInt | crate::tables::PostgresTypes::RandomUuid |
                crate::tables::PostgresTypes::Uuid |
                crate::tables::PostgresTypes::Float | crate::tables::PostgresTypes::Double |
                crate::tables::PostgresTypes::Enum(_) =>
                    Err(QueryError::incompatable_operation("wildcard", &self.kind.type_string(), location)),
                crate::tables::PostgresTypes::Char(_) |
                crate::tables::PostgresTypes::Text |
                crate::tables::PostgresTypes::TextArrayInvert |
                crate::tables::PostgresTypes::TextInvert |
                crate::tables::PostgresTypes::TextTrigram => {
                    Ok(Constraints::Field { name: self.name.clone(), operation: Op::Like, value: query.to_sql().into() })
                }
            },
            FieldQuery::Number(number_query, location) => match self.kind {
                crate::tables::PostgresTypes::Char(_) |
                crate::tables::PostgresTypes::Enum(_) |
                crate::tables::PostgresTypes::Uuid |
                crate::tables::PostgresTypes::Text |
                crate::tables::PostgresTypes::TextArrayInvert |
                crate::tables::PostgresTypes::TextInvert |
                crate::tables::PostgresTypes::TextTrigram =>
                    Err(QueryError::incompatable_operation("match_number", &self.kind.type_string(), location)),
                crate::tables::PostgresTypes::Timestamp => {
                    let (op, value) = numeric_operator(number_query);
                    let value = match chrono::DateTime::from_timestamp_millis((value * 1_000.0) as i64) {
                        Some(value) => value,
                        None => return Err(QueryError::UnexpectedLiteral { expected: "timestamp", location })
                    };
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: value.into() })
                },
                crate::tables::PostgresTypes::Boolean => {
                    let (op, value) = numeric_operator(number_query);
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: (value != 0.0).into() })
                },
                crate::tables::PostgresTypes::SmallInt => {
                    let (op, value) = numeric_operator(number_query);
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: (value as i16).into() })
                },
                crate::tables::PostgresTypes::Int => {
                    let (op, value) = numeric_operator(number_query);
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: (value as i32).into() })
                },
                crate::tables::PostgresTypes::RandomUuid |
                crate::tables::PostgresTypes::BigInt => {
                    let (op, value) = numeric_operator(number_query);
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: (value as i64).into() })
                }
                crate::tables::PostgresTypes::Float => {
                    let (op, value) = numeric_operator(number_query);
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: (value as f32).into() })
                },
                crate::tables::PostgresTypes::Double => {
                    let (op, value) = numeric_operator(number_query);
                    Ok(Constraints::Field { name: self.name.clone(), operation: op, value: value.into() })
                },
            },
            FieldQuery::Match(string_query, location) => match self.kind {
                crate::tables::PostgresTypes::Timestamp => todo!(),
                crate::tables::PostgresTypes::Boolean => {
                    if string_query.operator.is_some() {
                        Err(QueryError::incompatable_operation("string_match_with_prefix", "bool", location))
                    } else {
                        match string_query.value.to_ascii_lowercase().trim() {
                            "true" => Ok(Constraints::Field { name: self.name.clone(), operation: Op::Equal, value: true.into() }),
                            "false" => Ok(Constraints::Field { name: self.name.clone(), operation: Op::Equal, value: false.into() }),
                            _ => Err(QueryError::UnexpectedLiteral { expected: "boolean", location })
                        }
                    }
                },
                crate::tables::PostgresTypes::SmallInt => todo!(),
                crate::tables::PostgresTypes::Int => todo!(),
                crate::tables::PostgresTypes::Uuid |
                crate::tables::PostgresTypes::BigInt => todo!(),
                crate::tables::PostgresTypes::Char(_) => todo!(),
                crate::tables::PostgresTypes::Enum(_) => todo!(),
                crate::tables::PostgresTypes::Text => {
                    let operation = string_operator(string_query.operator);
                    Ok(Constraints::Field { name: self.name.clone(), operation, value: string_query.value.into() })
                },
                crate::tables::PostgresTypes::TextArrayInvert => todo!(),
                crate::tables::PostgresTypes::TextInvert => todo!(),
                crate::tables::PostgresTypes::TextTrigram => {
                    let operation = string_operator(string_query.operator);
                    Ok(Constraints::Field { name: self.name.clone(), operation, value: string_query.value.into() })
                },
                crate::tables::PostgresTypes::RandomUuid => todo!(),
                crate::tables::PostgresTypes::Float => todo!(),
                crate::tables::PostgresTypes::Double => todo!(),
            },
            FieldQuery::Range(range_query, location) => todo!(),
            FieldQuery::Or(items, _location) => {
                Ok(Constraints::Or(items.into_iter().map(|item| self.query(item)).collect::<Result<_, _>>()?))
            },
            FieldQuery::And(items, _location) => {
                Ok(Constraints::And(items.into_iter().map(|item| self.query(item)).collect::<Result<_, _>>()?))
            }
            FieldQuery::Not(field_query, _location) => {
                Ok(Constraints::Not(Box::new(self.query(*field_query)?)))
            },
            FieldQuery::Nested(_query, location) => Err(QueryError::incompatable_operation("nested", &self.kind.type_string(), location)),
        }
    }
}


pub fn numeric_operator(mut query: lucene::NumberQuery) -> (Op, f64) {
    let op = match query.operator {
        None => Op::Equal,
        Some(PrefixOperator::Forbid) => {
            query.value = -1.0;
            Op::Equal
        },
        Some(PrefixOperator::Require) => Op::Equal,
        Some(PrefixOperator::GreaterThan) => Op::GreaterThan,
        Some(PrefixOperator::GreaterThanOrEqual) => Op::GreaterThanOrEqual,
        Some(PrefixOperator::LessThan) => Op::LessThan,
        Some(PrefixOperator::LessThanOrEqual) => Op::LessThanOrEqual,
    };
    (op, query.value)
}

pub fn string_operator(op: Option<lucene::PrefixOperator>) -> Op {
    let op = match op {
        Some(op) => op,
        None => return Op::Equal,
    };
    match op {
        PrefixOperator::Require => Op::Equal,
        PrefixOperator::Forbid => Op::NotEqual,
        PrefixOperator::GreaterThanOrEqual => Op::GreaterThanOrEqual,
        PrefixOperator::LessThanOrEqual => Op::LessThanOrEqual,
        PrefixOperator::GreaterThan => Op::GreaterThan,
        PrefixOperator::LessThan => Op::LessThan,
    }
}

#[derive(Debug, Clone, thiserror::Error)]
enum QueryError {
    #[error("{location}: {operation} operation cannot be applied to {field} field type.")]
    IncompatableOperation {
        operation: &'static str,
        field: String,
        location: Location,
    },
    #[error("{location}: A literal of type {expected} was expected at this location.")]
    UnexpectedLiteral {
        expected: &'static str,
        location: Location,
    }
}

impl QueryError {
    fn incompatable_operation(operation: &'static str, field: &str, location: Location) -> Self {
        Self::IncompatableOperation { operation, field: field.to_string(), location }
    }
}


#[test]
fn build_query() {
    let ce = assemblyline_markings::classification::ClassificationParser::new(assemblyline_markings::classification::sample_config()).unwrap();

    // simple single term search
    let query = lucene::Query::parse("params.deep_scan: true").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();

    let command = query.clone().apply_classification(ce.get_access_control_values("L1//LE/AC//REL A, B/R1", true).unwrap());
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a WHERE (a.params_deep_scan = $5) AND (a.__access_lvl__ <= $1 AND a.__access_req__ <@ $2 AND a.__access_grp1__ && $3 AND a.__access_grp2__ && $4)");
    assert_eq!(command.parameters.parameters.len(), 5);
    assert_eq!(command.parameters.parameters[0], 5i32.into());
    assert_eq!(command.parameters.parameters[1], vec!["AC".to_owned(), "LE".to_owned()].into());
    assert_eq!(command.parameters.parameters[2], vec!["A".to_owned(), "B".to_owned()].into());
    assert_eq!(command.parameters.parameters[3], vec!["R1".to_owned()].into());
    assert_eq!(command.parameters.parameters[4], true.into());

    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a WHERE (a.params_deep_scan = $1)");
    assert_eq!(command.parameters.parameters.len(), 1);
    assert_eq!(command.parameters.parameters[0], true.into());

    // simple multi term search
    let query = lucene::Query::parse("params.deep_scan: true AND params.submitter: Jimmy").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();
    let command = query.clone().apply_classification(ce.get_access_control_values("L1//LE/AC//REL A, B/R1", true).unwrap());
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a WHERE ((a.params_deep_scan = $5) AND (a.params_submitter = $6)) AND (a.__access_lvl__ <= $1 AND a.__access_req__ <@ $2 AND a.__access_grp1__ && $3 AND a.__access_grp2__ && $4)");
    assert_eq!(command.parameters.parameters.len(), 6);
    assert_eq!(command.parameters.parameters[0], 5i32.into());
    assert_eq!(command.parameters.parameters[1], vec!["AC".to_owned(), "LE".to_owned()].into());
    assert_eq!(command.parameters.parameters[2], vec!["A".to_owned(), "B".to_owned()].into());
    assert_eq!(command.parameters.parameters[3], vec!["R1".to_owned()].into());
    assert_eq!(command.parameters.parameters[4], true.into());
    assert_eq!(command.parameters.parameters[5], "Jimmy".to_string().into());

    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a WHERE ((a.params_deep_scan = $1) AND (a.params_submitter = $2))");
    assert_eq!(command.parameters.parameters.len(), 2);
    assert_eq!(command.parameters.parameters[0], true.into());
    assert_eq!(command.parameters.parameters[1], "Jimmy".to_string().into());

    // join onto the metadata table
    let query = lucene::Query::parse("params.deep_scan: true AND params.submitter: Jimmy AND metadata.animal: cat").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();

    let command = query.clone().apply_classification(ce.get_access_control_values("L1//LE/AC//REL A, B/R1", true).unwrap());
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_metadata as b ON a.sid = b.sid WHERE ((a.params_deep_scan = $5) AND (a.params_submitter = $6) AND ((b.key = $7) AND (b.value = $8))) AND (a.__access_lvl__ <= $1 AND a.__access_req__ <@ $2 AND a.__access_grp1__ && $3 AND a.__access_grp2__ && $4)");
    assert_eq!(command.parameters.parameters.len(), 8);
    assert_eq!(command.parameters.parameters[0], 5i32.into());
    assert_eq!(command.parameters.parameters[1], vec!["AC".to_owned(), "LE".to_owned()].into());
    assert_eq!(command.parameters.parameters[2], vec!["A".to_owned(), "B".to_owned()].into());
    assert_eq!(command.parameters.parameters[3], vec!["R1".to_owned()].into());
    assert_eq!(command.parameters.parameters[4], true.into());
    assert_eq!(command.parameters.parameters[5], "Jimmy".to_string().into());
    assert_eq!(command.parameters.parameters[6], "animal".to_string().into());
    assert_eq!(command.parameters.parameters[7], "cat".to_string().into());

    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_metadata as b ON a.sid = b.sid WHERE ((a.params_deep_scan = $1) AND (a.params_submitter = $2) AND ((b.key = $3) AND (b.value = $4)))");
    assert_eq!(command.parameters.parameters.len(), 4);
    assert_eq!(command.parameters.parameters[0], true.into());
    assert_eq!(command.parameters.parameters[1], "Jimmy".to_string().into());
    assert_eq!(command.parameters.parameters[2], "animal".to_string().into());
    assert_eq!(command.parameters.parameters[3], "cat".to_string().into());

    // join onto the results table twice
    let query = lucene::Query::parse("results.response.service_name: Extract AND results.result.score: >=1000").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();

    let command = query.clone().apply_classification(ce.get_access_control_values("L1//LE/AC//REL A, B/R1", true).unwrap());
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_results as b ON a.sid = b.sid LEFT JOIN analysis_results as c ON a.sid = c.sid WHERE ((b.response_service_name = $5) AND (c.result_score >= $6)) AND (a.__access_lvl__ <= $1 AND a.__access_req__ <@ $2 AND a.__access_grp1__ && $3 AND a.__access_grp2__ && $4) AND (b.__access_lvl__ <= $1 AND b.__access_req__ <@ $2 AND b.__access_grp1__ && $3 AND b.__access_grp2__ && $4) AND (c.__access_lvl__ <= $1 AND c.__access_req__ <@ $2 AND c.__access_grp1__ && $3 AND c.__access_grp2__ && $4)");
    assert_eq!(command.parameters.parameters.len(), 6);
    assert_eq!(command.parameters.parameters[0], 5i32.into());
    assert_eq!(command.parameters.parameters[1], vec!["AC".to_owned(), "LE".to_owned()].into());
    assert_eq!(command.parameters.parameters[2], vec!["A".to_owned(), "B".to_owned()].into());
    assert_eq!(command.parameters.parameters[3], vec!["R1".to_owned()].into());
    assert_eq!(command.parameters.parameters[4], "Extract".to_owned().into());
    assert_eq!(command.parameters.parameters[5], 1000i32.into());

    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_results as b ON a.sid = b.sid LEFT JOIN analysis_results as c ON a.sid = c.sid WHERE ((b.response_service_name = $1) AND (c.result_score >= $2))");
    assert_eq!(command.parameters.parameters.len(), 2);
    assert_eq!(command.parameters.parameters[0], "Extract".to_owned().into());
    assert_eq!(command.parameters.parameters[1], 1000i32.into());

    // join onto the results table once
    let query = lucene::Query::parse("results: {response.service_name: Extract AND result.score: >=1000}").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();

    let command = query.clone().apply_classification(ce.get_access_control_values("L1//LE/AC//REL A, B/R1", true).unwrap());
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_results as b ON a.sid = b.sid WHERE ((b.response_service_name = $5) AND (b.result_score >= $6)) AND (a.__access_lvl__ <= $1 AND a.__access_req__ <@ $2 AND a.__access_grp1__ && $3 AND a.__access_grp2__ && $4) AND (b.__access_lvl__ <= $1 AND b.__access_req__ <@ $2 AND b.__access_grp1__ && $3 AND b.__access_grp2__ && $4)");
    assert_eq!(command.parameters.parameters.len(), 6);
    assert_eq!(command.parameters.parameters[0], 5i32.into());
    assert_eq!(command.parameters.parameters[1], vec!["AC".to_owned(), "LE".to_owned()].into());
    assert_eq!(command.parameters.parameters[2], vec!["A".to_owned(), "B".to_owned()].into());
    assert_eq!(command.parameters.parameters[3], vec!["R1".to_owned()].into());
    assert_eq!(command.parameters.parameters[4], "Extract".to_owned().into());
    assert_eq!(command.parameters.parameters[5], 1000i32.into());

    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_results as b ON a.sid = b.sid WHERE ((b.response_service_name = $1) AND (b.result_score >= $2))");
    assert_eq!(command.parameters.parameters.len(), 2);
    assert_eq!(command.parameters.parameters[0], "Extract".to_owned().into());
    assert_eq!(command.parameters.parameters[1], 1000i32.into());

    // A not equals outside of a join
    let query = lucene::Query::parse("NOT params.submitter: Jimmy").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();
    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a WHERE (a.params_submitter <> $1 OR a.params_submitter IS NULL)");
    assert_eq!(command.parameters.parameters.len(), 1);
    assert_eq!(command.parameters.parameters[0], "Jimmy".to_owned().into());

    // a not equals inside of a join
    let query = lucene::Query::parse("NOT results.response.service_name: Extract").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();
    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_results as b ON a.sid = b.sid WHERE (b.response_service_name <> $1 OR b.response_service_name IS NULL)");
    assert_eq!(command.parameters.parameters.len(), 1);
    assert_eq!(command.parameters.parameters[0], "Extract".to_owned().into());

    // not over multiple fields
    let query = lucene::Query::parse("NOT (params.submitter: Jimmy OR params.submitter: Pong)").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();
    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a WHERE ((a.params_submitter <> $1 OR a.params_submitter IS NULL) AND (a.params_submitter <> $2 OR a.params_submitter IS NULL))");
    assert_eq!(command.parameters.parameters.len(), 2);
    assert_eq!(command.parameters.parameters[0], "Jimmy".to_owned().into());
    assert_eq!(command.parameters.parameters[1], "Pong".to_owned().into());

    // not over multiple fields and a join
    let query = lucene::Query::parse("NOT (params.submitter: Jimmy OR results.response.service_name: Extract)").unwrap();
    let query = SqlQuery::parse(&init_submission_table(), query).unwrap();
    let command = query.system_level_query();
    assert_eq!(command.statement, "SELECT a.raw FROM analysis_submissions as a LEFT JOIN analysis_results as b ON a.sid = b.sid WHERE ((a.params_submitter <> $1 OR a.params_submitter IS NULL) AND (b.response_service_name <> $2 OR b.response_service_name IS NULL))");
    assert_eq!(command.parameters.parameters, vec![
        "Jimmy".to_owned().into(),
        "Extract".to_owned().into(),
    ]);

}


// #[test]
// fn where_not_exists() {
//     todo!("Build a single term query where the join gets converted to a not exists in clause");
//     todo!("Build a multi term query where the join gets converted to a not exists in clause");
//     todo!("Build a multi term query where the join gets converted to a not exists in clause in a context that requires inversion");
// }

// #[tokio::test]
// async fn missing_metadata() {
//     let db = yugabyte::Yugabyte::development(true).await.unwrap();

//     db.client.execute("create table submission(sid serial primary key, submitter text)", &[]).await.unwrap();
//     db.client.execute("create table metadata(sid int, key text, value text, PRIMARY KEY(sid, key));", &[]).await.unwrap();

//     let sid1: i32 = db.client.query_one("INSERT INTO submission (submitter) VALUES ('cat') RETURNING sid", &[]).await.unwrap().get(0);
//     let sid2: i32 = db.client.query_one("INSERT INTO submission (submitter) VALUES ('dog') RETURNING sid", &[]).await.unwrap().get(0);

//     db.client.execute("INSERT INTO metadata (sid, key, value) VALUES ($1, 'colour', 'red')", &[&sid1]).await.unwrap();

//     let rows = db.client.query("select s.sid from submission as s left join metadata as m on s.sid = m.sid", &[]).await.unwrap();
//     println!("left join no where");
//     for row in rows {
//         let id: i32 = row.get(0);
//         println!("{id}");
//     }

//     let rows = db.client.query("select s.sid from submission as s left join metadata as m on s.sid = m.sid where m.key = $1", &[&"colour"]).await.unwrap();
//     println!("left join where");
//     for row in rows {
//         let id: i32 = row.get(0);
//         println!("{id}");
//     }

//     let rows = db.client.query("select s.sid from submission as s left join metadata as m on s.sid = m.sid where (m.key <> $1 OR m.key is null)", &[&"colour"]).await.unwrap();
//     println!("left join where");
//     for row in rows {
//         let id: i32 = row.get(0);
//         println!("{id}");
//     }

//     let rows = db.client.query("select s.sid from submission as s left join metadata as m on s.sid = m.sid where (m.key <> $1 OR m.key is null)", &[&"round"]).await.unwrap();
//     println!("left join where");
//     for row in rows {
//         let id: i32 = row.get(0);
//         println!("{id}");
//     }

//     todo!()
// }
