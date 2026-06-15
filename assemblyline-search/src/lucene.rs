


use std::fmt::Display;
use std::ops::Sub;
use std::str::FromStr;

use anyhow::Result;
use chrono::{DateTime, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, Utc, Datelike, Timelike, SubsecRound};
use nom::{IResult, Offset, Parser};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, escaped_transform, is_not, take_while1, tag_no_case, is_a};
use nom::character::complete::{alphanumeric1, multispace0, multispace1, one_of};
use nom::combinator::{eof, map, map_opt, map_res, opt, peek, value};
use nom::error::ParseError;
use nom::multi::{count, many_till, many1, separated_list1};
use nom::number::complete::double;
use nom::sequence::{delimited, pair, terminated};
use nom_locate::LocatedSpan;

// MARK: AST


/// Root of the AST for lucene queries we have parsed.
/// The parsing done in this module is just for syntax and doesn't validate against
/// any particular set of target fields.
#[derive(Debug, Clone)]
pub enum Query {
    And(Vec<Query>, Location),
    Or(Vec<Query>, Location),
    Not(Box<Query>, Location),
    MatchAny(StringQuery, Location),
    RegexAny(regex::Regex, Location),
    WildcardAny(WildcardQuery, Location),
    MatchField(Vec<String>, FieldQuery, Location),
    FieldExists(Vec<String>, Location),
}

impl Query {
    pub fn location(&self) -> Location {
        match self {
            Query::And(_, location) => location.clone(),
            Query::Or(_, location) => location.clone(),
            Query::Not(_, location) => location.clone(),
            Query::MatchAny(_, location) => location.clone(),
            Query::RegexAny(_, location) => location.clone(),
            Query::WildcardAny(_, location) => location.clone(),
            Query::MatchField(_, _, location) => location.clone(),
            Query::FieldExists(_, location) => location.clone(),
        }
    }

    pub fn parse(query: &str) -> Result<Query, ParsingError> {
        // jsut make sure we can parse the query at all
        let span = Span::new(query);
        let (remain, query) = match expression(span) {
            Ok(row) => row,
            Err(err) => return Err(ParsingError::CouldNotParseSubmissionFilter(err.to_string()))
        };
        if !remain.is_empty() {
            return Err(ParsingError::CouldNotParseSubmissionFilterTrailing(remain.to_string()))
        }
        Ok(query)
    }

    pub fn list_fields(&self) -> Vec<Vec<String>> {
        let mut fields = vec![];
        match self {
            Query::And(parts, _) => for part in parts {
                fields.extend(part.list_fields().into_iter());
            },
            Query::Or(parts, _) => for part in parts {
                fields.extend(part.list_fields().into_iter());
            },
            Query::Not(part, _) => fields.extend(part.list_fields()),
            Query::MatchAny(..) | Query::RegexAny(..) | Query::WildcardAny(..) => fields.push(vec![]),
            Query::FieldExists(field, ..) => {
                fields.push(field.clone());
            },
            Query::MatchField(field, query, ..) => {
                if let FieldQuery::Nested(query, ..) = query {
                    for part in query.list_fields() {
                        let mut field = field.clone();
                        field.extend(part);
                        fields.push(field);
                    }
                } else {
                    fields.push(field.clone());
                }
            },
        }
        fields.sort_unstable();
        fields.dedup();
        return fields;
    }
}

#[derive(Debug, Clone)]
pub enum FieldQuery {
    Regex(regex::Regex, Location),
    Number(NumberQuery, Location),
    Match(StringQuery, Location),
    Wildcard(WildcardQuery, Location),
    Range(RangeQuery, Location),
    Or(Vec<FieldQuery>, Location),
    And(Vec<FieldQuery>, Location),
    Not(Box<FieldQuery>, Location),
    Nested(Box<Query>, Location),
}

#[derive(Debug, Clone)]
pub struct StringQuery {
    pub operator: Option<PrefixOperator>,
    pub value: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct NumberQuery {
    pub operator: Option<PrefixOperator>,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct WildcardQuery {
    // pub operator: Option<PrefixOperator>,
    pub query: Vec<WildcardToken>,
}

impl WildcardQuery {
    pub fn to_regex(&self) -> Result<regex::Regex, regex::Error> {
        let mut buffer = String::new();
        for token in &self.query {
            match token {
                WildcardToken::Single => buffer += ".",
                WildcardToken::Multiple => buffer += ".*",
                WildcardToken::Literal(value) => buffer += &regex::escape(value),
            }
        }
        regex::Regex::new(&buffer)
    }

    pub fn to_sql(&self) -> String {
        let mut buffer = String::new();
        for token in &self.query {
            match token {
                WildcardToken::Single => buffer += "_",
                WildcardToken::Multiple => buffer += "%",
                WildcardToken::Literal(chars) => {
                    for char in chars.chars() {
                        match char {
                            '_' => {
                                buffer.push('\\');
                                buffer.push('_');
                            }
                            '%' => {
                                buffer.push('\\');
                                buffer.push('%');

                            }
                            '\\' => {
                                buffer.push('\\');
                                buffer.push('\\');
                            }
                            other => {
                                buffer.push(other);
                            }
                        }
                    }
                },
            }
        }
        buffer
    }
}

#[derive(Debug, Clone)]
pub enum WildcardToken {
    Single,
    Multiple,
    Literal(String)
}


#[derive(Debug, Clone, Copy)]
pub enum RangeBound {
    Inclusive,
    Exclusive
}

#[derive(Debug, Clone)]
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

// MARK: Error

/// Errors related to parsing or interpreting a post processing rule
#[derive(Debug, PartialEq, Eq)]
pub enum ParsingError {
    UnknownPrefixOperator(String),
    InvalidDate(String),
    InvalidTime(String),

    CouldNotParseSubmissionFilter(String),
    CouldNotParseSubmissionFilterTrailing(String),
    // SubmissionFilterUsesUnknownFields(Vec<String>),
    // AlwaysTrue(String),
}


impl ParsingError {
    pub (crate) fn invalid_date<D: Display>(input: D) -> Self {
        Self::InvalidDate(input.to_string())
    }
}

impl std::fmt::Display for ParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsingError::UnknownPrefixOperator(value) => write!(f, "An unknown prefix operator was used: {value}"),
            ParsingError::InvalidDate(value) => write!(f, "An invalid date string was provided: {value}"),
            ParsingError::InvalidTime(value) => write!(f, "An invalid time string was provided: {value}"),
            ParsingError::CouldNotParseSubmissionFilter(value) => write!(f, "The submission filter could not be parsed: {value}"),
            ParsingError::CouldNotParseSubmissionFilterTrailing(value) => write!(f, "There was trailing data after the filter was processed: {value}"),
            // ParsingError::SubmissionFilterUsesUnknownFields(value) => {
            //     let fields = value.join(", ");
            //     write!(f, "Unknown fields were used in the submission filter: {fields}")
            // },
        }
    }
}

impl std::error::Error for ParsingError {}


// MARK: Parser

type Span<'a> = LocatedSpan<&'a str>;
fn expression(input: Span) -> IResult<Span, Query> {
    // println!("expression: {input}");
    // terminated(or_expr, eof).parse(input)
    or_expr(input)
}

// fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
// where
// F: FnMut(&'a str) -> IResult<&'a str, O, E>,
// {
//     delimited(multispace0, inner, multispace0)
// }
fn ws<'a, O, E: ParseError<Span<'a>>, F>(
    inner: F,
) -> impl Parser<Span<'a>, Output = O, Error = E>
where
    F: Parser<Span<'a>, Output = O, Error = E>,
{
    delimited(multispace0, inner, multispace0)
}

fn or_expr(input: Span) -> IResult<Span, Query> {
    let (remain, mut sub_queries) = separated_list1(or_operator, and_expr).parse(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, Query::Or(sub_queries, Location::used(input, remain))))
    }
}
fn or_operator(input: Span) -> IResult<Span, ()> {
    let (remain, _) = ws(alt((tag("OR"), tag("||")))).parse(input)?;
    return Ok((remain, ()))
}

// and_expr: not_expr ("AND" not_expr)*
fn and_expr(input: Span) -> IResult<Span, Query> {
    let (remain, mut sub_queries) = separated_list1(and_operator, not_expr).parse(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, Query::And(sub_queries, Location::used(input, remain))))
    }
}
fn and_operator(input: Span) -> IResult<Span, ()> {
    let (remain, _) = ws(alt((tag("AND"), tag("&&")))).parse(input)?;
    return Ok((remain, ()))
}


// not_expr: NOT_OPERATOR? atom
// NOT_OPERATOR: "NOT"
fn not_expr(input: Span) -> IResult<Span, Query> {
    let (remain, (not_operator, sub_query)) = (opt(not_operator), atom).parse(input)?;
    if not_operator.is_some() {
        Ok((remain, Query::Not(Box::new(sub_query), Location::used(input, remain))))
    } else {
        Ok((remain, sub_query))
    }
}
fn not_operator(input: Span) -> IResult<Span, ()> {
    let (remain, _) = ws(alt((tag("NOT"), tag("!")))).parse(input)?;
    return Ok((remain, ()))
}

// atom: "(" expression ")"
//     | exists
//     | field
//     | term
fn atom(input: Span) -> IResult<Span, Query> {
    // println!("atom: {input}");
    alt((
        delimited(ws(tag("(")), expression, ws(tag(")"))),
        exists,
        field,
        term
    )).parse(input)
}


// term: PREFIX_OPERATOR? (phrase_term | SIMPLE_TERM)
fn term(input: Span) -> IResult<Span, Query> {
    // println!("term: {input}");
    alt((
        map(string_query, |(q, l)|Query::MatchAny(q, l)),
        map(pattern_term, |(q, l)| Query::WildcardAny(WildcardQuery { query: q }, l)),
    )).parse(input)
}

fn number_term(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, (operator, value)) = (opt(ws(prefix_operator)), double).parse(input)?;
    Ok((remain, FieldQuery::Number(NumberQuery { operator, value }, Location::used(input, remain))))
}

// field_term: PREFIX_OPERATOR? (phrase_term | SIMPLE_TERM)
fn field_term(input: Span) -> IResult<Span, FieldQuery> {
    // println!("field_term: {input}");
    alt((
        map(string_query, |(q, l)|FieldQuery::Match(q, l)),
        map(pattern_term, |(q, l)|FieldQuery::Wildcard(WildcardQuery { query: q }, l)),
    )).parse(input)
}
fn string_query(input: Span) -> IResult<Span, (StringQuery, Location)> {
    // println!("string_query: {input}");
    let (remain, (operator, value)) = (opt(ws(prefix_operator)), alt((phrase_term, simple_term))).parse(input)?;
    Ok((remain, (StringQuery { operator, value }, Location::used(input, remain))))
}

// PREFIX_OPERATOR: "-" | "+" | ">=" | "<=" | ">" | "<"
fn prefix_operator(input: Span) -> IResult<Span, PrefixOperator> {
    map_res(alt((tag("-"), tag("+"), tag(">="), tag("<="), tag(">"), tag("<"))), |r: Span| PrefixOperator::from_str(*r)).parse(input)
}

// SIMPLE_TERM: ("\\+" | "\\-" | "\\&&" | "\\&" | "\\||" | "\\|" | "\\!" | "\\(" | "\\)" | "\\{"
//              | "\\}" | "\\[" | "\\]" | "\\^" | "\\\"" | "\\~" | "\\*" | "\\ "
//              | "\\?" | "\\:" | "\\\\" | "*" | "?" | "_" | "-" | DIGIT | LETTER)+
// (escaped (multi character | single character)) | special chars | alphanum
fn is_special(value: char) -> bool {
    matches!(value, '_' | '-' | '.')
}
fn primitive_simple_term(input: Span) -> IResult<Span, String> {
    // println!("simple_term: {input}");
    map_res(escaped_transform(
        alt((alphanumeric1, take_while1(is_special))),
        '\\',
        alt((
            value("&&", tag("&&")),
            value("||", tag("||")),
            value("+", tag("+")),
            value("-", tag("-")),
            value("&", tag("&")),
            value("|", tag("|")),
            value("!", tag("!")),
            value("(", tag("(")),
            value(")", tag(")")),
            value("{", tag("{")),
            value("}", tag("}")),
            value("[", tag("[")),
            value("]", tag("]")),
            value("^", tag("^")),
            value("\"", tag("\"")),
            value("~", tag("~")),
            value("*", tag("*")),
            value(" ", tag(" ")),
            value("?", tag("?")),
            value(":", tag(":")),
            value("\\", tag("\\")),
        ))
    ),|res|{
        if res.is_empty() {
            Err(())
        } else {
            Ok(res)
        }
    }).parse(input)
}

fn simple_term(input: Span) -> IResult<Span, String> {
    // println!("simple_term: {input}");
    terminated(primitive_simple_term, end_term).parse(input)
}

fn pattern_term(input: Span) -> IResult<Span, (Vec<WildcardToken>, Location)> {
    let (remain, out) = map_res(many_till(
        alt((
            map(tag("*"), |_|{WildcardToken::Multiple}),
            map(tag("?"), |_|{WildcardToken::Single}),
            map(primitive_simple_term, |row|{WildcardToken::Literal(row)}),
        )),
        end_term
    ), |(parts, _)|{
        anyhow::Ok(parts)
    }).parse(input)?;
    Ok((remain, (out, Location::used(input, remain))))
}

fn end_term(input: Span) -> IResult<Span, ()> {
    alt((
        map(peek(tag(")")), |_| ()),
        map(peek(tag("}")), |_| ()),
        map(peek(eof), |_|()),
        map(multispace1, |_| ()),
    )).parse(input)
}

// phrase_term: ESCAPED_STRING
fn phrase_term(input: Span) -> IResult<Span, String> {
    quoted_string(input)
}

// field: FIELD_LABEL ":" field_value
fn field(input: Span) -> IResult<Span, Query> {
    // println!("field: {input}");
    let (remain, (label, _, query)) = (field_label, ws(tag(":")), field_value).parse(input)?;
    Ok((remain, Query::MatchField(label, query, Location::used(input, remain))))
}

// exists: "_exists_" ":" FIELD_LABEL
fn exists(input: Span) -> IResult<Span, Query> {
    let (remain, (_, _, label)) = (ws(tag_no_case("_exists_")), ws(tag(":")), field_label).parse(input)?;
    Ok((remain, Query::FieldExists(label, Location::used(input, remain))))
}

// FIELD_LABEL: CNAME ["." CNAME]*
fn field_label(input: Span) -> IResult<Span, Vec<String>> {
    separated_list1(tag("."), cname).parse(input)
}

fn cname(input: Span) -> IResult<Span, String> {
    // println!("cname: {input}");
    let (remain, (a, b)) = (take_while1(|item: char| item.is_alphabetic() || item == '_'), take_while(|item: char| item.is_alphanumeric() || item == '_')).parse(input)?;
    // println!("cname X: {a} {b}");
    Ok((remain, a.to_string() + *b))
}

// field_value: range
//            | field_term
//            | REGEX_TERM
//            | "(" field_expression ")"
fn field_value(input: Span) -> IResult<Span, FieldQuery> {
    // println!("field_value: {input}");
    alt((
        range,
        delimited(ws(tag("(")), field_expression, ws(tag(")"))),
        regex_term,
        number_term,
        field_term,
        nested,
    )).parse(input)
}

fn nested(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, expression) = nested_inner(input)?;
    Ok((remain, FieldQuery::Nested(Box::new(expression), Location::used(input, remain))))
}

fn nested_inner(input: Span) -> IResult<Span, Query> {
    delimited(ws(tag("{")), expression, ws(tag("}"))).parse(input)
}

// REGEX_TERM: /\/([^\/]|(\\\/))*\//
fn regex_term(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, regex) = map_res(delimited(tag("/"), escaped_transform(
        is_not("/"),
        '\\',
        alt((
            value("/", tag("/")),
        ))
    ), tag("/")), |pattern| regex::Regex::new(&pattern)).parse(input)?;
    Ok((remain, FieldQuery::Regex(regex, Location::used(input, remain))))
}

// range: RANGE_START first_range_term "TO" second_range_term RANGE_END
// RANGE_START: "[" | "{"
// RANGE_END: "]" | "}"
fn range(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, (start_bound, start, _, end, end_bound)) = (range_start, first_range_term, ws(tag("TO")), second_range_term, range_end).parse(input)?;
    Ok((remain, FieldQuery::Range(RangeQuery{
        start,
        end,
        start_bound,
        end_bound,
    }, Location::used(input, remain))))
}
fn range_start(input: Span) -> IResult<Span, RangeBound> {
    ws(alt((value(RangeBound::Exclusive, tag("{")), value(RangeBound::Inclusive, tag("["))))).parse(input)
}
fn range_end(input: Span) -> IResult<Span, RangeBound> {
    ws(alt((value(RangeBound::Exclusive, tag("}")), value(RangeBound::Inclusive, tag("]"))))).parse(input)
}

// field_expression: field_or_expr
fn field_expression(input: Span) -> IResult<Span, FieldQuery> {
    field_or_expr(input)
}
// field_or_expr: field_and_expr ("OR" field_and_expr)*
fn field_or_expr(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, mut sub_queries) = separated_list1(or_operator, field_and_expr).parse(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, FieldQuery::Or(sub_queries, Location::used(input, remain))))
    }
}
// field_and_expr: field_not_expr ("AND" field_not_expr)*
fn field_and_expr(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, mut sub_queries) = separated_list1(and_operator, field_not_expr).parse(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, FieldQuery::And(sub_queries, Location::used(input, remain))))
    }
}
// field_not_expr: NOT_OPERATOR? field_atom
fn field_not_expr(input: Span) -> IResult<Span, FieldQuery> {
    let (remain, (not_operator, sub_query)) = (opt(not_operator), field_atom).parse(input)?;
    if not_operator.is_some() {
        Ok((remain, FieldQuery::Not(Box::new(sub_query), Location::used(input, remain))))
    } else {
        Ok((remain, sub_query))
    }
}
// field_atom: field_term
//           | "(" field_expression ")"
fn field_atom(input: Span) -> IResult<Span, FieldQuery> {
    alt((delimited(ws(tag("(")), field_expression, ws(tag(")"))), field_term)).parse(input)
}

// first_range_term: RANGE_WILD | DATE_EXPRESSION | QUOTED_RANGE | FIRST_RANGE
fn first_range_term(input: Span) -> IResult<Span, RangeTerm> {
    alt((range_wild, range_date, range_number, quoted_range, first_range)).parse(input)
}
// second_range_term: RANGE_WILD | DATE_EXPRESSION QUOTED_RANGE | SECOND_RANGE
fn second_range_term(input: Span) -> IResult<Span, RangeTerm> {
    alt((range_wild, range_date, range_number, quoted_range, second_range)).parse(input)
}
// QUOTED_RANGE: ESCAPED_STRING
fn quoted_range(input: Span) -> IResult<Span, RangeTerm> {
    let (remain, string) = quoted_string.parse(input)?;
    Ok((remain, RangeTerm::Value(string)))
}
// FIRST_RANGE: /[^ ]+/
fn first_range(input: Span) -> IResult<Span, RangeTerm> {
    let (remain, value) = escaped_transform(
        is_not(" "),
        '\\',
        alt((
            value(" ", tag(" ")),
        ))
    ).parse(input)?;
    Ok((remain, RangeTerm::Value(value)))
}
// SECOND_RANGE: /[^\]\}]+/
fn second_range(input: Span) -> IResult<Span, RangeTerm> {
    let (remain, value) = escaped_transform(
        is_not(" ]}"),
        '\\',
        alt((
            value(" ", tag(" ")),
            value("]", tag("]")),
            value("}", tag("}")),
        ))
    ).parse(input)?;
    Ok((remain, RangeTerm::Value(value)))
}
// RANGE_WILD: "*"
fn range_wild(input: Span) -> IResult<Span, RangeTerm> {
    value(RangeTerm::Wildcard, ws(tag("*"))).parse(input)
}
fn range_number(input: Span) -> IResult<Span, RangeTerm> {
    let (remain, value) = nom::number::complete::double.parse(input)?;
    Ok((remain, RangeTerm::Numeric(value)))
}

fn quoted_string(input: Span) -> IResult<Span, String> {
    delimited(tag("\""), escaped_transform(
        is_not("\""),
        '\\',
        alt((
            value("\"", tag("\"")),
        ))
    ), tag("\"")).parse(input)
}

fn range_date(input: Span) -> IResult<Span, RangeTerm> {
    let (remain, value) = date_expression.parse(input)?;
    Ok((remain, RangeTerm::Date(value)))
}

fn date_expression(input: Span) -> IResult<Span, DateExpression> {
    alt((relative_date_expression, fixed_date_expression)).parse(input)
}

// date_expression: "now" [offset] [truncate]
fn relative_date_expression(input: Span) -> IResult<Span, DateExpression> {
    let (remain, (_, offset, truncation)) = (tag_no_case("now"), opt(de_offset), opt(de_truncate)).parse(input)?;
    Ok((remain, DateExpression::Relative{changes: offset.unwrap_or_default(), truncation}))
}

// date_expression: date ["T" time] [timezone] [offset] [truncate]
fn fixed_date_expression(input: Span) -> IResult<Span, DateExpression> {
    let (remain, (date, time, timezone, changes)) = (de_date, opt((tag("T"), de_time)), opt(de_timezone), opt((ws(tag("||")), opt(de_offset), opt(de_truncate)))).parse(input)?;
    // merge date and time
    let mut date: NaiveDateTime = match time {
        Some((_, time)) => date.and_time(time),
        None => date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
    };

    // todo! timezones might be backwards
    let mut date: DateTime<Utc> = match timezone {
        Some(zone) => {
            date += Duration::minutes((zone * 60.0) as i64);
            date.and_utc()
        },
        None => date.and_utc(),
    };

    if let Some((_, changes, trun)) = changes {
        if let Some(changes) = changes {
            // todo! return error rather than unwrap
            date = DateExpression::apply_changes(date, &changes).unwrap();
        }
        if let Some(trun) = trun {
            date = DateExpression::apply_truncation(date, trun);
        }
    }

    return Ok((remain, DateExpression::Fixed(date)))
}

// date: yyyymmdd | yyyyddd | yyyy-ddd | yyyy-mm[-dd] | yyyy-"W"ww[-d] | yyyy"W"ww[-d]
fn de_date(input: Span) -> IResult<Span, NaiveDate> {
    alt((de_date_undelimited, de_date_ordinal, de_date_delimited, de_date_week)).parse(input)
}

// yyyymmdd
fn de_date_undelimited(input: Span) -> IResult<Span, NaiveDate> {
    map_opt((
        count(one_of("0123456789"), 4),
        count(one_of("0123456789"), 2),
        count(one_of("0123456789"), 2)
    ), |(year, month, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().ok()?;
        let month: u32 = String::from_iter(month.into_iter()).parse().ok()?;
        let day: u32 = String::from_iter(day.into_iter()).parse().ok()?;
        NaiveDate::from_ymd_opt(year, month, day)
    }).parse(input)
    // date.map_err(|_| ParsingError::invalid_date(input))
}
// yyyyddd | yyyy-ddd
fn de_date_ordinal(input: Span) -> IResult<Span, NaiveDate> {
    map_res((
        count(one_of("0123456789"), 4),
        opt(tag("-")),
        count(one_of("0123456789"), 3)
    ), |(year, _, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let day: u32 = String::from_iter(day.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        NaiveDate::from_yo_opt(year, day).ok_or(ParsingError::invalid_date(input))
    }).parse(input)
}
// yyyy-mm[-dd]
fn de_date_delimited(input: Span) -> IResult<Span, NaiveDate> {
    map_res((
        count(one_of("0123456789"), 4),
        tag("-"),
        count(one_of("0123456789"), 2),
        opt(pair(tag("-"), count(one_of("0123456789"), 2)))
    ), |(year, _, month, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let month: u32 = String::from_iter(month.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let day: u32 = match day {
            Some((_, day)) => String::from_iter(day.into_iter()).parse().map_err(ParsingError::invalid_date)?,
            None => 0,
        };
        NaiveDate::from_ymd_opt(year, month, day).ok_or(ParsingError::invalid_date(input))
    }).parse(input)
}
// yyyy-"W"ww[-d] | yyyy"W"ww[-d]
fn de_date_week(input: Span) -> IResult<Span, NaiveDate> {
    map_res((
        count(one_of("0123456789"), 4),
        opt(tag("-")),
        tag("W"),
        count(one_of("0123456789"), 2),
        opt(pair(opt(tag("-")), one_of("1234567")))
    ), |(year, _, _, week, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let week: u32 = String::from_iter(week.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let day: chrono::Weekday = match day {
            Some((_, day)) => match day {
                '1' => chrono::Weekday::Mon,
                '2' => chrono::Weekday::Tue,
                '3' => chrono::Weekday::Wed,
                '4' => chrono::Weekday::Thu,
                '5' => chrono::Weekday::Fri,
                '6' => chrono::Weekday::Sat,
                '7' => chrono::Weekday::Sun,
                _ => chrono::Weekday::Mon
            },
            None => chrono::Weekday::Mon,
        };
        NaiveDate::from_isoywd_opt(year, week, day).ok_or(ParsingError::invalid_date(input))
    }).parse(input)
}

// time: hh[:mm[:ss[.sss]]] | hh[mm[ss[.sss]]]
fn de_time(input: Span) -> IResult<Span, NaiveTime> {
    let (remain, time) = map_res((
        sixty,
        opt((
            opt(tag(":")),
            sixty,
            opt((
                opt(tag(":")),
                sixty,
                opt((tag("."), is_a("0123456789")))
            ))
        ))
    ), |(hours, parts)|{
        let mut min = 0;
        let mut sec = 0;
        let mut nano = 0;
        if let Some((_, minits, parts)) = parts {
            min = minits as u32;
            if let Some((_, seconds, parts)) = parts {
                sec = seconds as u32;
                if let Some((_, millis)) = parts {
                    let mut digits = millis.len();
                    nano = millis.parse::<u32>().map_err(ParsingError::invalid_date)?;
                    while digits < 9 {
                        nano *= 10;
                        digits += 1;
                    }
                }
            }
        }
        NaiveTime::from_hms_nano_opt(hours as u32, min, sec, nano).ok_or(ParsingError::InvalidTime(input.to_string()))
    }).parse(input)?;
    return Ok((remain, time))
}

fn sixty(input: Span) -> IResult<Span, i64> {
    map_res((one_of("012345"), one_of("0123456789")),
    |(a, b)| {
        String::from_iter([a, b]).parse::<i64>()
    }).parse(input)
}

fn two_digit(input: Span) -> IResult<Span, i64> {
    map_res((one_of("0123456789"), one_of("0123456789")),
    |(a, b)| {
        String::from_iter([a, b]).parse::<i64>()
    }).parse(input)
}

// timezone: "Z" | (+|-) hh ([:mm] | [mm])
fn de_timezone(input: Span) -> IResult<Span, f64> {
    alt((value(0.0, tag("Z")), map((
        alt((value(1.0, tag("+")), value(-1.0, tag("-")))),
        two_digit, opt(pair(opt(tag(":")), sixty))
    ),
        |(sign, hours, minutes)|{
            sign * (hours as f64 + match minutes {
                Some((_, minutes)) => minutes as f64 / 60.0,
                None => 0.0,
            })
        }
    ))).parse(input)
}

// offset: (+|-) number((year|y)|(month)|(day|d)|(hour|h)|(minute|m)|(second|s)) number
fn de_offset(input: Span) -> IResult<Span, Vec<(i64, DateUnit)>> {
    let (remain, changes) = many1((
        ws(alt((value(1, tag("+")), value(-1, tag("-"))))),
        ws(take_while1(|x: char| x.is_ascii_digit())),
        opt(date_unit)
    )).parse(input)?;
    let mut out = vec![];
    for (sign, number, unit) in changes {
        let number: i64 = number.parse().unwrap();
        out.push((sign * number, match unit {
            Some(unit) => unit,
            None => DateUnit::Millis,
        }));
    }
    return Ok((remain, out));
}

// round: "/"
fn de_truncate(input: Span) -> IResult<Span, DateUnit> {
    let (remain, (_, unit)) = pair(ws(tag("/")), date_unit).parse(input)?;
    Ok((remain, unit))
}

fn date_unit(input: Span) -> IResult<Span, DateUnit> {
    alt((
        value(DateUnit::Year, tag_no_case("years")),
        value(DateUnit::Year, tag_no_case("year")),
        value(DateUnit::Year, tag_no_case("y")),
        value(DateUnit::Month, tag_no_case("months")),
        value(DateUnit::Month, tag_no_case("month")),
        value(DateUnit::Week, tag_no_case("weeks")),
        value(DateUnit::Week, tag_no_case("week")),
        value(DateUnit::Week, tag_no_case("w")),
        value(DateUnit::Day, tag_no_case("Days")),
        value(DateUnit::Day, tag_no_case("Day")),
        value(DateUnit::Day, tag_no_case("d")),
        value(DateUnit::Hour, tag_no_case("Hours")),
        value(DateUnit::Hour, tag_no_case("Hour")),
        value(DateUnit::Hour, tag_no_case("h")),
        value(DateUnit::Minute, tag_no_case("Minutes")),
        value(DateUnit::Minute, tag_no_case("Minute")),
        value(DateUnit::Month, tag("M")),
        value(DateUnit::Minute, tag("m")),
        value(DateUnit::Second, tag_no_case("Seconds")),
        value(DateUnit::Second, tag_no_case("Second")),
        value(DateUnit::Second, tag_no_case("s")),
    )).parse(input)
}


#[derive(Debug, Clone)]
pub struct Location {
    pub offset: usize,
    pub length: usize,
}

impl Location {
    pub fn used(original: Span, remaining: Span) -> Self {
        Self {
            offset: original.location_offset(),
            length: original.len().saturating_sub(remaining.len())
        }
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.offset, self.length))
    }
}

// impl From<Span<'_>> for Location {
//     fn from(value: Span) -> Self {
//         Location { offset: value.location_offset(), length: value.len() }
//     }
// }

// impl Sub<Span<'_>> for Location {
//     type Output = Self;

//     fn sub(self, rhs: Span) -> Self::Output {
//         Self {
//             offset: self.offset,
//             length: self.length.saturating_sub(rhs.len())
//         }
//     }
// }