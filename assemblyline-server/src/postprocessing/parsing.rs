

use std::str::FromStr;

use chrono::{DateTime, Utc, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, escaped_transform, is_not, take_while1, tag_no_case, is_a};
use nom::character::complete::{multispace0, alphanumeric1, one_of};
use nom::combinator::{map, map_opt, map_res, opt, value};
use nom::error::ParseError;
use nom::multi::{separated_list1, count, many1};
use nom::number::complete::double;
use nom::sequence::{delimited, tuple, pair};

use super::search::{Query, PrefixOperator, StringQuery, FieldQuery, RangeBound, RangeTerm, RangeQuery, DateExpression, DateUnit, NumberQuery};
use super::ParsingError;

pub fn expression(input: &str) -> IResult<&str, Query> {
    // println!("expression: {input}");
    or_expr(input)
}

fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

fn or_expr(input: &str) -> IResult<&str, Query> {
    let (remain, mut sub_queries) = separated_list1(or_operator, and_expr)(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, Query::Or(sub_queries)))
    }
}
fn or_operator(input: &str) -> IResult<&str, ()> {
    let (remain, _) = ws(alt((tag("OR"), tag("||"))))(input)?;
    return Ok((remain, ()))
}

// and_expr: not_expr ("AND" not_expr)*
fn and_expr(input: &str) -> IResult<&str, Query> {
    let (remain, mut sub_queries) = separated_list1(and_operator, not_expr)(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, Query::And(sub_queries)))
    }
}
fn and_operator(input: &str) -> IResult<&str, ()> {
    let (remain, _) = ws(alt((tag("AND"), tag("&&"))))(input)?;
    return Ok((remain, ()))
}


// not_expr: NOT_OPERATOR? atom
// NOT_OPERATOR: "NOT"
fn not_expr(input: &str) -> IResult<&str, Query> {
    let (remain, (not_operator, sub_query)) = tuple((opt(not_operator), atom))(input)?;
    if not_operator.is_some() {
        Ok((remain, Query::Not(Box::new(sub_query))))
    } else {
        Ok((remain, sub_query))
    }
}
fn not_operator(input: &str) -> IResult<&str, ()> {
    let (remain, _) = ws(alt((tag("NOT"), tag("!"))))(input)?;
    return Ok((remain, ()))
}

// atom: "(" expression ")"
//     | exists
//     | field
//     | term
fn atom(input: &str) -> IResult<&str, Query> {
    // println!("atom: {input}");
    alt((
        delimited(ws(tag("(")), expression, ws(tag(")"))), 
        exists,
        field, 
        term
    ))(input)
}

// term: PREFIX_OPERATOR? (phrase_term | SIMPLE_TERM)
fn term(input: &str) -> IResult<&str, Query> {
    // println!("term: {input}");
    alt((
        map(string_query, Query::MatchAny),
        map(pattern_term, Query::RegexAny),
    ))(input)
}

fn number_term(input: &str) -> IResult<&str, FieldQuery> {
    let (remain, (operator, value)) = tuple((opt(ws(prefix_operator)), double))(input)?;
    Ok((remain, FieldQuery::Number(NumberQuery { operator, value })))
}

// field_term: PREFIX_OPERATOR? (phrase_term | SIMPLE_TERM)
fn field_term(input: &str) -> IResult<&str, FieldQuery> {
    // println!("field_term: {input}");
    alt((
        map(string_query, FieldQuery::Match),
        map(pattern_term, FieldQuery::Regex),
    ))(input)
}
fn string_query(input: &str) -> IResult<&str, StringQuery> {
    // println!("string_query: {input}");
    let (remain, (operator, value)) = tuple((opt(ws(prefix_operator)), alt((phrase_term, simple_term))))(input)?;
    Ok((remain, StringQuery { operator, value }))
}

// PREFIX_OPERATOR: "-" | "+" | ">=" | "<=" | ">" | "<"
fn prefix_operator(input: &str) -> IResult<&str, PrefixOperator> {
    map_res(alt((tag("-"), tag("+"), tag(">="), tag("<="), tag(">"), tag("<"))), PrefixOperator::from_str)(input)
}

// SIMPLE_TERM: ("\\+" | "\\-" | "\\&&" | "\\&" | "\\||" | "\\|" | "\\!" | "\\(" | "\\)" | "\\{"
//              | "\\}" | "\\[" | "\\]" | "\\^" | "\\\"" | "\\~" | "\\*" | "\\ "
//              | "\\?" | "\\:" | "\\\\" | "*" | "?" | "_" | "-" | DIGIT | LETTER)+
// (escaped (multi character | single character)) | special chars | alphanum
fn is_special(value: char) -> bool {
    matches!(value, '_' | '-')
}
fn simple_term(input: &str) -> IResult<&str, String> {
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
    })(input)
}

fn pattern_term(input: &str) -> IResult<&str, regex::Regex> {
    // println!("pattern_term: {input}");
    map_res(many1(alt((
        map(tag("*"), |_|{String::from(".*")}),
        map(tag("?"), |_|{String::from(".")}),
        map(simple_term, |row|{regex::escape(&row)}),
    ))), |parts|{
        let pattern = parts.join("");
        regex::Regex::new(&pattern)
    })(input)
}

// phrase_term: ESCAPED_STRING
fn phrase_term(input: &str) -> IResult<&str, String> {
    quoted_string(input)
}

// field: FIELD_LABEL ":" field_value
fn field(input: &str) -> IResult<&str, Query> {
    // println!("field: {input}");
    let (remain, (label, _, query)) = tuple((field_label, ws(tag(":")), field_value))(input)?;
    Ok((remain, Query::MatchField(label, query)))
}

// exists: "_exists_" ":" FIELD_LABEL
fn exists(input: &str) -> IResult<&str, Query> {
    let (remain, (_, _, label)) = tuple((ws(tag_no_case("_exists_")), ws(tag(":")), field_label))(input)?;
    Ok((remain, Query::FieldExists(label)))
}

// FIELD_LABEL: CNAME ["." CNAME]*
fn field_label(input: &str) -> IResult<&str, Vec<String>> {
    separated_list1(tag("."), cname)(input)
}

fn cname(input: &str) -> IResult<&str, String> {
    // println!("cname: {input}");
    let (remain, (a, b)) = tuple((take_while1(|item: char| item.is_alphabetic() || item == '_'), take_while(|item: char| item.is_alphanumeric() || item == '_')))(input)?;
    // println!("cname X: {a} {b}");
    Ok((remain, a.to_owned() + b))
}

// field_value: range
//            | field_term
//            | REGEX_TERM
//            | "(" field_expression ")"
fn field_value(input: &str) -> IResult<&str, FieldQuery> {
    // println!("field_value: {input}");
    alt((range, delimited(ws(tag("(")), field_expression, ws(tag(")"))), regex_term, number_term, field_term))(input)
}

// REGEX_TERM: /\/([^\/]|(\\\/))*\//
fn regex_term(input: &str) -> IResult<&str, FieldQuery> {
    let (remain, regex) = map_res(delimited(tag("/"), escaped_transform(
        is_not("/"),
        '\\',
        alt((
            value("/", tag("/")),
        ))
    ), tag("/")), |pattern| regex::Regex::new(&pattern))(input)?;
    Ok((remain, FieldQuery::Regex(regex)))
}

// range: RANGE_START first_range_term "TO" second_range_term RANGE_END
// RANGE_START: "[" | "{"
// RANGE_END: "]" | "}"
fn range(input: &str) -> IResult<&str, FieldQuery> {
    let (remain, (start_bound, start, _, end, end_bound)) = tuple((range_start, first_range_term, ws(tag("TO")), second_range_term, range_end))(input)?;
    Ok((remain, FieldQuery::Range(RangeQuery{
        start,
        end,
        start_bound,
        end_bound
    })))
}
fn range_start(input: &str) -> IResult<&str, RangeBound> {
    ws(alt((value(RangeBound::Exclusive, tag("{")), value(RangeBound::Inclusive, tag("[")))))(input)
}
fn range_end(input: &str) -> IResult<&str, RangeBound> {
    ws(alt((value(RangeBound::Exclusive, tag("}")), value(RangeBound::Inclusive, tag("]")))))(input)
}

// field_expression: field_or_expr
fn field_expression(input: &str) -> IResult<&str, FieldQuery> {
    field_or_expr(input)
}
// field_or_expr: field_and_expr ("OR" field_and_expr)*
fn field_or_expr(input: &str) -> IResult<&str, FieldQuery> {
    let (remain, mut sub_queries) = separated_list1(or_operator, field_and_expr)(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, FieldQuery::Or(sub_queries)))
    }
}
// field_and_expr: field_not_expr ("AND" field_not_expr)*
fn field_and_expr(input: &str) -> IResult<&str, FieldQuery> {
    let (remain, mut sub_queries) = separated_list1(and_operator, field_not_expr)(input)?;
    if sub_queries.len() == 1 {
        Ok((remain, sub_queries.pop().unwrap()))
    } else {
        Ok((remain, FieldQuery::And(sub_queries)))
    }
}
// field_not_expr: NOT_OPERATOR? field_atom
fn field_not_expr(input: &str) -> IResult<&str, FieldQuery> {
    let (remain, (not_operator, sub_query)) = tuple((opt(not_operator), field_atom))(input)?;
    if not_operator.is_some() {
        Ok((remain, FieldQuery::Not(Box::new(sub_query))))
    } else {
        Ok((remain, sub_query))
    }
}
// field_atom: field_term
//           | "(" field_expression ")"
fn field_atom(input: &str) -> IResult<&str, FieldQuery> {
    alt((delimited(ws(tag("(")), field_expression, ws(tag(")"))), field_term))(input)
}

// first_range_term: RANGE_WILD | DATE_EXPRESSION | QUOTED_RANGE | FIRST_RANGE
fn first_range_term(input: &str) -> IResult<&str, RangeTerm> {
    alt((range_wild, range_date, range_number, quoted_range, first_range))(input)
}
// second_range_term: RANGE_WILD | DATE_EXPRESSION QUOTED_RANGE | SECOND_RANGE
fn second_range_term(input: &str) -> IResult<&str, RangeTerm> {
    alt((range_wild, range_date, range_number, quoted_range, second_range))(input)
}
// QUOTED_RANGE: ESCAPED_STRING
fn quoted_range(input: &str) -> IResult<&str, RangeTerm> {
    let (remain, string) = quoted_string(input)?;
    Ok((remain, RangeTerm::Value(string)))
}
// FIRST_RANGE: /[^ ]+/
fn first_range(input: &str) -> IResult<&str, RangeTerm> {
    let (remain, value) = escaped_transform(
        is_not(" "),
        '\\',
        alt((
            value(" ", tag(" ")),
        ))
    )(input)?;
    Ok((remain, RangeTerm::Value(value)))
}
// SECOND_RANGE: /[^\]\}]+/
fn second_range(input: &str) -> IResult<&str, RangeTerm> {
    let (remain, value) = escaped_transform(
        is_not(" ]}"),
        '\\',
        alt((
            value(" ", tag(" ")),
            value("]", tag("]")),
            value("}", tag("}")),
        ))
    )(input)?;
    Ok((remain, RangeTerm::Value(value)))
}
// RANGE_WILD: "*"
fn range_wild(input: &str) -> IResult<&str, RangeTerm> {
    value(RangeTerm::Wildcard, ws(tag("*")))(input)
}
fn range_number(input: &str) -> IResult<&str, RangeTerm> {
    let (remain, value) = nom::number::complete::double(input)?;
    Ok((remain, RangeTerm::Numeric(value)))
}

fn quoted_string(input: &str) -> IResult<&str, String> {
    delimited(tag("\""), escaped_transform(
        is_not("\""),
        '\\',
        alt((
            value("\"", tag("\"")),
        ))
    ), tag("\""))(input)
}

fn range_date(input: &str) -> IResult<&str, RangeTerm> {
    let (remain, value) = date_expression(input)?;
    Ok((remain, RangeTerm::Date(value)))
}

fn date_expression(input: &str) -> IResult<&str, DateExpression> {
    alt((relative_date_expression, fixed_date_expression))(input)
}

// date_expression: "now" [offset] [truncate]
fn relative_date_expression(input: &str) -> IResult<&str, DateExpression> {
    let (remain, (_, offset, truncation)) = tuple((tag_no_case("now"), opt(de_offset), opt(de_truncate)))(input)?;
    Ok((remain, DateExpression::Relative{changes: offset.unwrap_or_default(), truncation}))
}

// date_expression: date ["T" time] [timezone] [offset] [truncate]
fn fixed_date_expression(input: &str) -> IResult<&str, DateExpression> {
    let (remain, (date, time, timezone, changes)) = tuple((de_date, opt(tuple((tag("T"), de_time))), opt(de_timezone), opt(tuple((ws(tag("||")), opt(de_offset), opt(de_truncate))))))(input)?;
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
fn de_date(input: &str) -> IResult<&str, NaiveDate> {
    alt((de_date_undelimited, de_date_ordinal, de_date_delimited, de_date_week))(input)
}

// yyyymmdd
fn de_date_undelimited(input: &str) -> IResult<&str, NaiveDate> {
    map_opt(tuple((
        count(one_of("0123456789"), 4),
        count(one_of("0123456789"), 2),
        count(one_of("0123456789"), 2)
    )), |(year, month, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().ok()?;
        let month: u32 = String::from_iter(month.into_iter()).parse().ok()?;
        let day: u32 = String::from_iter(day.into_iter()).parse().ok()?;
        NaiveDate::from_ymd_opt(year, month, day)
    })(input)
    // date.map_err(|_| ParsingError::invalid_date(input))
}
// yyyyddd | yyyy-ddd
fn de_date_ordinal(input: &str) -> IResult<&str, NaiveDate> {
    map_res(tuple((
        count(one_of("0123456789"), 4),
        opt(tag("-")),
        count(one_of("0123456789"), 3)
    )), |(year, _, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let day: u32 = String::from_iter(day.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        NaiveDate::from_yo_opt(year, day).ok_or(ParsingError::invalid_date(input))
    })(input)
}
// yyyy-mm[-dd]
fn de_date_delimited(input: &str) -> IResult<&str, NaiveDate> {
    map_res(tuple((
        count(one_of("0123456789"), 4),
        tag("-"),
        count(one_of("0123456789"), 2),
        opt(pair(tag("-"), count(one_of("0123456789"), 2)))
    )), |(year, _, month, day)| {
        let year: i32 = String::from_iter(year.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let month: u32 = String::from_iter(month.into_iter()).parse().map_err(ParsingError::invalid_date)?;
        let day: u32 = match day {
            Some((_, day)) => String::from_iter(day.into_iter()).parse().map_err(ParsingError::invalid_date)?,
            None => 0,
        };
        NaiveDate::from_ymd_opt(year, month, day).ok_or(ParsingError::invalid_date(input))
    })(input)
}
// yyyy-"W"ww[-d] | yyyy"W"ww[-d]
fn de_date_week(input: &str) -> IResult<&str, NaiveDate> {
    map_res(tuple((
        count(one_of("0123456789"), 4),
        opt(tag("-")),
        tag("W"),
        count(one_of("0123456789"), 2),
        opt(pair(opt(tag("-")), one_of("1234567")))
    )), |(year, _, _, week, day)| {
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
    })(input)
}

// time: hh[:mm[:ss[.sss]]] | hh[mm[ss[.sss]]]
fn de_time(input: &str) -> IResult<&str, NaiveTime> {
    let (remain, time) = map_res(tuple((
        sixty,
        opt(tuple((
            opt(tag(":")),
            sixty,
            opt(tuple((
                opt(tag(":")),
                sixty,
                opt(tuple((tag("."), is_a("0123456789"))))
            )))
        )))
    )), |(hours, parts)|{
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
        NaiveTime::from_hms_nano_opt(hours as u32, min, sec, nano).ok_or(ParsingError::InvalidTime(input.to_owned()))
    })(input)?;
    return Ok((remain, time))
}

fn sixty(input: &str) -> IResult<&str, i64> {
    map_res(tuple((one_of("012345"), one_of("0123456789"))),
    |(a, b)| {
        String::from_iter([a, b]).parse::<i64>()
    })(input)
}

fn two_digit(input: &str) -> IResult<&str, i64> {
    map_res(tuple((one_of("0123456789"), one_of("0123456789"))),
    |(a, b)| {
        String::from_iter([a, b]).parse::<i64>()
    })(input)
}

// timezone: "Z" | (+|-) hh ([:mm] | [mm])
fn de_timezone(input: &str) -> IResult<&str, f64> {
    alt((value(0.0, tag("Z")), map(tuple((
        alt((value(1.0, tag("+")), value(-1.0, tag("-")))),
        two_digit, opt(pair(opt(tag(":")), sixty))
    )),
        |(sign, hours, minutes)|{
            sign * (hours as f64 + match minutes {
                Some((_, minutes)) => minutes as f64 / 60.0,
                None => 0.0,
            })
        }
    )))(input)
}

// offset: (+|-) number((year|y)|(month)|(day|d)|(hour|h)|(minute|m)|(second|s)) number
fn de_offset(input: &str) -> IResult<&str, Vec<(i64, DateUnit)>> {
    let (remain, changes) = many1(tuple((
        ws(alt((value(1, tag("+")), value(-1, tag("-"))))),
        ws(take_while1(|x: char| x.is_ascii_digit())),
        opt(date_unit)
    )))(input)?;
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
fn de_truncate(input: &str) -> IResult<&str, DateUnit> {
    let (remain, (_, unit)) = pair(ws(tag("/")), date_unit)(input)?;
    Ok((remain, unit))
}

fn date_unit(input: &str) -> IResult<&str, DateUnit> {
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
    ))(input)
}

