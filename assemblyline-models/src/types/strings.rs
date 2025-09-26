use std::borrow::Cow;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::LazyLock;

use idna::domain_to_ascii;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;
use unicode_normalization::UnicodeNormalization;
use constcat::concat;

use crate::types::net_static::TLDS_SPECIAL_BY_DOMAIN;
use crate::{ElasticMeta, ModelError};


/// A string that maps to a keyword field in elasticsearch.
/// 
/// This is the default behaviour for a String in a mapped struct, the only reason
/// to use this over a standard String is cases where the 'mapping' field has been overwritten
/// by a container and the more explicit 'mapping' this provided is needed to reassert
/// the keyword type.
/// 
/// Example:
///         #[metadata(store=false, mapping="flattenedobject")]
///         pub safelisted_tags: HashMap<String, Vec<Keyword>>,
/// 
/// In that example, if the inner Keyword was String the entire HashMap would have its 
/// mapping set to 'flattenedobject', the inner Keyword more explicitly overrides this.
#[derive(Debug, Serialize, Deserialize, Described, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="keyword")]
pub struct Keyword(String);

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for Keyword {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Keyword {
    fn from(s: String) -> Self {
        Keyword(s)
    }
}

impl From<&str> for Keyword {
    fn from(s: &str) -> Self {
        Keyword(s.to_string())
    }
}


#[derive(Debug, Serialize, Deserialize, Described, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="wildcard")]
pub struct Wildcard(String);

impl std::fmt::Display for Wildcard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for Wildcard {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Wildcard {
    fn from(s: String) -> Self {
        Wildcard(s)
    }
}

impl From<&str> for Wildcard {
    fn from(s: &str) -> Self {
        Wildcard(s.to_string())
    }
}


/// Uppercase String
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[metadata_type(ElasticMeta)]
pub struct UpperString(String);


impl std::fmt::Display for UpperString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for UpperString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for UpperString {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let value = s.trim().to_uppercase();
        Ok(UpperString(value))
    }
}

impl From<&str> for UpperString {
    fn from(s: &str) -> Self {
        let value = s.trim().to_uppercase();
        UpperString(value)
    }
}

impl PartialEq<&str> for UpperString {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq(other)
    }
}


#[derive(Serialize, Deserialize, Described, PartialEq, Eq, Debug, Clone, Default)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="text")]
pub struct Text(pub String);

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<Text> for String {
    fn from(value: Text) -> String {
        value.0
    }
}

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Text {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Could not process {original} as a {name}: {error}")]
pub struct ValidationError {
    original: String, 
    name: &'static str, 
    error: String
}


pub trait StringValidator {
    fn validate<'a>(data: &'a str) -> Result<Cow<'a, str>, ValidationError>;
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValidatedString<Validator> {
    value: String,
    validator: PhantomData<Validator>
}

impl<Validator> std::fmt::Display for ValidatedString<Validator> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

impl<Validator> std::ops::Deref for ValidatedString<Validator> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<Validator> Described<ElasticMeta> for ValidatedString<Validator> {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {
        String::metadata()
    }
}

impl<Validator: StringValidator> Serialize for ValidatedString<Validator> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        self.value.serialize(serializer)
    }
}

impl<'de, Validator: StringValidator> Deserialize<'de> for ValidatedString<Validator> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        match Validator::validate(&value) {
            Ok(value) => Ok(Self { value: value.to_string(), validator: PhantomData}),
            Err(error) => Err(serde::de::Error::custom(error.to_string())),
        }
    }
}

impl<Validator: StringValidator> FromStr for ValidatedString<Validator> {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match check_domain(s) {
            Ok(value) => Ok(Self { value, validator: PhantomData}),
            Err(err) => Err(ValidationError {
                original: s.to_owned(),
                name: "domain",
                error: format!("Domain rejected: {err:?}"),
            }),
        }
    }
}

// MARK: Domains

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("An empty string was provided where a domain was expected")]
    Empty,
    #[error("No top level domain name found")]
    NoDot,
    #[error("An invalid IDNA string was found")]
    InvalidIDNA,
    #[error("Illigal characters were found")]
    IlligalCharacter,
    #[error("The top level domain was rejected")]
    InvalidTLD,
    #[error("Input failed validation")]
    Validation,
}

const DOMAIN_REGEX: &str = r"(?:(?:[A-Za-z0-9\u00a1-\U0010ffff][A-Za-z0-9\u00a1-\U0010ffff_-]{0,62})?[A-Za-z0-9\u00a1-\U0010ffff]\.)+(?:[Xx][Nn]--)?(?:[A-Za-z0-9\u00a1-\U0010ffff]{2,}\.?)";
const DOMAIN_ONLY_REGEX: &str = concat!("^", DOMAIN_REGEX, "$");
const DOMAIN_EXCLUDED_NORM_CHARS: &str = "./?@#";


fn is_domain_excluded_char(item: char) -> bool {
    DOMAIN_EXCLUDED_NORM_CHARS.contains(item)
    // todo!()
    // self.excluded_chars = set(DOMAIN_EXCLUDED_NORM_CHARS)
}

pub fn check_domain(data: &str) -> Result<String, DomainError> {

    if data.is_empty() {
        return Err(DomainError::Empty)
    }

    let data = data.replace('\u{3002}', ".");

    if !data.contains('.') {
        return Err(DomainError::NoDot)
    }

    let mut normalized_parts = vec![];
    for segment in data.split('.'){
        if segment.is_ascii() {
            let segment = segment.to_ascii_lowercase();
            if segment.starts_with("xn--") {
                let (domain, error) = idna::domain_to_unicode(&segment);
                if error.is_err() {
                    return Err(DomainError::InvalidIDNA)
                }
                normalized_parts.push(domain);
                continue
            }
            normalized_parts.push(segment);
        } else {
            let segment_norm = segment.nfkc().collect::<String>();
            // segment_norm = unicodedata.normalize('NFKC', segment)
            if segment != segment_norm && segment_norm.chars().any(is_domain_excluded_char) {
                return Err(DomainError::IlligalCharacter)
                // raise ValueError(f"[{self.name or self.parent_name}] '{segment}' in '{value}' "
                //                     f"includes a Unicode character that can not be normalized to '{segment_norm}'.")
            } 
            normalized_parts.push(segment_norm);
        }
    }
    
    let mut domain = normalized_parts.join(".");

    static VALIDATION_REGEX: LazyLock<Regex> = LazyLock::new(||{
        Regex::new(DOMAIN_ONLY_REGEX).expect("Error in static domain only regex")
    });
    if !VALIDATION_REGEX.is_match(&domain){
        return Err(DomainError::Validation)
        // raise ValueError(f"[{self.name or self.parent_name}] '{domain}' not match the "
        //                     f"validator: {self.validation_regex.pattern}")
    }
    while let Some(new_domain) = domain.strip_suffix(".") {
        domain = new_domain.to_owned();
    }

    if domain.contains("@") {
        return Err(DomainError::IlligalCharacter)   
    }

    if let Some((_, tld)) = domain.rsplit_once(".") {
        let mut tld = tld.to_uppercase();
        if !tld.is_ascii() {
            tld = match domain_to_ascii(&tld) {
                Ok(tld) => tld.to_ascii_uppercase(),
                Err(_) => return Err(DomainError::InvalidTLD),
            };
        }

        let domain = domain.to_uppercase();
        let combined_tlds = find_top_level_domains();
        if combined_tlds.contains(&tld) || TLDS_SPECIAL_BY_DOMAIN.iter().any(|d| domain.ends_with(d)) {
            return Ok(domain.to_lowercase())
        }
    }

    Err(DomainError::InvalidTLD)
} 

// def is_valid_domain(domain: str) -> bool:
//     if "@" in domain:
//         return False

//     if "." in domain:
//         domain = domain.upper()
//         tld = domain.split(".")[-1]
//         if not tld.isascii():
//             try:
//                 tld = tld.encode('idna').decode('ascii').upper()
//             except ValueError:
//                 return False

//         combined_tlds = find_top_level_domains()
//         if tld in combined_tlds:
//             # Single term TLD check
//             return True

//         elif any(domain.endswith(d) for d in TLDS_SPECIAL_BY_DOMAIN):
//             # Multi-term TLD check
//             return True

//     return False


fn system_local_tld() -> Vec<String> {
    let raw_tlds = match std::env::var("SYSTEM_LOCAL_TLD") {
        Ok(tlds) => tlds,
        Err(std::env::VarError::NotPresent) => String::new(),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("SYSTEM_LOCAL_TLD contains non unicode data")
        }
    };

    let mut tlds = vec![];
    for tld in raw_tlds.split(";") {
        let tld = tld.trim();
        if !tld.is_empty() {
            tlds.push(tld.to_owned())
        }
    }
    tlds
}

/// Combine (once and memoize) the three different sources of TLD.
fn find_top_level_domains() -> &'static HashSet<String> {
    static TLDS: LazyLock<HashSet<String>> = LazyLock::new(|| {
        use super::net_static::TLDS_ALPHA_BY_DOMAIN;
        let mut combined_tlds = HashSet::<String>::new(); 
        combined_tlds.extend(TLDS_ALPHA_BY_DOMAIN.iter().map(|s|s.to_string()));

        for d in TLDS_SPECIAL_BY_DOMAIN {
            if !d.contains(".") {
                combined_tlds.insert(d.to_owned());
            }
        } 

        for tld in system_local_tld() {
            let tld = tld.trim_matches('.').to_uppercase();
            if !tld.is_empty() {
                combined_tlds.insert(tld);
            }
        }

        combined_tlds
    });
    &TLDS
}

pub struct DomainValidator;
impl StringValidator for DomainValidator {
    fn validate<'a>(data: &'a str) -> Result<Cow<'a, str>, ValidationError> {
        match check_domain(data) {
            Ok(domain) => Ok(domain.into()),
            Err(err) => Err(ValidationError { original: data.to_string(), name: "domain", error: err.to_string() }),
        }
    }
}

/// validated domain string
pub type Domain = ValidatedString<DomainValidator>;

#[test]
fn internationalized_domains() {
    assert_eq!(check_domain("ουτοπία.δπθ.gr").unwrap(), "ουτοπία.δπθ.gr"); 
    assert_eq!(check_domain("xn--kxae4bafwg.xn--pxaix.gr").unwrap(), "ουτοπία.δπθ.gr");
    assert_eq!(check_domain("site.XN--W4RS40L").unwrap(), "site.嘉里"); 
    assert!(check_domain("ουτοπία.δπθ.g").is_err()); 
    assert!(check_domain("ουτοπία..gr").is_err()); 
    assert!(check_domain("xn--kxae4bafwg.xn--pxaix.g").is_err());
    assert!(check_domain("xn--kxae4bafwg.xn--xaix.gr").is_err());
}

// MARK: URI
// Used for finding URIs in a blob
const URI_PATH: &str = r"([/?#]\S*)";
const URI_REGEX: &str = concat!("((?:(?:[A-Za-z][A-Za-z0-9+.-]*:)//)(?:[^/?#\\s]+@)?(", IP_REGEX, "|", DOMAIN_REGEX, ")(?::\\d{1,5})?", URI_PATH, "?)");
// Used for direct matching
const FULL_URI: &str = concat!("^", URI_REGEX, "$");

#[derive(Debug, thiserror::Error)]
pub enum UriParseError{
    #[error("An empty string was provided as a URI")]
    Empty,
    #[error("The value {0} failed to match the URI validator")]
    Validator(String),
    #[error("Suggested URI {0} failed domain validation {1}")]
    Domain(String, String)
}

pub fn check_uri(value: &str) -> Result<String, UriParseError> {
    if value.is_empty() {
        return Err(UriParseError::Empty)
    }

    static FULL_URI_VALIDATOR: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(FULL_URI).expect("Error in uri regex")
    });

    let matches = match FULL_URI_VALIDATOR.captures(value) {
        Some(matches) => matches,
        None => return Err(UriParseError::Validator(value.to_owned()))
    };

    let host = match matches.get(2) {
        Some(host) => host.as_str(),
        None => return Err(UriParseError::Validator(value.to_owned()))
    };

    let uri = match matches.get(0) {
        Some(uri) => uri.as_str(),
        None => return Err(UriParseError::Validator(value.to_owned()))
    };

    match check_domain(host) {
        Ok(domain) => Ok(uri.replace(host, &domain)),
        Err(_) => if is_ip(host) {
            Ok(uri.replace(host, &host.to_uppercase()))
        } else {
            Err(UriParseError::Domain(value.to_owned(), host.to_owned()))
        },
    }
}

pub struct UriValidator;
impl StringValidator for UriValidator {
    fn validate<'a>(data: &'a str) -> Result<Cow<'a, str>, ValidationError> {
        match check_uri(data) {
            Ok(data) => Ok(data.into()),
            Err(err) => Err(ValidationError { original: data.to_string(), name: "uri", error: err.to_string() }),
        }
    }
}


// class URI(Keyword):
//     def __init__(self, *args, **kwargs):
//         super().__init__(*args, **kwargs)
//         self.validation_regex = re.compile(FULL_URI)


/// Validated uri type
pub type Uri = ValidatedString<UriValidator>;

// /// Unvalidated platform type
// pub type Platform = String;

// /// Unvalidated processor type
// pub type Processor = String;

// /// Unvalidated phone number type
// pub type PhoneNumber = String;

// /// Unvalidated MAC type
// pub type Mac = String;

// /// Unvalidated UNCPath type
// pub type UNCPath = String;

// /// Unvalidated UriPath type
// pub type UriPath = String;

// MARK: IP

const IPV4_REGEX: &str = r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
const IPV6_REGEX: &str = concat!(
    r"(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|",
    r"(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|",
    r"(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|",
    r"(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|",
    r":(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|",
    r"::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|",
    r"(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|",
    r"(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"
);
const IP_REGEX: &str = concat!("(?:", IPV4_REGEX, "|", IPV6_REGEX, ")");
const IP_ONLY_REGEX: &str = concat!("^", IP_REGEX, "$");
// const IPV4_ONLY_REGEX: &str = concat!("^", IPV4_REGEX, "$");
// const IPV6_ONLY_REGEX: &str = concat!("^", IPV6_REGEX, "$");

pub fn is_ip(value: &str) -> bool {
    static IP: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(IP_ONLY_REGEX).expect("IP Regex error")
    });
    IP.is_match(value)
}

// class IP(Keyword):
//     def __init__(self, *args, allow_ipv6=True, allow_ipv4=True, **kwargs):
//         super().__init__(*args, **kwargs)
//         if allow_ipv4 and allow_ipv6:
//             self.validation_regex = re.compile(IP_ONLY_REGEX)
//         elif allow_ipv4:
//             self.validation_regex = re.compile(IPV4_ONLY_REGEX)
//         elif allow_ipv6:
//             self.validation_regex = re.compile(IPV6_ONLY_REGEX)
//         else:
//             raise ValueError("IP type field should allow at least one of IPv4 or IPv6...")

//     def check(self, value, **kwargs):
//         if self.optional and value is None:
//             return None

//         if not value:
//             return None

//         if not self.validation_regex.match(value):
//             raise ValueError(f"[{self.name or self.parent_name}] '{value}' not match the "
//                              f"validator: {self.validation_regex.pattern}")

//         # An additional check for type validation

//         # IPv4
//         if "." in value:
//             return ".".join([str(int(x)) for x in value.split(".")])
//         # IPv6
//         else:
//             return ":".join([str(x) for x in value.split(":")])


// MARK: UNC Path
const PORT_REGEX: &str = r"(0|[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])";
const UNC_PATH_REGEX: &str = concat!(
    r"^\\\\[a-zA-Z0-9\-_\s]{1,63}(?:\.[a-zA-Z0-9\-_\s]{1,63}){0,3}",
    "(?:@SSL)?(?:@", PORT_REGEX, ")?",
    r#"(?:\\[^\\\/\:\*\?"<>\|\r\n]{1,64})+\\*$"#
);

pub fn is_unc_path(value: &str) -> bool {
    static PARSER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(UNC_PATH_REGEX).expect("UNC path regex error")
    });
    PARSER.is_match(value)
}

// class UNCPath(ValidatedKeyword):
//     def __init__(self, *args, **kwargs):
//         super().__init__(UNC_PATH_REGEX, *args, **kwargs)


// MARK: URI Path

static URI_PATH_PARSER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(URI_PATH).expect("URI path regex error")
});

pub fn is_uri_path(value: &str) -> bool {
    URI_PATH_PARSER.is_match(value)
}

// MARK: MAC

const MAC_REGEX: &str = r"^(?:(?:[0-9a-f]{2}-){5}[0-9a-f]{2}|(?:[0-9a-f]{2}:){5}[0-9a-f]{2})$";

static MAC_PARSER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(MAC_REGEX).expect("MAC regex error")
});


pub fn is_mac(value: &str) -> bool {
    MAC_PARSER.is_match(value)
}

// MARK: Email
pub struct EmailValidator;
impl StringValidator for EmailValidator {
    fn validate<'a>(data: &'a str) -> Result<Cow<'a, str>, ValidationError> {
        match check_email(data) {
            Ok(email) => Ok(email.into()),
            Err(err) => Err(ValidationError { original: data.to_string(), name: "email", error: err.to_string() }),
        }
    }
}

/// validated Email string
pub type Email = ValidatedString<EmailValidator>;

#[derive(Debug, thiserror::Error)]
pub enum EmailError {
    #[error("an empty string was provided where an email was expected")]
    Empty,
    #[error("{0} did not match email validator")]
    Validation(String),
    #[error("{0} is not a valid domain in an email")]
    Domain(String),
}

const EMAIL_REGEX: &str = concat!("^[a-zA-Z0-9!#$%&'*+/=?^_‘{|}~-]+(?:\\.[a-zA-Z0-9!#$%&'*+/=?^_‘{|}~-]+)*@(", DOMAIN_REGEX, ")$");

static EMAIL_VALIDATOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(EMAIL_REGEX).expect("Error in email validator")
});

pub fn check_email(email: &str) -> Result<String, EmailError> {
    if email.is_empty() {
        return Err(EmailError::Empty)
    }

    let matches = match EMAIL_VALIDATOR.captures(email) {
        Some(matches) => matches,
        None => return Err(EmailError::Validation(email.to_owned())),
    };

    match matches.get(1) {
        Some(domain) => if check_domain(domain.as_str()).is_ok() {
            Ok(email.to_lowercase())
        } else {
            Err(EmailError::Domain(domain.as_str().to_owned()))
        },
        None => Err(EmailError::Validation(email.to_owned()))
    }
}

// MARK: Phone number

const PHONE_REGEX: &str = r"^(\+?\d{1,2})?[ .-]?(\(\d{3}\)|\d{3})[ .-](\d{3})[ .-](\d{4})$";

static PHONE_PARSER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(PHONE_REGEX).expect("Phone regex error")
});


pub fn is_phone_number(value: &str) -> bool {
    PHONE_PARSER.is_match(value)
}
