
use std::sync::OnceLock;

use serde_with::{DeserializeFromStr, SerializeDisplay};

use struct_metadata::Described;

use crate::ElasticMeta;


#[derive(SerializeDisplay, DeserializeFromStr, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct JA4(String);

impl std::fmt::Display for JA4 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

static JA4_REGEX: OnceLock<regex::Regex> = OnceLock::new();

pub fn is_ja4(value: &str) -> bool {
    let regex = JA4_REGEX.get_or_init(|| {
        regex::Regex::new(r"(t|q)([sd]|[0-3]){2}(d|i)\d{2}\d{2}\w{2}_[a-f0-9]{12}_[a-f0-9]{12}").unwrap()
    });
    
    regex.is_match(value)     
}

impl std::str::FromStr for JA4 {
    type Err = BadJA4;

    fn from_str(s: &str) -> Result<Self, Self::Err> {        
        if is_ja4(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(BadJA4(s.to_owned()))
        }
    }
}


#[derive(thiserror::Error, Debug)]
#[error("Invalid JA4 fingerprint ({0})")]
pub struct BadJA4(String);

/// Parse a set of sample JA4 fingerprints from https://github.com/FoxIO-LLC/ja4
/// saved locally as ja4plus-mapping-2025-02-11.csv
#[test]
fn load_sample_ja4_fingerprints() {
    let data = include_str!("ja4plus-mapping-2025-02-11.csv");
    let mut hits = 0;
    for row in data.split("\n").skip(1) {
        for entry in row.split(",").skip(4).take(1) {
            if entry.is_empty() { continue }
            let _value: JA4 = entry.parse().unwrap();
            hits += 1;
        }
    }
    assert_eq!(hits, 30);
}