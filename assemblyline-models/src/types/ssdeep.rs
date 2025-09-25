use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{ElasticMeta, ModelError};



/// Validated ssdeep type
#[derive(SerializeDisplay, DeserializeFromStr, Described, PartialEq, Eq, Debug, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="text", analyzer="text_fuzzy")]
pub struct SSDeepHash(String);

impl std::fmt::Display for SSDeepHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn is_ssdeep_char(value: char) -> bool {
    value.is_ascii_alphanumeric() || value == '/' || value == '+'
}

pub fn is_ssdeep_hash(value: &str) -> bool {
    // SSDEEP_REGEX = r"^[0-9]{1,18}:[a-zA-Z0-9/+]{0,64}:[a-zA-Z0-9/+]{0,64}$"
    let (numbers, hashes) = match value.split_once(":") {
        Some(value) => value,
        None => return false
    };
    let (hasha, hashb) = match hashes.split_once(":") {
        Some(value) => value,
        None => return false
    };
    if numbers.is_empty() || numbers.len() > 18 || numbers.chars().any(|c|!c.is_ascii_digit()) {
        return false
    }
    if hasha.len() > 64 || hasha.chars().any(|c|!is_ssdeep_char(c)) {
        return false
    }
    if hashb.len() > 64 || hashb.chars().any(|c|!is_ssdeep_char(c)) {
        return false
    }
    return true
}

impl std::str::FromStr for SSDeepHash {
    type Err = ModelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if is_ssdeep_hash(s) {
            Ok(SSDeepHash(s.to_owned()))
        } else {
            Err(ModelError::InvalidSSDeep(s.to_owned()))
        }
    }
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<SSDeepHash> for rand::distr::StandardUniform {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> SSDeepHash {
        use rand::distr::{Alphanumeric, SampleString};
        let mut output = String::new();
        output += &rng.random_range(0..10000).to_string();
        output += ":";
        let len = rng.random_range(0..64);
        output += &Alphanumeric.sample_string(rng, len);
        output += ":";
        let len = rng.random_range(0..64);
        output += &Alphanumeric.sample_string(rng, len);
        SSDeepHash(output)
    }
}
