use std::sync::LazyLock;



#[derive(Debug, Clone, thiserror::Error)]
#[error("An invalid field key found: {0}")]
pub struct InvalidField(String);

pub struct FieldName(String);

impl std::fmt::Display for FieldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for FieldName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Regex to match valid elasticsearch/json field names. 
/// Compile it once on first access, from then on just share a compiled regex instance.
pub static FIELD_SANITIZER: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new("^[a-z][a-z0-9_.]*$").expect("Field Regex could not compile")
});

impl std::str::FromStr for FieldName {
    type Err = InvalidField;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if FIELD_SANITIZER.is_match(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(InvalidField(s.to_string()))
        }
    }
}