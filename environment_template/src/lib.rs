use std::collections::HashMap;

/// Apply environment variable substitution to a string
/// 
/// On failure returns the best effort and collection of variables that couldn't be substituted.
pub fn apply_env(data: &str) -> Result<String, EnvError> {
    apply_variables(data, &std::env::vars().collect())
}

/// Regex used by the apply_variables method
const REGEX: &str = r#"(^|.)\$\{([0-9[:alpha:]_]+)(?::-?((?:\\\}|[^}])*))?\}"#;

/// Apply variable substitution to the string using the bash syntax
/// 
/// On failure returns the best effort and collection of variables that couldn't be substituted.
pub fn apply_variables(data: &str, vars: &HashMap<String, String>) -> Result<String, EnvError> {
    let parser = regex::Regex::new(REGEX).expect("Failed to compile hard coded regex");
    let mut input = data;
    let mut output: String = "".to_owned();
    let mut failed_variables = vec![];

    let vars: HashMap<String, String> = vars.iter()
        .map(|(k, v)|(k.clone(), v.replace('\n', "\\n")))
        .collect();

    while let Some(capture) = parser.captures(input) {
        // Include the input before the match
        let full_match = capture.get(0).unwrap();
        let start = full_match.start();
        output += &input[0..start];

        // Advance window
        let end = full_match.end();
        input = &input[end..];

        let prefix = capture.get(1).unwrap().as_str();

        // Handle the case where the substition is prefixed with a backslash
        if prefix == "\\" {
            output += &full_match.as_str()[1..];
            continue
        } else {
            output += prefix;
        }

        let name = capture.get(2).unwrap().as_str();
        let default = capture.get(3).map(|val| val.as_str());

        // Get the variable form the map
        if let Some(value) = vars.get(name) {
            output += value;
        } else if let Some(value) = default {
            output += &value.replace("\\}", "}");
        } else {
            failed_variables.push(name.to_owned());
        }
    }

    output += input;
    if failed_variables.is_empty() {
        Ok(output)
    } else {
        Err(EnvError { body: output, failed_variables })
    }
}


#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::apply_variables;


    #[test]
    fn env_application() {
        // Simple substitution0
        let values = HashMap::from([
            ("Abc".to_owned(), "cats".to_owned()),
            ("A_b".to_owned(), "rats".to_owned()),
            ("MULTILINE_STRING".to_owned(), "abc\nabc".to_owned())
        ]);
        assert_eq!(apply_variables("abc123", &values).unwrap(), "abc123".to_owned());
        assert_eq!(apply_variables("abc${Abc}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("${Abc}", &values).unwrap(), "cats".to_owned());
        assert_eq!(apply_variables("${Abc}123", &values).unwrap(), "cats123".to_owned());
        assert_eq!(apply_variables("abc${Abc}", &values).unwrap(), "abccats".to_owned());
        assert_eq!(apply_variables("\\${Abc}123", &values).unwrap(), "${Abc}123".to_owned());
        assert_eq!(apply_variables("abc\\${Abc}123", &values).unwrap(), "abc${Abc}123".to_owned());

        // underscore
        assert_eq!(apply_variables("abc${A_b}123", &values).unwrap(), "abcrats123".to_owned());

        // substitution with default
        assert_eq!(apply_variables("abc${Abc:-dogs}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("abc${xyz:-dogs}123", &values).unwrap(), "abcdogs123".to_owned());
        assert_eq!(apply_variables("abc${Abc:dogs}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("abc${xyz:dogs}123", &values).unwrap(), "abcdogs123".to_owned());

        // empty default
        assert_eq!(apply_variables("abc${Abc:}123", &values).unwrap(), "abccats123".to_owned());
        assert_eq!(apply_variables("abc${xyz:}123", &values).unwrap(), "abc123".to_owned());

        // Error for missing variables
        assert!(apply_variables("abc${xyz}123", &values).is_err());

        // Handle complex value string
        assert_eq!(apply_variables(r#"abc${xyz:{\/$$_0[\}]-+}123"#, &values).unwrap(), "abc{\\/$$_0[}]-+123".to_owned());

        // handle multiline string escaping
        let json_string = apply_variables(r#"{
            "param": "${MULTILINE_STRING}"
        }"#, &values).unwrap();
        assert_eq!(json_string, r#"{
            "param": "abc\nabc"
        }"#);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&json_string).unwrap(), serde_json::json!({"param": "abc\nabc"}));
    }
}

/// Output of the variable application on failure
#[derive(Debug, Clone)]
pub struct EnvError {
    /// The best effort to apply the variable substitution
    pub body: String,
    /// Variables that the body expected but had no provided value
    pub failed_variables: Vec<String>,
}

impl std::fmt::Display for EnvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not complete environment substitution. Had no value or default for the following variables: {}", self.failed_variables.join(", "))
    }
}

impl std::error::Error for EnvError {}