// Used for validating a classification string against the current configuration from the command-line
use std::process::ExitCode;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_markings::config::ClassificationConfig;

pub fn main(classification: String, c12n_config: ClassificationConfig) -> std::process::ExitCode {
    // Validate the classification string and print the normalized result or an error
    match ClassificationParser::new(c12n_config) {
        Ok(parser) => {
            match parser.normalize_classification(&classification) {
                        Ok(norm_c12n) => {
                            println!("Classification is valid: {norm_c12n:#?}");
                            return ExitCode::SUCCESS;
                        },
                        Err(err) => {
                            println!("Classification is invalid: {err:?}");
                            return ExitCode::FAILURE;
                        }
                    }
        },
        Err(err) => {
            println!("Classification configuration is invalid: {err:?}");
            return ExitCode::FAILURE;
        }
    };  
}

#[cfg(test)]
mod tests {
    use super::main;
    use assemblyline_markings::config::{ClassificationConfig, ready_classification};

    async fn setup() -> ClassificationConfig {
        // Create a default classification configuration for testing
        let mut c12n_config = ready_classification(None).unwrap();

        // Enable enforcement of classification rules for testing purposes
        c12n_config.enforce = true;
        c12n_config
    }

    #[tokio::test]
    async fn test_valid_classification() {
        // A simple test to ensure the validation function works as expected with a valid classification string
        let c12n_config = setup().await;
        let classification = "TLP:W//REL CSE".to_string();
        let result = main(classification, c12n_config);
        assert_eq!(result, std::process::ExitCode::SUCCESS);
    }

    #[tokio::test]
    async fn test_invalid_classification() {
        // A simple test to ensure the validation function works as expected with an invalid classification string
        let invalid_classification = "TLP:R//REL CSE".to_string();
        let c12n_config = setup().await;
        let result = main(invalid_classification, c12n_config);
        assert_eq!(result, std::process::ExitCode::FAILURE);
    }
}
