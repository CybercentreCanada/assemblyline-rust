// Used for validating a classification string against the current configuration from the command-line
use log::{info};
use std::{fs};
use std::process::ExitCode;
use std::sync::Arc;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_markings::config::{ClassificationConfig, ready_classification};
use assemblyline_models::config::Config;

pub fn main(classification: String, config: Arc<Config>) -> std::process::ExitCode {

    // For troubleshooting ensure it's understood what the parser is using when performing validation
    let c12n_config = match &config.classification.path {
        Some(config_path) => {
            info!("Testing against mounted classification configuration: {config_path:?}");
            ready_classification(Some(&fs::read_to_string(config_path).expect("Could not read classification config from file"))).expect("Could not load classification config from file")
        },
        None => {
            info!("No mounted classification configuration found. Proceeding with default configuration...");
            ClassificationConfig::default()
        }
    };

    // Validate the classification string and print the normalized result or an error
    match ClassificationParser::new(c12n_config) {
        Ok(parser) => {
            match parser.normalize_classification(&classification) {
                        Ok(norm_c12n) => {
                            info!("Classification is valid: {norm_c12n:#?}");
                            return ExitCode::SUCCESS;
                        },
                        Err(err) => {
                            info!("Classification is invalid: {err:?}");
                            return ExitCode::FAILURE;
                        }
                    }
        },
        Err(err) => {
            info!("Classification configuration is invalid: {err:?}");
            return ExitCode::FAILURE;
        }
    };  
}

#[cfg(test)]
mod tests {
    use super::main;
    use crate::load_configuration;

    #[tokio::test]
    async fn test_valid_classification() {
        // Load configuration
        let (config, _) = load_configuration(None).await.expect("Could not load configuration");

        // A simple test to ensure the validation function works as expected with a valid classification string
        let classification = "TLP:W//REL CSE".to_string();
        let result = main(classification, config.clone());
        assert_eq!(result, std::process::ExitCode::SUCCESS);
    }

    #[tokio::test]
    async fn test_invalid_classification() {
        // Load configuration
        let (config, _) = load_configuration(None).await.expect("Could not load configuration");

        // A simple test to ensure the validation function works as expected with an invalid classification string
        let invalid_classification = "TLP:R//REL CSE".to_string();
        let result = main(invalid_classification, config.clone());
        assert_eq!(result, std::process::ExitCode::FAILURE);
    }
}
