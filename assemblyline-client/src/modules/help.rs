use std::sync::Arc;

use crate::{types::Error, JsonMap};
use crate::connection::{Connection, convert_api_output_string, convert_api_output_map};

use super::api_path;


const HELP_PATH: &str = "help";

pub struct Help {
    connection: Arc<Connection>,
}

impl Help {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }

    /// Return the current system classification definition
    pub async fn classification_definition(&self) -> Result<JsonMap, Error> {
        let url = api_path!(HELP_PATH, "classification_definition");
        return self.connection.get(&url, convert_api_output_map).await
    }

    /// Return the current system configuration:
    /// * Max file size
    /// * Max number of embedded files
    /// * Extraction's max depth
    /// * and many others...
    pub async fn configuration(&self) -> Result<JsonMap, Error> {
        let url = api_path!(HELP_PATH, "configuration");
        return self.connection.get(&url, convert_api_output_map).await
    }

    /// Return the current system configuration constants which include:
    /// * Priorities
    /// * File types
    /// * Service tag types
    /// * Service tag contexts
    pub async fn constants(&self) -> Result<JsonMap, Error> {
        let url = api_path!(HELP_PATH, "constants");
        return self.connection.get(&url, convert_api_output_map).await
    }

    /// Return the current system terms of service
    pub async fn tos(&self) -> Result<String, Error> {
        let url = api_path!(HELP_PATH, "tos");
        return self.connection.get(&url, convert_api_output_string).await
    }
}

#[cfg(test)]
mod test {
    use crate::tests::prepare_client;
    use assemblyline_markings::config::ClassificationConfig;

    #[tokio::test]
    async fn fetch_classification() {
        let client = prepare_client().await;
        let mut def = client.help.classification_definition().await.unwrap();
        let definition = def.remove("original_definition").unwrap();
        let _classification: ClassificationConfig = serde_json::from_value(definition).unwrap();
    }

    #[tokio::test]
    async fn fetch_configuration() {
        let client = prepare_client().await;
        let def = client.help.configuration().await.unwrap();
        assert!(!def.is_empty());
    }

    #[tokio::test]
    async fn fetch_constants() {
        let client = prepare_client().await;
        let def = client.help.constants().await.unwrap();
        assert!(!def.is_empty());
    }

    #[tokio::test]
    async fn fetch_tos() {
        let client = prepare_client().await;
        let def = client.help.tos().await.unwrap();
        assert!(!def.is_empty())
    }

}