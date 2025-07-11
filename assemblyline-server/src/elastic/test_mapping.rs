
// from assemblyline import odm
// from assemblyline.datastore.collection import ESCollection
// from assemblyline.datastore.support.build import build_mapping

use std::collections::HashMap;
use std::sync::Arc;

use assemblyline_models::config::Config;
use assemblyline_models::meta::build_mapping;
use assemblyline_models::{ElasticMeta, Readable, types::Wildcard};
use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::elastic::collection::{Collection, FieldInformation};
use crate::{get_datastore_ca, get_datastore_verify};

use super::ElasticHelper;

async fn build_elastic_helper() -> ElasticHelper {
    // connect to elastic
    let config = Config::default();
    let datastore_ca = get_datastore_ca().await.unwrap();
    let datastore_ca = datastore_ca.as_ref().map(|val|&val[..]);
    let datastore_verify = get_datastore_verify().unwrap();
    ElasticHelper::connect(&config.datastore.hosts[0], false, datastore_ca, !datastore_verify).await.unwrap()
}

#[derive(Described, Serialize, Deserialize)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true)]
struct OdmTestMapping1 {
    pub stable_text_field: String,
    pub swapped_text_field: String,
    pub stable_number_field: i32,
    pub swapped_number_field: i32,
}

#[derive(Described, Serialize, Deserialize)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true)]
struct OdmTestMapping2 {
    pub stable_text_field: String,
    pub swapped_text_field: Wildcard,
    pub stable_number_field: i32,
    pub swapped_number_field: i64,
}

impl Readable for OdmTestMapping1 {fn set_from_archive(&mut self, _from_archive: bool) {}}
impl Readable for OdmTestMapping2 {fn set_from_archive(&mut self, _from_archive: bool) {}}

/// Test that the example models produce the expected mapping types
#[test]
fn test_example_mapping_type() {
    let mappings = build_mapping::<OdmTestMapping1>().unwrap();   

    // There should be no dynamic mappings, just one rule forbidding implicit mappings
    assert_eq!(mappings.dynamic_templates.len(), 1);
    assert!(mappings.dynamic_templates[0].contains_key("refuse_all_implicit_mappings"));

    // Check that the static fields have the mapping type we want
    assert_eq!(mappings.properties.len(), 4);
    assert_eq!(mappings.properties.get("stable_text_field").unwrap().type_.as_ref().unwrap(), "keyword");
    assert_eq!(mappings.properties.get("swapped_text_field").unwrap().type_.as_ref().unwrap(), "keyword");
    assert_eq!(mappings.properties.get("stable_number_field").unwrap().type_.as_ref().unwrap(), "integer");
    assert_eq!(mappings.properties.get("swapped_number_field").unwrap().type_.as_ref().unwrap(), "integer");

    let mappings = build_mapping::<OdmTestMapping2>().unwrap();   

    // There should be no dynamic mappings, just one rule forbidding implicit mappings
    assert_eq!(mappings.dynamic_templates.len(), 1);
    assert!(mappings.dynamic_templates[0].contains_key("refuse_all_implicit_mappings"));

    // Check that the static fields have the mapping type we want
    assert_eq!(mappings.properties.len(), 4);
    assert_eq!(mappings.properties.get("stable_text_field").unwrap().type_.as_ref().unwrap(), "keyword");
    assert_eq!(mappings.properties.get("swapped_text_field").unwrap().type_.as_ref().unwrap(), "wildcard");
    assert_eq!(mappings.properties.get("stable_number_field").unwrap().type_.as_ref().unwrap(), "integer");
    assert_eq!(mappings.properties.get("swapped_number_field").unwrap().type_.as_ref().unwrap(), "long");
}

/// Test that changing a field from keyword to wildcard doesn't break anything.
#[tokio::test]
async fn test_field_upgrade_ok() {
    let connection = Arc::new(build_elastic_helper().await);

    // Clean up from any previous runs
    {
        connection.remove_index("test_field_upgrade_ok_hot__reindex").await.unwrap();
        connection.remove_index("test_field_upgrade_ok_hot").await.unwrap();
        // let collection =  Collection::<OdmTestMapping1>::new(connection.clone(), "test_field_upgrade_ok".to_string(), None, "".to_string(), false).await.unwrap();
        // collection.wipe(false, None).await.unwrap();
        assert!(!connection.does_index_exist("test_field_upgrade_ok").await.unwrap());
        assert!(!connection.does_index_exist("test_field_upgrade_ok_hot").await.unwrap());
    }
    
    // Create the collection in elastic 
    {
        let collection =  Collection::<OdmTestMapping1>::new(connection.clone(), "test_field_upgrade_ok".to_string(), None, "".to_string(), true).await.unwrap();
        let properties = collection.fields().await.unwrap();
        assert_eq!(properties.get("stable_text_field").unwrap().mapping, "keyword");
        assert_eq!(properties.get("swapped_text_field").unwrap().mapping, "keyword");
        assert_eq!(properties.get("stable_number_field").unwrap().mapping, "integer");
        assert_eq!(properties.get("swapped_number_field").unwrap().mapping, "integer");
    }

    // Open that same collection using the new mapping
    let collection =  Collection::<OdmTestMapping2>::new(connection.clone(), "test_field_upgrade_ok".to_string(), None, "".to_string(), true).await.unwrap();

    // Check that the fields haven't changed
    let properties = collection.fields().await.unwrap();
    assert_eq!(properties.get("stable_text_field").unwrap().mapping, "keyword");
    assert_eq!(properties.get("swapped_text_field").unwrap().mapping, "keyword");
    assert_eq!(properties.get("stable_number_field").unwrap().mapping, "integer");
    assert_eq!(properties.get("swapped_number_field").unwrap().mapping, "integer");

    // Reindex
    collection.reindex(None).await.expect("reindex error");

    // Check that the fields match the new model
    let properties = collection.fields().await.unwrap();
    assert_eq!(properties.get("stable_text_field").unwrap().mapping, "keyword");
    assert_eq!(properties.get("swapped_text_field").unwrap().mapping, "wildcard");
    assert_eq!(properties.get("stable_number_field").unwrap().mapping, "integer");
    assert_eq!(properties.get("swapped_number_field").unwrap().mapping, "long");
}

#[tokio::test]
async fn test_metadata_indexing() {
    let connection = Arc::new(build_elastic_helper().await);

    #[derive(Debug, Serialize, Deserialize, Described)]
    #[metadata_type(ElasticMeta)]
    #[metadata(index=true)]
    struct TestMapping {
        pub id: String,
        #[metadata(copyto="__text__")]
        pub metadata: HashMap<String, Wildcard>,
    }
    impl Readable for TestMapping {fn set_from_archive(&mut self, _from_archive: bool) {}}

    // Clean up from any previous runs
    let collection =  Collection::<TestMapping>::new(connection.clone(), "test_metadata_indexing".to_string(), None, "".to_string(), false).await.unwrap();
    collection.wipe(false, None).await.unwrap();

    // Create with new mapping configuration
    let collection =  Collection::<TestMapping>::new(connection.clone(), "test_metadata_indexing".to_string(), None, "".to_string(), true).await.unwrap();

    // Insert data to trigger dynamic field creation
    collection.save("1", &TestMapping{id: "1".to_owned(), metadata: [("field1".to_string(), "123".into())].into()}, None, None).await.unwrap();
    collection.save("2", &TestMapping{id: "2".to_owned(), metadata: [("field2".to_string(), "123".into())].into()}, None, None).await.unwrap();
    collection.save("3", &TestMapping{id: "3".to_owned(), metadata: [("field3".to_string(), "{\"subfield\": \"cat dog cat\"}".into())].into()}, None, None).await.unwrap();
    collection.save("4", &TestMapping{id: "4".to_owned(), metadata: [("address".to_string(), "https://cyber.gc.ca".into())].into()}, None, None).await.unwrap();
    collection.commit(None).await.unwrap();

    // Check if those fields are the type and config we want
    let mut fields = collection.fields().await.unwrap();
    fields.remove("id");

    assert_eq!(fields.len(), 4);
    for (_field_name, field) in fields {
        assert_eq!(field.mapping, "wildcard");
        assert!(field.indexed);
        assert!(field.default);
    }

    // Check that copyto and regex work
    let search = collection.search("cyber.gc.ca").fields("*").execute::<()>().await.unwrap();
    assert_eq!(search.total, 1);
    assert_eq!(search.source_items[0].id, "4");

    let search = collection.search("address: /http[s]://cyber\\.(gc\\.ca|com)/").fields("*").execute::<()>().await.unwrap();
    assert_eq!(search.total, 1);
    assert_eq!(search.source_items[0].id, "4");
}



#[tokio::test]
async fn test_fields() {
    let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Debug).try_init();
    let connection = Arc::new(build_elastic_helper().await);

    #[derive(Debug, Serialize, Deserialize, Described)]
    #[metadata_type(ElasticMeta)]
    #[metadata(index=true, copyto="__text__")]
    struct InnerModel {
        pub key: String,
        pub number: i32,
        pub regex: Wildcard,
    }


    #[derive(Debug, Serialize, Deserialize, Described)]
    #[metadata_type(ElasticMeta)]
    #[metadata(index=false, store=true)]
    struct TestModel {
        pub id: String,
        pub number: i32,
        pub regex: Wildcard,
        pub inner: InnerModel,
    }
    impl Readable for TestModel {fn set_from_archive(&mut self, _from_archive: bool) {}}

    // Clean up from any previous runs
    let collection = Collection::<TestModel>::new(connection.clone(), "test_fields".to_string(), None, "".to_string(), false).await.unwrap();
    collection.wipe(false, None).await.unwrap();

    // Create with new mapping configuration
    let collection =  Collection::<TestModel>::new(connection.clone(), "test_fields".to_string(), None, "".to_string(), true).await.unwrap();

    // Check if those fields are the type and config we want
    let mut fields = collection.fields().await.unwrap();

    let _id = fields.remove("id").unwrap();

    let field = fields.remove("number").unwrap();
    assert_eq!(field.mapping, "integer");
    assert!(!field.indexed);
    assert!(field.stored);
    assert!(!field.default);

    let field = fields.remove("regex").unwrap();
    assert_eq!(field.mapping, "wildcard");
    assert!(field.indexed);
    assert!(!field.stored);
    assert!(!field.default);

    let field = fields.remove("inner.key").unwrap();
    assert_eq!(field.mapping, "keyword");
    assert!(field.indexed);
    assert!(field.stored);
    assert!(field.default);

    let field = fields.remove("inner.number").unwrap();
    assert_eq!(field.mapping, "integer");
    assert!(field.indexed);
    assert!(field.stored);
    assert!(field.default);

    let field = fields.remove("inner.regex").unwrap();
    assert_eq!(field.mapping, "wildcard");
    assert!(field.indexed);
    assert!(!field.stored);
    assert!(field.default);

    assert!(fields.is_empty(), "{fields:?}");
}

#[tokio::test]
async fn add_field() {
    let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Debug).try_init();
    let connection = Arc::new(build_elastic_helper().await);

    #[derive(Debug, Serialize, Deserialize, Described)]
    #[metadata_type(ElasticMeta)]
    #[metadata(index=true, copyto="__text__")]
    struct InnerModel {
        pub regex: Wildcard,
    }

    #[derive(Debug, Serialize, Deserialize, Described)]
    #[metadata_type(ElasticMeta)]
    #[metadata(index=false, store=true)]
    struct TestModel {
        pub id: String,
        pub number: i32,
    }
    impl Readable for TestModel {fn set_from_archive(&mut self, _from_archive: bool) {}}

    #[derive(Debug, Serialize, Deserialize, Described)]
    #[metadata_type(ElasticMeta)]
    #[metadata(index=false, store=true)]
    struct TestModel2 {
        pub id: String,
        pub number: i32,
        pub big_number: i64,
        #[metadata(copyto="__text__")]
        pub hats: String,
        pub inner: InnerModel,
    }
    impl Readable for TestModel2 {fn set_from_archive(&mut self, _from_archive: bool) {}}

    // Clean up from any previous runs
    let collection = Collection::<TestModel>::new(connection.clone(), "add_field".to_string(), None, "".to_string(), false).await.unwrap();
    collection.wipe(false, None).await.unwrap();

    // Create with new mapping configuration
    let collection =  Collection::<TestModel>::new(connection.clone(), "add_field".to_string(), None, "".to_string(), true).await.unwrap();

    // Check if those fields are the type and config we want
    let mut fields = collection.fields().await.unwrap();
    assert_eq!(fields.remove("id").unwrap(), FieldInformation { default: false, indexed: false, stored: true, mapping: "keyword".to_string() });
    assert_eq!(fields.remove("number").unwrap(), FieldInformation { default: false, indexed: false, stored: true, mapping: "integer".to_string() });
    assert!(fields.is_empty());

    // Create with new mapping configuration
    let collection =  Collection::<TestModel2>::new(connection.clone(), "add_field".to_string(), None, "".to_string(), true).await.unwrap();

    // Check if those fields are the type and config we want
    let mut fields = collection.fields().await.unwrap();
    assert_eq!(fields.remove("id").unwrap(), FieldInformation { default: false, indexed: false, stored: true, mapping: "keyword".to_string() });
    assert_eq!(fields.remove("number").unwrap(), FieldInformation { default: false, indexed: false, stored: true, mapping: "integer".to_string() });
    assert_eq!(fields.remove("big_number").unwrap(), FieldInformation { default: false, indexed: false, stored: true, mapping: "long".to_string() });
    assert_eq!(fields.remove("hats").unwrap(), FieldInformation { default: true, indexed: false, stored: true, mapping: "keyword".to_string() });
    assert_eq!(fields.remove("inner.regex").unwrap(), FieldInformation { default: true, indexed: true, stored: false, mapping: "wildcard".to_string() });
    assert!(fields.is_empty());
}