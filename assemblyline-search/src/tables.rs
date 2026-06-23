use assemblyline_models::ElasticMeta;
use assemblyline_models::types::Sid;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::OnceLock;
use anyhow::Result;

use log::{error, warn};

use struct_metadata::{Described, Entry, Kind, MetadataKind};

use crate::titanium::Titanium;
// use crate::titanium::Titanium;
use crate::yugabyte::Yugabyte;

pub const ANALYSIS_SUBMISSIONS_TABLE: &str = "analysis_submissions";
pub const ANALYSIS_RESULTS_TABLE: &str = "analysis_results";
pub const ANALYSIS_ERRORS_TABLE: &str = "analysis_errors";
pub const ANALYSIS_METADATA_TABLE: &str = "analysis_metadata";
pub const ANALYSIS_TAGS_TABLE: &str = "analysis_tags";
pub const ANALYSIS_RELATIONS_TABLE: &str = "analysis_relations";
pub const ANALYSIS_FILES_TABLE: &str = "analysis_files";


pub const ALL_ANALYSIS_TABLES: [&str; 7] = [
    ANALYSIS_SUBMISSIONS_TABLE,
    ANALYSIS_RESULTS_TABLE,
    ANALYSIS_ERRORS_TABLE,
    ANALYSIS_METADATA_TABLE,
    ANALYSIS_TAGS_TABLE,
    ANALYSIS_RELATIONS_TABLE,
    ANALYSIS_FILES_TABLE,
];


#[derive(Debug, Clone, Copy)]
pub enum TableTypes {
    Timestamp,
    Boolean,
    SmallInt,
    Int,
    BigInt,
    Char(usize),
    Enum(&'static str),
    Text,
    TextArrayInvert,
    TextInvert,
    TextTrigram,
    // RandomId,
    Id,
    Float,
    Double,
}

impl TableTypes {
    pub fn postgres_type_string(&self) -> String {
        match self {
            TableTypes::Timestamp => "timestamp with time zone".to_owned(),
            TableTypes::Boolean => "boolean".to_owned(),
            TableTypes::SmallInt => "smallint".to_owned(),
            TableTypes::Int => "integer".to_owned(),
            TableTypes::BigInt => "bigint".to_owned(),
            // PostgresTypes::Uuid => "uuid".to_owned(),
            TableTypes::Text => "text".to_owned(),
            TableTypes::Char(len) => format!("char({len})"),
            TableTypes::TextTrigram => "text".to_owned(),
            TableTypes::TextArrayInvert => "text[]".to_owned(),
            TableTypes::TextInvert => "text".to_owned(),
            // PostgresTypes::JsonInverse => "jsonb".to_owned(),
            // TableTypes::RandomId => "uuid DEFAULT uuid_generate_v4()".to_owned(),
            TableTypes::Id => "uuid".to_owned(),
            TableTypes::Float => "real".to_owned(),
            TableTypes::Double => "double precision".to_owned(),
            TableTypes::Enum(name) => name.to_string(),
        }
    }

    pub fn titanium_type_string(&self) -> String {
        match self {
            TableTypes::Timestamp => "datetime".to_owned(),
            TableTypes::Boolean => "boolean".to_owned(),
            TableTypes::SmallInt => "smallint".to_owned(),
            TableTypes::Int => "integer".to_owned(),
            TableTypes::BigInt => "bigint".to_owned(),
            // PostgresTypes::Uuid => "uuid".to_owned(),
            TableTypes::Text => "text".to_owned(),
            TableTypes::Char(len) => format!("char({len})"),
            TableTypes::TextTrigram => "text".to_owned(),
            TableTypes::TextArrayInvert => "json".to_owned(),
            TableTypes::TextInvert => "longblob".to_owned(),
            // PostgresTypes::JsonInverse => "jsonb".to_owned(),
            // TableTypes::RandomId => "binary(16) DEFAULT (uuid_to_bin(uuid()))".to_owned(),
            TableTypes::Id => "binary(16)".to_owned(),
            TableTypes::Float => "real".to_owned(),
            TableTypes::Double => "double precision".to_owned(),
            TableTypes::Enum(name) => format!("enum {name}"),
        }
    }

    // pub fn generated(&self) -> bool {
    //     matches!(self, TableTypes::RandomId)
    // }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub optional: bool,
    pub extraction: Option<Vec<String>>,
    pub kind: TableTypes,
}


#[derive(Debug, Clone)]
pub enum Index {
    Custom(String),
    Default(String),
}

#[derive(Debug)]
pub struct Table {
    pub (crate) name: &'static str,
    pub (crate) primary: Index,
    pub (crate) fields: Vec<Field>,
    pub (crate) indices: Vec<Index>,
}

// struct FieldBuilder<'a> {
//     parent: &'a mut Table,
// }


impl Table {

    pub fn extract_unindexed_field(&mut self, name: String, path: &[&str], kind: TableTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: Some(path.iter().map(|r| r.to_string()).collect()),
            optional: false,
            kind,
        });
        self.fields.len() - 1
    }

    pub fn extract_default_field(&mut self, name: String, path: &[&str], kind: TableTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: Some(path.iter().map(|r| r.to_string()).collect()),
            optional: false,
            kind,
        });
        self.indices.push(Index::Default(name));
        self.fields.len() - 1
    }

    pub fn extract_index_field(&mut self, name: String, path: &[&str], kind: TableTypes, index: String) -> usize {
        self.fields.push(Field {
            name,
            extraction: Some(path.iter().map(|r| r.to_string()).collect()),
            optional: false,
            kind,
        });
        self.indices.push(Index::Custom(index));
        self.fields.len() - 1
    }

    pub fn add_unindexed_field(&mut self, name: String, kind: TableTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: None,
            optional: false,
            kind,
        });
        self.fields.len() - 1
    }

    pub fn add_default_field(&mut self, name: String, kind: TableTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: None,
            optional: false,
            kind,
        });
        self.indices.push(Index::Default(name));
        self.fields.len() - 1
    }

    pub fn add_index_field(&mut self, name: String, kind: TableTypes, index: String) -> usize {
        self.fields.push(Field {
            name,
            extraction: None,
            optional: false,
            kind,
        });
        self.indices.push(Index::Custom(index));
        self.fields.len() - 1
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        for field in &self.fields {
            if field.name == name {
                return Some(field)
            }
        }
        None
    }

    fn init_field(
        &mut self,
        label: &str,
        path: &[&str],
        metadata: &ElasticMeta,
        kind: Kind<ElasticMeta>,
        remove: &HashSet<&'static str>,
    ) -> Vec<usize> {
        if remove.contains(label) {
            return vec![];
        }

        if !metadata.index.unwrap_or_default() {
            return vec![];
        }

        match kind {
            Kind::Struct { name, children } => {
                let mut entries = vec![];

                for child in children {
                    let mut path = path.to_vec();
                    path.push(child.label);
                    entries.extend(self.init_field(
                        &format!("{label}_{}", child.label),
                        &path,
                        &child.metadata,
                        child.type_info.kind,
                        remove,
                    ))
                }

                entries
            }

            Kind::Aliased { name, kind } => match name {
                "ClassificationString" => vec![self.extract_default_field(label.to_string(), path, TableTypes::Text)],
                "Sid" => vec![self.extract_index_field(label.to_string(), path, TableTypes::Text, "sid".to_owned())], // HASH
                "Text" => vec![self.extract_default_field(label.to_string(), path, TableTypes::TextInvert)],
                "Sha256" => vec![self.extract_index_field(label.to_string(), path, TableTypes::Char(64), format!("{label}"))], // HASH
                "MD5" => vec![self.extract_index_field(label.to_string(), path, TableTypes::Char(32), format!("{label}"))], // HASH
                "Sha1" => vec![self.extract_index_field(label.to_string(), path, TableTypes::Char(40), format!("{label}"))], // HASH
                "SSDeepHash" => vec![self.extract_default_field(label.to_string(), path, TableTypes::Text)],
                _ => todo!(),
            },

            Kind::Enum { name, variants } => {
                match name {
                    "Status" => {
                        if ["response_status"].contains(&label) {
                            vec![self.extract_default_field(label.to_string(), path, TableTypes::Enum("error_status"))]
                        } else {
                            todo!("Other enum {label} -> {name}")
                        }
                    },
                    "ErrorTypes" => {
                        vec![self.extract_default_field(label.to_string(), path, TableTypes::Enum("error_types"))]
                    },
                    "ErrorSeverity" => {
                        vec![self.extract_default_field(label.to_string(), path, TableTypes::Enum("error_severity"))]
                    }
                    _ => {
                        todo!("Other enum {label} -> {name}")
                    }
                }
            }

            Kind::Sequence(descriptor) => match descriptor.kind {
                Kind::Aliased {name: "UpperString", kind} => {
                    vec![self.extract_default_field(
                        label.to_string(), path,
                        TableTypes::TextArrayInvert,
                    )]
                }

                Kind::String => {
                    vec![self.extract_default_field(
                        label.to_string(), path,
                        TableTypes::TextArrayInvert,
                    )]
                }

                _ => todo!(),
            },

            Kind::Option(descriptor) => {
                let entries = self.init_field(label, path, metadata, descriptor.kind, remove);

                for entry in &entries {
                    self.fields[*entry].optional = true;
                }

                entries
            }

            Kind::Mapping(descriptor, descriptor1) => todo!(),

            Kind::DateTime => {
                vec![self.extract_default_field(label.to_string(), path, TableTypes::Timestamp)]
            }

            Kind::String => {
                vec![self.extract_default_field(label.to_string(), path, TableTypes::Text)]
            }

            Kind::U128 => todo!(),

            Kind::I128 => todo!(),

            Kind::U64 | Kind::I64 => {
                vec![self.extract_default_field(label.to_string(), path, TableTypes::BigInt)]
            }

            Kind::U32 | Kind::I32 => {
                vec![self.extract_default_field(label.to_string(), path, TableTypes::Int)]
            }

            Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 => {
                vec![self.extract_default_field(label.to_string(), path, TableTypes::SmallInt)]
            }

            Kind::F64 => vec![self.extract_default_field(label.to_string(), path, TableTypes::Double)],
            Kind::F32 => vec![self.extract_default_field(label.to_string(), path, TableTypes::Float)],

            Kind::Bool => {
                vec![self.extract_default_field(label.to_string(), path, TableTypes::Boolean)]
            }

            Kind::JSON => todo!(),

            Kind::Any => todo!(),

            _ => todo!(),
        }
    }

    fn classification_fields(&mut self) {
        self.extract_default_field("classification".to_string(), &["classification"], TableTypes::Text);
        self.extract_default_field("__access_lvl__".to_string(), &["__access_lvl__"], TableTypes::Int);
        self.extract_default_field("__access_req__".to_string(), &["__access_req__"], TableTypes::TextArrayInvert);
        self.extract_default_field("__access_grp1__".to_string(), &["__access_grp1__"], TableTypes::TextArrayInvert);
        self.extract_default_field("__access_grp2__".to_string(), &["__access_grp2__"], TableTypes::TextArrayInvert);
    }
}



pub fn init_submission_table() -> Table {
    let meta = assemblyline_models::datastore::Submission::metadata();

    let Kind::Struct { children, .. } = meta.kind else {
        panic!()
    };

    static SUBMISSION_REMOVED: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let removed = SUBMISSION_REMOVED.get_or_init(|| {
        let mut removed = HashSet::new();
        removed.insert("archived");
        removed.insert("errors");
        removed.insert("files");
        removed.insert("metadata");
        removed.insert("results");
        removed.insert("classification");
        removed.insert("sid");
        removed.insert("to_be_deleted");
        removed.insert("state");
        removed.insert("verdict");
        removed.insert("archive_ts");
        removed
    });

    let mut table = Table {
        name: ANALYSIS_SUBMISSIONS_TABLE,
        fields: vec![],
        primary: Index::Custom("sid ASC, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    for child in children {
        if removed.contains(child.label) {
            continue;
        }

        table.init_field(
            child.label,
            &[child.label],
            &child.metadata,
            child.type_info.kind,
            &Default::default(),
        );
    }

    // insert classification fields
    table.classification_fields();

    // insert raw fields
    table.add_default_field("raw".to_string(), TableTypes::TextInvert);

    // insert primary key field
    table.extract_unindexed_field("sid".to_string(), &["sid"], TableTypes::Id);

    table
}

pub fn init_error_table() -> Table {
    let meta = assemblyline_models::datastore::Error::metadata();

    let Kind::Struct { name, children } = meta.kind else {
        panic!()
    };

    static SUBMISSION_REMOVED: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let removed = SUBMISSION_REMOVED.get_or_init(|| {
        let mut removed = HashSet::new();
        removed.insert("archive_ts");
        removed.insert("response_service_debug_info");
        // removed.insert("files");
        // removed.insert("metadata");
        // removed.insert("results");
        // removed.insert("classification");
        // removed.insert("sid");
        removed
    });

    let mut table = Table {
        name: ANALYSIS_ERRORS_TABLE,
        fields: vec![],
        primary: Index::Custom("sid, key".to_string()),
        indices: vec![],
    };

    for child in children {
        if removed.contains(child.label) {
            continue;
        }

        table.init_field(
            child.label,
            &[child.label],
            &child.metadata,
            child.type_info.kind,
            &removed,
        );
    }

    for field in &mut table.fields {
        if field.name == "response_service_name" || field.name == "response_service_version" {
            field.optional = true;
        }
    }

    // insert classification fields
    table.classification_fields();

    // insert raw fields
    table.add_default_field("raw".to_string(), TableTypes::TextInvert);

    // add indexed field for submission this error is associated with
    table.add_index_field("sid".to_string(), TableTypes::Id, "sid".to_owned());

    // insert primary key field
    table.add_unindexed_field("key".to_string(), TableTypes::Text);

    table
}

pub fn init_result_table() -> Table {
    let meta = assemblyline_models::datastore::Result::metadata();

    let Kind::Struct { name, children } = meta.kind else {
        panic!()
    };

    static RESULT_REMOVE: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let remove = RESULT_REMOVE.get_or_init(|| {
        let mut removed = HashSet::new();
        removed.insert("archive_ts");

        // handled in the file table
        removed.insert("response_supplementary");
        removed.insert("response_extracted");

        // handled as tags and raw indexing
        removed.insert("result_sections");

        // handled explicitly
        removed.insert("classification");

        removed
    });

    let mut table = Table {
        name: ANALYSIS_RESULTS_TABLE,
        fields: vec![],
        primary: Index::Custom("sid, original_key".to_string()),
        indices: vec![],
    };

    for child in children {
        table.init_field(
            child.label,
            &[child.label],
            &child.metadata,
            child.type_info.kind,
            remove,
        );
    }

    // insert classification fields
    table.classification_fields();

    // insert raw fields
    table.add_index_field("original_key".to_string(), TableTypes::Text, "original_key(128)".to_owned());
    table.add_default_field("raw".to_string(), TableTypes::TextInvert);
    table.add_index_field("sid".to_string(), TableTypes::Id, "sid".to_owned());

    table
}

pub fn init_file_table() -> Table {
    let meta = assemblyline_models::datastore::File::metadata();

    let Kind::Struct { name, children } = meta.kind else {
        panic!()
    };

    static RESULT_REMOVE: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let remove = RESULT_REMOVE.get_or_init(|| {
        let mut removed = HashSet::new();
        removed.insert("archive_ts");
        removed.insert("uri_info");
        removed.insert("classification");
        removed.insert("labels");
        removed.insert("label_categories");
        removed.insert("comments");
        removed.insert("from_archive");
        removed
    });

    let mut table = Table {
        name: ANALYSIS_FILES_TABLE,
        fields: vec![],
        primary: Index::Custom("sid, sha256".to_string()),
        indices: vec![],
    };

    for child in children {
        table.init_field(
            child.label,
            &[child.label],
            &child.metadata,
            child.type_info.kind,
            remove,
        );
    }

    // insert classification fields
    table.classification_fields();

    // insert raw fields
    table.add_default_field("raw".to_string(), TableTypes::TextInvert);
    table.add_index_field("sid".to_string(), TableTypes::Id, "sid".to_owned());

    // insert primary key field
    // table.add_unindexed_field("id".to_string(), TableTypes::Text);

    table
}

#[derive(Serialize, Deserialize)]
pub struct TagRow<'a> {
    // pub id: PostgresTypes::BigSerial);
    pub expiry_ts: Option<DateTime<Utc>>,
    pub sid: &'a str,
    pub result: &'a str,
    pub name: &'a str,
    pub score: i32,
    pub heuristic: bool,
    pub value: &'a str,
    pub counter: i32,
}

pub fn init_tag_table() -> Table {
    let mut table = Table {
        name: ANALYSIS_TAGS_TABLE,
        fields: vec![],
        primary: Index::Custom("sid, counter".to_string()),
        indices: vec![],
    };

    table.extract_unindexed_field("counter".to_string(), &["counter"], TableTypes::Int);
    let index = table.add_default_field("expiry_ts".to_string(), TableTypes::Timestamp);
    table.fields[index].optional = true;
    table.extract_index_field("sid".to_string(), &["sid"], TableTypes::Id, "sid, name(64), value(128)".to_string());
    table.extract_index_field("result".to_string(), &["result"], TableTypes::Id, "result".to_string());
    table.extract_index_field("name".to_string(), &["name"], TableTypes::Text, "name(64), value(128)".to_string());
    table.extract_default_field("score".to_string(), &["score"], TableTypes::Int);
    table.extract_default_field("heuristic".to_string(), &["heuristic"], TableTypes::Boolean);
    table.extract_index_field("value".to_string(), &["value"], TableTypes::TextTrigram, "value(128)".to_owned());

    table
}

#[derive(Serialize, Deserialize)]
pub struct RelationRow<'a> {
    // pub id: PostgresTypes::BigSerial);
    pub expiry_ts: Option<DateTime<Utc>>,
    pub result: Cow<'a, str>,
    pub sid: Cow<'a, str>,
    pub parent: Cow<'a, str>,
    pub child: Cow<'a, str>,
    pub name: Cow<'a, str>,
    pub relation: Cow<'a, str>,
    pub supplementary: bool,
    pub counter: i32,
}

pub fn init_file_relation_table() -> Table {
    let mut table = Table {
        name: ANALYSIS_RELATIONS_TABLE,
        fields: vec![],
        primary: Index::Custom("sid, counter, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("counter".to_string(), TableTypes::Int);
    let index = table.add_default_field("expiry_ts".to_string(), TableTypes::Timestamp);
    table.fields[index].optional = true;
    table.extract_index_field("result".to_string(), &["result"], TableTypes::Id, "result".to_string());
    table.extract_index_field("sid".to_string(), &["sid"], TableTypes::Id, "sid".to_string());
    table.extract_index_field("parent".to_string(), &["parent"], TableTypes::Text, "parent(64), child(64) ASC".to_string());
    table.extract_index_field("child".to_string(), &["child"], TableTypes::Text, "child(64), parent(64) ASC".to_string());
    table.extract_index_field("name".to_string(), &["name"], TableTypes::Text, "name(128)".to_string());
    table.extract_default_field("supplementary".to_string(), &["supplementary"], TableTypes::Boolean);

    table.extract_unindexed_field("relation".to_string(), &["relation"], TableTypes::Text);
    table.indices.push(Index::Custom("relation(16), parent(64) ASC, child(64) ASC".to_string()));
    table.indices.push(Index::Custom("relation(16), child(64) ASC, parent(64) ASC".to_string()));

    table
}

// #[derive(Debug, MetadataKind)]
// struct PostgresMetadata {
//     index: Option<&'static str>,
//     class: PostgresTypes,
// }

// impl Default for PostgresMetadata {
//     fn default() -> Self {
//         Self {
//             index: None,
//             class: PostgresTypes::Int
//         }
//     }
// }


// #[derive(Debug, Described)]
// #[metadata_type(PostgresMetadata)]
// struct MetadataRow {
//     #[metadata(class=PostgresTypes::BigSerial)]
//     id: u64,
//     #[metadata(class=PostgresTypes::Text)]
//     submission: String,
//     #[metadata(index="submission HASH, key ASC, value ASC")]
//     key: String,
// }

#[derive(Serialize, Deserialize)]
pub struct MetadataRow {
    pub counter: i32,
    pub sid: String,
    pub name: String,
    pub value: String,
    pub expiry_ts: Option<DateTime<Utc>>,
}


pub fn init_metadata_table() -> Table {
    let mut table = Table {
        name: ANALYSIS_METADATA_TABLE,
        fields: vec![],
        primary: Index::Custom("sid, counter".to_string()),
        indices: vec![],
    };

    table.extract_unindexed_field("counter".to_string(), &["counter"], TableTypes::Int);

    let index = table.add_index_field("sid".to_string(),  TableTypes::Id, "sid, name(32), value(64)".to_string());
    table.fields[index].extraction = Some(vec!["sid".to_string()]);

    let index = table.add_index_field("name".to_string(), TableTypes::Text, "name(32), value(64), sid".to_string());
    table.fields[index].extraction = Some(vec!["name".to_string()]);

    let index = table.add_index_field("value".to_string(), TableTypes::TextTrigram, "value(128)".to_string());
    table.fields[index].extraction = Some(vec!["value".to_string()]);

    let index = table.add_default_field("expiry_ts".to_string(), TableTypes::Timestamp);
    table.fields[index].optional = true;

    table
}

// pub enum Database {
//     Yugabyte(Yugabyte),
//     Ti(Titanium),
// }

// impl Database {
//     pub async fn register_type<Type: strum::IntoEnumIterator + Into<&'static str>>(&self, name: &str) -> Result<()> {
//         match self {
//             Database::Yugabyte(yugabyte) => yugabyte.register_type::<Type>(name).await,
//             Database::Ti(ti) => ti.register_type::<Type>(name).await,
//         }
//     }

//     pub async fn create_table(&self, table: &Table, wipe: bool) -> Result<()> {
//         match self {
//             Database::Yugabyte(yugabyte) => yugabyte.create_table(table, wipe).await,
//             Database::Ti(ti) => ti.create_table(table, wipe).await,
//         }
//     }
// }

pub async fn init_database_tables(client: &Titanium, wipe: bool) -> Result<()> {

    // register types
    // client.register_type::<assemblyline_models::datastore::error::StatusDiscriminants>("error_status").await?;
    // client.register_type::<assemblyline_models::datastore::error::ErrorTypesDiscriminants>("error_types").await?;
    // client.register_type::<assemblyline_models::datastore::error::ErrorSeverityDiscriminants>("error_severity").await?;

    let tables = vec![
        init_submission_table(),
        init_metadata_table(),
        init_error_table(),
        init_result_table(),
        init_file_table(),
        init_tag_table(),
        init_file_relation_table(),
    ];

    for table in tables {
        client.create_table(&table, wipe).await?;
    }

    Ok(())
}
