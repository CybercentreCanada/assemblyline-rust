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
pub enum PostgresTypes {
    Timestamp,
    Boolean,
    SmallInt,
    Int,
    BigInt,
    // Uuid,
    Char(usize),
    Enum(&'static str),
    Text,
    TextArrayInvert,
    TextInvert,
    TextTrigram,
    // JsonInverse,
    BigSerial,
    Float,
    Double,
}

impl PostgresTypes {
    pub fn type_string(&self) -> String {
        match self {
            PostgresTypes::Timestamp => "timestamp with time zone".to_owned(),
            PostgresTypes::Boolean => "boolean".to_owned(),
            PostgresTypes::SmallInt => "smallint".to_owned(),
            PostgresTypes::Int => "integer".to_owned(),
            PostgresTypes::BigInt => "bigint".to_owned(),
            // PostgresTypes::Uuid => "uuid".to_owned(),
            PostgresTypes::Text => "text".to_owned(),
            PostgresTypes::Char(len) => format!("char({len})"),
            PostgresTypes::TextTrigram => "text".to_owned(),
            PostgresTypes::TextArrayInvert => "text[]".to_owned(),
            PostgresTypes::TextInvert => "text".to_owned(),
            // PostgresTypes::JsonInverse => "jsonb".to_owned(),
            PostgresTypes::BigSerial => "bigserial".to_owned(),
            PostgresTypes::Float => "real".to_owned(),
            PostgresTypes::Double => "double precision".to_owned(),
            PostgresTypes::Enum(name) => name.to_string(),
        }
    }

    pub fn generated(&self) -> bool {
        matches!(self, PostgresTypes::BigSerial)
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub optional: bool,
    pub extraction: Option<Vec<String>>,
    pub kind: PostgresTypes,
}

// impl Field {
//     pub fn extract(&self, data: &Value) -> Option<String> {
//         self._extract(self.extraction.as_ref()?, data)
//     }

//     fn _extract(&self, path: &[String], data: &Value) -> Option<String> {
//         if path.is_empty() {
//             match self.kind {
//                 PostgresTypes::Timestamp
//                 | PostgresTypes::SmallInt
//                 | PostgresTypes::Int
//                 | PostgresTypes::BigInt => {
//                     Some(data.as_number()?.as_i64()?.to_string())
//                 }
//                 PostgresTypes::Float | PostgresTypes::Double => {
//                     Some(data.as_number()?.as_f64()?.to_string())
//                 }
//                 PostgresTypes::Boolean => {
//                     Some(data.as_bool()?.to_string())
//                 },
//                 // PostgresTypes::Uuid => todo!(),
//                 PostgresTypes::Enum(_) => todo!(),
//                 PostgresTypes::Char(_) => todo!(),
//                 PostgresTypes::Text => todo!(),
//                 PostgresTypes::TextArrayInvert => todo!(),
//                 PostgresTypes::TextInvert => todo!(),
//                 PostgresTypes::TextTrigram => todo!(),
//                 // PostgresTypes::JsonInverse => todo!(),
//                 PostgresTypes::BigSerial => None,
//             }
//         } else {
//             self._extract(&path[1..], data.get(&path[0])?)
//         }
//     }
// }

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

    pub fn extract_unindexed_field(&mut self, name: String, path: &[&str], kind: PostgresTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: Some(path.iter().map(|r| r.to_string()).collect()),
            optional: false,
            kind,
        });
        self.fields.len() - 1
    }

    pub fn extract_default_field(&mut self, name: String, path: &[&str], kind: PostgresTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: Some(path.iter().map(|r| r.to_string()).collect()),
            optional: false,
            kind,
        });
        self.indices.push(Index::Default(name));
        self.fields.len() - 1
    }

    pub fn extract_index_field(&mut self, name: String, path: &[&str], kind: PostgresTypes, index: String) -> usize {
        self.fields.push(Field {
            name,
            extraction: Some(path.iter().map(|r| r.to_string()).collect()),
            optional: false,
            kind,
        });
        self.indices.push(Index::Custom(index));
        self.fields.len() - 1
    }

    pub fn add_unindexed_field(&mut self, name: String, kind: PostgresTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: None,
            optional: false,
            kind,
        });
        self.fields.len() - 1
    }

    pub fn add_default_field(&mut self, name: String, kind: PostgresTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            extraction: None,
            optional: false,
            kind,
        });
        self.indices.push(Index::Default(name));
        self.fields.len() - 1
    }

    pub fn add_index_field(&mut self, name: String, kind: PostgresTypes, index: String) -> usize {
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

    pub fn create_table_command(&self) -> (String, Vec<String>) {
        let mut fields = vec![];
        let mut indices = vec![];
        // let mut primary = None;

        for field in &self.fields {
            let mut string = field.name.clone();
            string += " ";
            string += &field.kind.type_string();

            if !field.optional {
                string += " NOT NULL";
            }

            fields.push(string);
        }

        let primary = match &self.primary {
            Index::Custom(custom) => custom.clone(),
            Index::Default(name) => format!("{name} HASH"),
        };


        for index in &self.indices {
            match index {
                Index::Custom(custom) => {
                    indices.push(format!("CREATE INDEX {0}_{1} ON {0}({custom})", self.name, indices.len()));
                },
                Index::Default(name) => {

                    let field = match self.get_field(name) {
                        Some(field) => field,
                        None => {
                            warn!("Tried to build index on missing field: {name}");
                            continue
                        }
                    };

                    match field.kind {
                        PostgresTypes::BigSerial => {
                            panic!("Serial type used outside of primary key?");
                        }

                        PostgresTypes::Text
                        | PostgresTypes::Char(_)
                        | PostgresTypes::SmallInt
                        | PostgresTypes::Boolean
                        | PostgresTypes::Int
                        | PostgresTypes::BigInt
                        | PostgresTypes::Float
                        | PostgresTypes::Double
                        | PostgresTypes::Timestamp => {
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0}({name} ASC)", self.name));
                        }

                        PostgresTypes::Enum(_) => { // | PostgresTypes::Uuid => {
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0}({name} HASH)", self.name));
                        }

                        PostgresTypes::TextArrayInvert => { //| PostgresTypes::JsonInverse => {
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0} USING ybgin({name})", self.name));
                        }

                        PostgresTypes::TextTrigram => {
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0}({name} ASC)", self.name));
                            indices.push(format!("CREATE INDEX {0}_{name}_tgram ON {0} USING ybgin({name} gin_trgm_ops)", self.name));
                        }

                        PostgresTypes::TextInvert => {
                            fields.push(format!("{name}_vectored tsvector"));
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0} USING ybgin({name}_vectored)", self.name));
                        },
                    }
                }
            }
        }

        let create = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {},\n    PRIMARY KEY({})\n) PARTITION BY RANGE (expiry_ts);",
            self.name, fields.join(",\n    "), primary
        );

        (create, indices)
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
                "ClassificationString" => vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Text)],
                "Sid" => vec![self.extract_index_field(label.to_string(), path, PostgresTypes::Text, "sid HASH".to_owned())],
                "Text" => vec![self.extract_default_field(label.to_string(), path, PostgresTypes::TextInvert)],
                "Sha256" => vec![self.extract_index_field(label.to_string(), path, PostgresTypes::Char(64), format!("{label} HASH"))],
                "MD5" => vec![self.extract_index_field(label.to_string(), path, PostgresTypes::Char(32), format!("{label} HASH"))],
                "Sha1" => vec![self.extract_index_field(label.to_string(), path, PostgresTypes::Char(40), format!("{label} HASH"))],
                "SSDeepHash" => vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Text)],
                _ => todo!(),
            },

            Kind::Enum { name, variants } => {
                match name {
                    "Status" => {
                        if ["response_status"].contains(&label) {
                            vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Enum("error_status"))]
                        } else {
                            todo!("Other enum {label} -> {name}")
                        }
                    },
                    "ErrorTypes" => {
                        vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Enum("error_types"))]
                    },
                    "ErrorSeverity" => {
                        vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Enum("error_severity"))]
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
                        PostgresTypes::TextArrayInvert,
                    )]
                }

                Kind::String => {
                    vec![self.extract_default_field(
                        label.to_string(), path,
                        PostgresTypes::TextArrayInvert,
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
                vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Timestamp)]
            }

            Kind::String => {
                vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Text)]
            }

            Kind::U128 => todo!(),

            Kind::I128 => todo!(),

            Kind::U64 | Kind::I64 => {
                vec![self.extract_default_field(label.to_string(), path, PostgresTypes::BigInt)]
            }

            Kind::U32 | Kind::I32 => {
                vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Int)]
            }

            Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 => {
                vec![self.extract_default_field(label.to_string(), path, PostgresTypes::SmallInt)]
            }

            Kind::F64 => vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Double)],
            Kind::F32 => vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Float)],

            Kind::Bool => {
                vec![self.extract_default_field(label.to_string(), path, PostgresTypes::Boolean)]
            }

            Kind::JSON => todo!(),

            Kind::Any => todo!(),

            _ => todo!(),
        }
    }

    fn classification_fields(&mut self) {
        self.extract_default_field("classification".to_string(), &["classification"], PostgresTypes::Text);
        self.extract_default_field("__access_lvl__".to_string(), &["__access_lvl__"], PostgresTypes::Int);
        self.extract_default_field("__access_req__".to_string(), &["__access_req__"], PostgresTypes::TextArrayInvert);
        self.extract_default_field("__access_grp1__".to_string(), &["__access_grp1__"], PostgresTypes::TextArrayInvert);
        self.extract_default_field("__access_grp2__".to_string(), &["__access_grp2__"], PostgresTypes::TextArrayInvert);
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
        primary: Index::Custom("sid HASH, expiry_ts ASC".to_string()),
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
    table.add_default_field("raw".to_string(), PostgresTypes::TextInvert);

    // insert primary key field
    table.extract_unindexed_field("sid".to_string(), &["sid"], PostgresTypes::Text);

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
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
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
    table.add_default_field("raw".to_string(), PostgresTypes::TextInvert);

    // add indexed field for submission this error is associated with
    table.add_index_field("submission".to_string(), PostgresTypes::Text, "submission HASH".to_owned());

    // insert primary key field
    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

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
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
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
    table.add_default_field("key".to_string(), PostgresTypes::Text);
    table.add_default_field("raw".to_string(), PostgresTypes::TextInvert);
    table.add_index_field("submission".to_string(), PostgresTypes::Text, "submission HASH".to_owned());

    // insert primary key field
    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

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
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
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
    table.add_default_field("raw".to_string(), PostgresTypes::TextInvert);
    table.add_index_field("submission".to_string(), PostgresTypes::Text, "submission HASH".to_owned());

    // insert primary key field
    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

    table
}

#[derive(Serialize, Deserialize)]
pub struct TagRow<'a> {
    // pub id: PostgresTypes::BigSerial);
    pub expiry_ts: Option<DateTime<Utc>>,
    pub submission: &'a str,
    pub result: i64,
    pub key: &'a str,
    pub score: i32,
    pub heuristic: bool,
    pub value: &'a str,
}

pub fn init_tag_table() -> Table {
    let mut table = Table {
        name: ANALYSIS_TAGS_TABLE,
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);
    table.add_unindexed_field("expiry_ts".to_string(), PostgresTypes::Timestamp);
    table.add_index_field("submission".to_string(), PostgresTypes::Text, "submission HASH, key ASC, value ASC".to_string());
    table.extract_index_field("result".to_string(), &["result"], PostgresTypes::BigInt, "result HASH".to_string());
    table.extract_index_field("key".to_string(), &["key"], PostgresTypes::Text, "key ASC, value ASC".to_string());
    table.extract_default_field("score".to_string(), &["score"], PostgresTypes::Int);
    table.extract_default_field("heuristic".to_string(), &["heuristic"], PostgresTypes::Boolean);
    table.extract_default_field("value".to_string(), &["value"], PostgresTypes::TextTrigram);

    table
}

#[derive(Serialize, Deserialize)]
pub struct RelationRow<'a> {
    // pub id: PostgresTypes::BigSerial);
    pub expiry_ts: Option<DateTime<Utc>>,
    pub result: i64,
    pub parent: Cow<'a, str>,
    pub child: Cow<'a, str>,
    pub name: Cow<'a, str>,
    pub relation: Cow<'a, str>,
    pub supplementary: bool,
}

pub fn init_file_relation_table() -> Table {
    let mut table = Table {
        name: ANALYSIS_RELATIONS_TABLE,
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);
    table.add_unindexed_field("expiry_ts".to_string(), PostgresTypes::Timestamp);
    table.extract_index_field("result".to_string(), &["result"], PostgresTypes::BigInt, "result HASH".to_string());
    table.extract_index_field("parent".to_string(), &["parent"], PostgresTypes::Text, "parent HASH, child ASC".to_string());
    table.extract_index_field("child".to_string(), &["child"], PostgresTypes::Text, "child HASH, parent ASC".to_string());
    table.extract_default_field("name".to_string(), &["name"], PostgresTypes::Text);
    table.extract_default_field("supplementary".to_string(), &["supplementary"], PostgresTypes::Boolean);

    table.extract_unindexed_field("relation".to_string(), &["relation"], PostgresTypes::Text);
    table.indices.push(Index::Custom("relation HASH, parent ASC, child ASC".to_string()));
    table.indices.push(Index::Custom("relation HASH, child ASC, parent ASC".to_string()));

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
    // pub id: u64,
    pub submission: String,
    pub key: String,
    pub value: String,
    pub expiry_ts: Option<DateTime<Utc>>,
}


pub fn init_metadata_table() -> Table {
    let mut table = Table {
        name: ANALYSIS_METADATA_TABLE,
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

    let index = table.add_index_field("submission".to_string(),  PostgresTypes::Text, "submission HASH, key ASC, value ASC".to_string());
    table.fields[index].extraction = Some(vec!["submission".to_string()]);

    let index = table.add_index_field("key".to_string(), PostgresTypes::Text, "key ASC, value ASC, submission ASC".to_string());
    table.fields[index].extraction = Some(vec!["key".to_string()]);

    let index = table.add_default_field("value".to_string(), PostgresTypes::TextTrigram);
    table.fields[index].extraction = Some(vec!["value".to_string()]);

    table.add_unindexed_field("expiry_ts".to_string(), PostgresTypes::Timestamp);

    table
}

pub async fn init_database_tables(client: Yugabyte, wipe: bool) -> Result<()> {

    // register types
    client.register_type::<assemblyline_models::datastore::error::StatusDiscriminants>("error_status").await?;
    client.register_type::<assemblyline_models::datastore::error::ErrorTypesDiscriminants>("error_types").await?;
    client.register_type::<assemblyline_models::datastore::error::ErrorSeverityDiscriminants>("error_severity").await?;

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

