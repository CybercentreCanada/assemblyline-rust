use assemblyline_models::ElasticMeta;
use std::collections::HashSet;
use std::sync::OnceLock;

use log::{error, warn};

use struct_metadata::{Described, Entry, Kind, MetadataKind};

use yb_tokio_postgres::{connect, NoTls};

#[derive(Debug, Clone, Copy)]
enum PostgresTypes {
    Timestamp,
    Boolean,
    SmallInt,
    SmallIntHash,
    Int,
    BigInt,
    Uuid,
    Char(usize),
    Text,
    TextArrayInvert,
    TextInvert,
    TextTrigram,
    JsonInverse,
    BigSerial,
    Float,
    Double,
}

impl PostgresTypes {
    pub fn type_string(&self) -> String {
        match self {
            PostgresTypes::Timestamp => "timestamp".to_owned(),
            PostgresTypes::Boolean => "boolean".to_owned(),
            PostgresTypes::SmallInt => "smallint".to_owned(),
            PostgresTypes::SmallIntHash => "smallint".to_owned(),
            PostgresTypes::Int => "integer".to_owned(),
            PostgresTypes::BigInt => "bigint".to_owned(),
            PostgresTypes::Uuid => "uuid".to_owned(),
            PostgresTypes::Text => "text".to_owned(),
            PostgresTypes::Char(len) => format!("char({len})"),
            PostgresTypes::TextTrigram => "text".to_owned(),
            PostgresTypes::TextArrayInvert => "text[]".to_owned(),
            PostgresTypes::TextInvert => "text".to_owned(),
            PostgresTypes::JsonInverse => "jsonb".to_owned(),
            PostgresTypes::BigSerial => "bigserial".to_owned(),
            PostgresTypes::Float => "real".to_owned(),
            PostgresTypes::Double => "double precision".to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
struct Field {
    name: String,
    optional: bool,
    kind: PostgresTypes,
}

#[derive(Debug, Clone)]
enum Index {
    Custom(String),
    Default(String),
}

impl Field {
    // fn new(name: String, kind: PostgresTypes) -> Self {
    //     Self {
    //         name,
    //         optional: false,
    //         kind,
    //         index: FieldIndex::Default,
    //         primary: false,
    //     }
    // }

    // fn new_index(name: String, kind: PostgresTypes, index: String) -> Self {
    //     Self {
    //         name,
    //         optional: false,
    //         kind,
    //         index: FieldIndex::Some(index),
    //         primary: false,
    //     }
    // }

    // fn index(&self, default: &str) -> Option<String> {
    //     match &self.index {
    //         FieldIndex::Some(value) => Some(value.clone()),
    //         FieldIndex::Default => Some(format!("{} {default}",self.name)),
    //         FieldIndex::None => None,
    //     }
    // }
}

#[derive(Debug)]

struct Table {
    name: String,

    primary: Index,
    fields: Vec<Field>,
    indices: Vec<Index>,
}

impl Table {

    pub fn add_unindexed_field(&mut self, name: String, kind: PostgresTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            optional: false,
            kind,
        });
        self.fields.len() - 1
    }

    pub fn add_default_field(&mut self, name: String, kind: PostgresTypes) -> usize {
        self.fields.push(Field {
            name: name.clone(),
            optional: false,
            kind,
        });
        self.indices.push(Index::Default(name));
        self.fields.len() - 1
    }

    pub fn add_index_field(&mut self, name: String, kind: PostgresTypes, index: String) -> usize {
        self.fields.push(Field {
            name,
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
                        | PostgresTypes::Int
                        | PostgresTypes::BigInt
                        | PostgresTypes::Float
                        | PostgresTypes::Double
                        | PostgresTypes::Timestamp => {
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0}({name} ASC)", self.name));
                        }

                        PostgresTypes::Boolean => continue,
                        // TODO do we want to support this without special handling?
                        PostgresTypes::SmallIntHash | PostgresTypes::Uuid => {
                            indices.push(format!("CREATE INDEX {0}_{name} ON {0}({name} HASH)", self.name));
                        }

                        PostgresTypes::TextArrayInvert | PostgresTypes::JsonInverse => {
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
        metadata: &ElasticMeta,
        kind: Kind<ElasticMeta>,
        remove: &HashSet<&'static str>,
    ) -> Vec<usize> {
        if remove.contains(label) {
            return vec![];
        }

        println!();
        println!("{} {:?} {:?}", label, metadata.index, kind);

        if !metadata.index.unwrap_or_default() {
            return vec![];
        }

        match kind {
            Kind::Struct { name, children } => {
                let mut entries = vec![];

                for child in children {
                    entries.extend(self.init_field(
                        &format!("{label}_{}", child.label),
                        &child.metadata,
                        child.type_info.kind,
                        remove,
                    ))
                }

                entries
            }

            Kind::Aliased { name, kind } => match name {
                "ClassificationString" => vec![self.add_default_field(label.to_string(), PostgresTypes::Text)],
                "Sid" => vec![self.add_default_field(label.to_string(), PostgresTypes::Uuid)],
                "Text" => vec![self.add_default_field(label.to_string(), PostgresTypes::TextInvert)],
                "Sha256" => vec![self.add_index_field(label.to_string(), PostgresTypes::Char(32), format!("{label} HASH"))],
                "MD5" => vec![self.add_index_field(label.to_string(), PostgresTypes::Char(16), format!("{label} HASH"))],
                "Sha1" => vec![self.add_index_field(label.to_string(), PostgresTypes::Char(20), format!("{label} HASH"))],
                "SSDeepHash" => vec![self.add_default_field(label.to_string(), PostgresTypes::Text)],
                _ => todo!(),
            },

            Kind::Enum { name, variants } => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::SmallIntHash)]
            }

            Kind::Sequence(descriptor) => match descriptor.kind {
                Kind::Aliased {name: "UpperString", kind} => {
                    vec![self.add_default_field(
                        label.to_string(),
                        PostgresTypes::TextArrayInvert,
                    )]
                }

                Kind::String => {
                    vec![self.add_default_field(
                        label.to_string(),
                        PostgresTypes::TextArrayInvert,
                    )]
                }

                _ => todo!(),
            },

            Kind::Option(descriptor) => {
                let entries = self.init_field(label, metadata, descriptor.kind, remove);

                for entry in &entries {
                    self.fields[*entry].optional = true;
                }

                entries
            }

            Kind::Mapping(descriptor, descriptor1) => todo!(),

            Kind::DateTime => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::Timestamp)]
            }

            Kind::String => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::Text)]
            }

            Kind::U128 => todo!(),

            Kind::I128 => todo!(),

            Kind::U64 | Kind::I64 => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::BigInt)]
            }

            Kind::U32 | Kind::I32 => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::Int)]
            }

            Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::SmallInt)]
            }

            Kind::F64 => vec![self.add_default_field(label.to_string(), PostgresTypes::Double)],
            Kind::F32 => vec![self.add_default_field(label.to_string(), PostgresTypes::Float)],

            Kind::Bool => {
                vec![self.add_default_field(label.to_string(), PostgresTypes::Boolean)]
            }

            Kind::JSON => todo!(),

            Kind::Any => todo!(),

            _ => todo!(),
        }
    }

    fn classification_fields(&mut self) {
        self.add_default_field("classification".to_string(), PostgresTypes::Text);
        self.add_default_field("__access_lvl__".to_string(), PostgresTypes::Int);
        self.add_default_field("__access_req__".to_string(), PostgresTypes::TextArrayInvert);
        self.add_default_field("__access_grp1__".to_string(), PostgresTypes::TextArrayInvert);
        self.add_default_field("__access_grp2__".to_string(), PostgresTypes::TextArrayInvert);
    }
}



fn init_submission_table() -> Table {
    let meta = assemblyline_models::datastore::Submission::metadata();

    let Kind::Struct { name, children } = meta.kind else {
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
        name: "submission".to_string(),
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
    table.add_unindexed_field("sid".to_string(), PostgresTypes::Uuid);

    println!("submission {} fields", table.fields.len());
    table
}

fn init_error_table() -> Table {
    let meta = assemblyline_models::datastore::Error::metadata();

    let Kind::Struct { name, children } = meta.kind else {
        panic!()
    };

    static SUBMISSION_REMOVED: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let removed = SUBMISSION_REMOVED.get_or_init(|| {
        let mut removed = HashSet::new();
        removed.insert("archive_ts");
        // removed.insert("files");
        // removed.insert("metadata");
        // removed.insert("results");
        // removed.insert("classification");
        // removed.insert("sid");
        removed
    });

    let mut table = Table {
        name: "error".to_string(),
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
            &child.metadata,
            child.type_info.kind,
            &Default::default(),
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
    table.add_default_field("submission".to_string(), PostgresTypes::Uuid);

    // insert primary key field
    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

    println!("error {} fields", table.fields.len());
    table
}

fn init_result_table() -> Table {
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
        name: "result".to_string(),
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    for child in children {
        table.init_field(
            child.label,
            &child.metadata,
            child.type_info.kind,
            &remove,
        );
    }

    // insert classification fields
    table.classification_fields();

    // insert raw fields
    table.add_default_field("raw".to_string(), PostgresTypes::TextInvert);
    table.add_default_field("submission".to_string(), PostgresTypes::Uuid);

    // insert primary key field
    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

    println!("result {} fields", table.fields.len());
    table
}

fn init_file_table() -> Table {
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
        name: "file".to_string(),
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    for child in children {
        table.init_field(
            child.label,
            &child.metadata,
            child.type_info.kind,
            remove,
        );
    }

    // insert classification fields
    table.classification_fields();

    // insert raw fields
    table.add_default_field("raw".to_string(), PostgresTypes::TextInvert);
    table.add_default_field("submission".to_string(), PostgresTypes::Uuid);

    // insert primary key field
    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);

    println!("file {} fields", table.fields.len());
    table
}

fn init_tag_table() -> Table {
    let mut table = Table {
        name: "metadata".to_owned(),
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);
    table.add_unindexed_field("expiry_ts".to_string(), PostgresTypes::Timestamp);
    table.add_index_field("submission".to_string(), PostgresTypes::Uuid, "submission HASH, key ASC, value ASC".to_string());
    table.add_index_field("result".to_string(), PostgresTypes::Text, "result HASH".to_string());
    table.add_index_field("key".to_string(), PostgresTypes::Text, "key ASC, value ASC".to_string());
    table.add_default_field("value".to_string(), PostgresTypes::TextTrigram);

    table
}

fn init_file_relation_table() -> Table {
    let mut table = Table {
        name: "relations".to_owned(),
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);
    table.add_unindexed_field("expiry_ts".to_string(), PostgresTypes::Timestamp);
    table.add_index_field("result".to_string(), PostgresTypes::BigInt, "result HASH".to_string());
    table.add_index_field("parent".to_string(), PostgresTypes::Text, "parent HASH, child ASC".to_string());
    table.add_index_field("child".to_string(), PostgresTypes::Text, "child HASH, parent ASC".to_string());
    table.add_default_field("name".to_string(), PostgresTypes::Text);

    table.add_unindexed_field("relation".to_string(), PostgresTypes::Text);
    table.indices.push(Index::Custom("relation HASH, parent ASC, child ASC".to_string()));
    table.indices.push(Index::Custom("relation HASH, child ASC, parent ASC".to_string()));

    table
}

// #[derive(Debug, Default, Described)]
// #[metadata_type(PostgresMetadata)]
// struct MetadataRow {
//     #[metadata(class=PostgresTypes::BigSerial, index="id HASH, expiry_ts ASC", primary=true)]
//     id: u64,
//     #[metadata(index="submission HASH, key ASC, value ASC")]
//     submission: u128,
//     #[metadata(index="submission HASH, key ASC, value ASC")]
//     key: String,
//     #[metadata(index="submission HASH, key ASC, value ASC")]
//     key: String,
// }

fn init_metadata_tag_table() -> Table {
    let mut table = Table {
        name: "metadata".to_owned(),
        fields: vec![],
        primary: Index::Custom("id HASH, expiry_ts ASC".to_string()),
        indices: vec![],
    };

    table.add_unindexed_field("id".to_string(), PostgresTypes::BigSerial);
    table.add_index_field("submission".to_string(),  PostgresTypes::Uuid, "submission HASH, key ASC, value ASC".to_string());
    table.add_index_field("key".to_string(), PostgresTypes::Text, "key ASC, value ASC, submission ASC".to_string());
    table.add_default_field("value".to_string(), PostgresTypes::TextTrigram);
    table.add_unindexed_field("expiry_ts".to_string(), PostgresTypes::Timestamp);

    table
}

pub async fn main() {
    // let connection_url: String = String::from("postgresql://9dc62c296bdc:5433/yugabyte?user=yugabyte&password=yugabyte&load_balance=true");
    let connection_url = String::from("postgresql://localhost:5433/yugabyte?user=yugabyte&password=yugabyte");
    println!("Database connecting...");
    let (client, connection) = connect(&connection_url, NoTls).await.unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {e}");
        }
    });

    println!("Enabling database extensions...");
    client.execute("CREATE extension IF NOT EXISTS pg_trgm", &[]).await.unwrap();
    println!("Database ready");

    let tables = vec![
        init_submission_table(),
        init_metadata_tag_table(),
        init_error_table(),
        init_result_table(),
        init_file_table(),
        init_tag_table(),
        init_file_relation_table(),
    ];

    for table in tables {
        println!("Creating table {} ...", table.name);

        let (create_table, create_indices) = table.create_table_command();

        println!("{create_table}");

        for index in &create_indices {
            println!("{index}");
        }

        client.execute(&format!("drop table if exists {}", table.name), &[]).await.unwrap();
        client.execute(&create_table, &[]).await.unwrap();

        for create_index in create_indices {
            client.execute(&create_index, &[]).await.unwrap();
        }
    }

    todo!("impl enums");
    todo!("normalize some fields on index, update, or search");
    todo!("add index to result for submission, hash combo");
}
