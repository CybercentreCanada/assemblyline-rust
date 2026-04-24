use assemblyline_models::ElasticMeta;
use std::collections::HashSet;
use std::sync::OnceLock;

use log::warn;

use struct_metadata::{Described, Entry, Kind, MetadataKind};

use yb_postgres::{Client, NoTls};

#[derive(Debug)]

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
        }
    }
}

#[derive(Debug)]
struct Field {
    name: String,
    optional: bool,
    primary: bool,
    kind: PostgresTypes,
    index: FieldIndex,
}

#[derive(Debug)]

enum FieldIndex {
    Some(String),
    None,
    Default,
}

impl Field {
    fn new(name: String, kind: PostgresTypes) -> Self {
        Self {
            name,
            optional: false,
            kind,
            index: FieldIndex::Default,
            primary: false,
        }
    }

    fn index(&self, default: &str) -> Option<String> {
        match &self.index {
            FieldIndex::Some(value) => Some(value.clone()),
            FieldIndex::Default => Some(format!("{} {default}",self.name)),
            FieldIndex::None => None,
        }
    }
}

#[derive(Debug)]

struct Table {
    name: String,

    // primary: String,
    fields: Vec<Field>,
}

impl Table {
    pub fn create_table_command(&self) -> (String, Vec<String>) {
        let mut fields = vec![];

        let mut indices = vec![];

        let mut primary = None;

        for field in &self.fields {
            let mut string = field.name.clone();

            string += " ";

            string += &field.kind.type_string();

            if !field.optional {
                string += " NOT NULL";
            }

            fields.push(string);

            if field.primary {
                primary = Some(match &field.index {
                    FieldIndex::Some(index) => index.clone(),

                    _ => format!(
                        "{} HASH",
                        field.name
                    ),
                });
            } else {
                match field.kind {
                    PostgresTypes::BigSerial => {
                        panic!(
                            "Serial type used outside of primary key?"
                        );
                    }

                    PostgresTypes::Text
                    | PostgresTypes::Char(_)
                    | PostgresTypes::SmallInt
                    | PostgresTypes::Int
                    | PostgresTypes::BigInt
                    | PostgresTypes::Timestamp => {
                        if let Some(index) = field.index("ASC") {
                            indices.push(format!("CREATE INDEX {}_{} ON {}({})",
                                                 self.name, field.name, self.name, index));
                        }
                    }

                    PostgresTypes::Boolean => continue,
                    // TODO do we want to support this without special handling?
                    PostgresTypes::SmallIntHash | PostgresTypes::Uuid => {
                        if let Some(index) = field.index("hash") {
                            indices.push(format!(
                                "CREATE
 INDEX {}_{}
 ON {}({})",
                                self.name, field.name, self.name, index
                            ));
                        }
                    }

                    PostgresTypes::TextArrayInvert | PostgresTypes::JsonInverse => {
                        if field.index("").is_some() {
                            warn!(
                                "Ignoring the index setting
 for: {}/{}",
                                self.name, field.name
                            );
                        }

                        indices.push(format!(
                            "CREATE
 INDEX {}_{}
 ON {} USING ybgin({})",
                            self.name, field.name, self.name, field.name
                        ));
                    }

                    PostgresTypes::TextTrigram => {
                        if let Some(index) = field.index("ASC") {
                            indices.push(format!(
                                "CREATE
 INDEX {}_{}
 ON {}({})",
                                self.name, field.name, self.name, index
                            ));
                        }

                        indices.push(format!(
                            "CREATE
 INDEX {}_{}_tgram
 ON {} USING ybgin({}
 gin_trgm_ops)",
                            self.name, field.name, self.name, field.name
                        ));
                    }

                    PostgresTypes::TextInvert => {
                        if field.index("").is_some() {
                            warn!(
                                "Ignoring the index setting
 for: {}/{}",
                                self.name, field.name
                            );
                        }

                        fields.push(format!(
                            "{}_vectored
 tsvector",
                            field.name
                        ));

                        indices.push(format!(
                            "CREATE
 INDEX {}_{}
 ON {} USING ybgin({}_vectored)",
                            self.name, field.name, self.name, field.name
                        ))
                    }
                }
            }
        }

        let primary = primary.unwrap();

        let create = format!(
            "CREATE TABLE IF NOT EXISTS
{} (\n    {},\n
    PRIMARY KEY({})\n) PARTITION BY RANGE (expiry_ts);",
            self.name,
            fields.join(
                ",\n
    "
            ),
            primary
        );

        (create, indices)
    }
}
fn init_field(
    label: &str,
    metadata: &ElasticMeta,
    kind: Kind<ElasticMeta>,
    remove: &HashSet<&'static str>,
) -> Vec<Field> {
    if remove.contains(label) {
        return vec![];
    }

    println!();

    println!(
        "{}
{:?}
{:?}",
        label, metadata.index, kind
    );

    if !metadata.index.unwrap_or_default() {
        return vec![];
    }

    match kind {
        Kind::Struct { name, children } => {
            let mut entries = vec![];

            for child in children {
                entries.extend(init_field(
                    &format!("{label}_{}", child.label),
                    &child.metadata,
                    child.type_info.kind,
                    remove,
                ))
            }

            entries
        }

        Kind::Aliased { name, kind } => match name {
            "ClassificationString" => {
                vec![Field::new(label.to_string(), PostgresTypes::Text)]
            }

            "Sid" => {
                vec![Field::new(label.to_string(), PostgresTypes::Uuid)]
            }

            "Text" => {
                vec![Field::new(label.to_string(), PostgresTypes::TextInvert)]
            }

            "Sha256" => {
                vec![Field::new(label.to_string(), PostgresTypes::Char(32))]
            }

            _ => {
                todo!()
            }
        },

        Kind::Enum { name, variants } => {
            vec![Field::new(label.to_string(), PostgresTypes::SmallIntHash)]
        }

        Kind::Sequence(descriptor) => match descriptor.kind {
            Kind::Aliased {
                name: "UpperString",
                kind,
            } => {
                vec![Field::new(
                    label.to_string(),
                    PostgresTypes::TextArrayInvert,
                )]
            }

            Kind::String => {
                vec![Field::new(
                    label.to_string(),
                    PostgresTypes::TextArrayInvert,
                )]
            }

            _ => todo!(),
        },

        Kind::Option(descriptor) => {
            let mut entries = init_field(label, metadata, descriptor.kind, remove);

            for entry in &mut entries {
                entry.optional = true;
            }

            entries
        }

        Kind::Mapping(descriptor, descriptor1) => todo!(),

        Kind::DateTime => {
            vec![Field::new(label.to_string(), PostgresTypes::Timestamp)]
        }

        Kind::String => {
            vec![Field::new(label.to_string(), PostgresTypes::Text)]
        }

        Kind::U128 => todo!(),

        Kind::I128 => todo!(),

        Kind::U64 | Kind::I64 => {
            vec![Field::new(label.to_string(), PostgresTypes::BigInt)]
        }

        Kind::U32 | Kind::I32 => {
            vec![Field::new(label.to_string(), PostgresTypes::Int)]
        }

        Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 => {
            vec![Field::new(label.to_string(), PostgresTypes::SmallInt)]
        }

        Kind::F64 => todo!(),

        Kind::F32 => todo!(),

        Kind::Bool => {
            vec![Field::new(label.to_string(), PostgresTypes::Boolean)]
        }

        Kind::JSON => todo!(),

        Kind::Any => todo!(),

        _ => todo!(),
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

        removed.insert("errors");

        removed.insert("files");

        removed.insert("metadata");

        removed.insert("results");

        removed.insert("classification");

        removed.insert("sid");

        removed
    });

    let mut fields = vec![];

    for child in children {
        if removed.contains(child.label) {
            continue;
        }

        fields.extend(init_field(
            child.label,
            &child.metadata,
            child.type_info.kind,
            &Default::default(),
        ));
    }

    // insert classification fields

    fields.extend(classification_fields());

    // insert raw fields

    fields.push(Field {
        name: "raw".to_string(),

        optional: false,

        primary: false,

        kind: PostgresTypes::TextInvert,

        index: FieldIndex::Default,
    });

    // insert primary key field

    fields.push(Field {
        name: "sid".to_string(),

        optional: false,

        primary: true,

        kind: PostgresTypes::Uuid,

        index: FieldIndex::Some("sid HASH, expiry_ts ASC".to_string()),
    });

    println!(
        "submission
{} fields",
        fields.len()
    );

    Table {
        name: "submission".to_string(),

        fields,
    }
}

fn init_error_table() -> Table {
    let meta = assemblyline_models::datastore::Error::metadata();

    let Kind::Struct { name, children } = meta.kind else {
        panic!()
    };

    static SUBMISSION_REMOVED: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let removed = SUBMISSION_REMOVED.get_or_init(|| {
        let mut removed = HashSet::new();

        // removed.insert("errors");

        // removed.insert("files");

        // removed.insert("metadata");

        // removed.insert("results");

        // removed.insert("classification");

        // removed.insert("sid");

        removed
    });

    let mut fields = vec![];

    for child in children {
        if removed.contains(child.label) {
            continue;
        }

        fields.extend(init_field(
            child.label,
            &child.metadata,
            child.type_info.kind,
            &Default::default(),
        ));
    }

    // insert classification fields

    fields.extend(classification_fields());

    // insert raw fields

    fields.push(Field {
        name: "raw".to_string(),

        optional: false,

        primary: false,

        kind: PostgresTypes::TextInvert,

        index: FieldIndex::Default,
    });

    fields.push(Field {
        name: "submission".to_string(),

        optional: false,

        kind: PostgresTypes::Uuid,

        index: FieldIndex::Default,

        primary: false,
    });

    // insert primary key field

    fields.push(Field {
        name: "id".to_string(),

        optional: false,

        kind: PostgresTypes::BigSerial,

        index: FieldIndex::Some("id HASH, expiry_ts ASC".to_string()),

        primary: true,
    });

    println!(
        "error
{} fields",
        fields.len()
    );

    Table {
        name: "error".to_string(),

        fields,
    }
}

fn init_result_table() -> Table {
    let meta = assemblyline_models::datastore::Result::metadata();

    let Kind::Struct { name, children } = meta.kind else {
        panic!()
    };

    static RESULT_REMOVE: OnceLock<HashSet<&'static str>> = OnceLock::new();

    let remove = RESULT_REMOVE.get_or_init(|| {
        let mut removed = HashSet::new();

        removed.insert("response_supplementary");

        // removed.insert("files");

        // removed.insert("metadata");

        // removed.insert("results");

        // removed.insert("classification");

        // removed.insert("sid");

        removed
    });

    let mut fields = vec![];

    for child in children {
        fields.extend(init_field(
            child.label,
            &child.metadata,
            child.type_info.kind,
            &remove,
        ));
    }

    // insert classification fields

    fields.extend(classification_fields());

    // insert raw fields

    fields.push(Field {
        name: "raw".to_string(),

        optional: false,

        primary: false,

        kind: PostgresTypes::TextInvert,

        index: FieldIndex::Default,
    });

    fields.push(Field {
        name: "submission".to_string(),

        optional: false,

        kind: PostgresTypes::Uuid,

        index: FieldIndex::Default,

        primary: false,
    });

    // insert primary key field

    fields.push(Field {
        name: "id".to_string(),

        optional: false,

        kind: PostgresTypes::BigSerial,

        index: FieldIndex::Some("id HASH, expiry_ts ASC".to_string()),

        primary: true,
    });

    println!(
        "result
{} fields",
        fields.len()
    );

    Table {
        name: "result".to_string(),

        fields,
    }
}

fn init_file_table() -> Table {
    todo!()
}

fn init_tag_table() -> Table {
    todo!()
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
    let fields = vec![
        Field {
            name: "id".to_string(),

            optional: false,

            kind: PostgresTypes::BigSerial,

            index: FieldIndex::Some(
                "id
 HASH, expiry_ts ASC"
                    .to_string(),
            ),

            primary: true,
        },
        Field {
            name: "submission".to_string(),

            optional: false,

            kind: PostgresTypes::Uuid,

            index: FieldIndex::Some(
                "submission
 HASH, key ASC, value ASC"
                    .to_string(),
            ),

            primary: false,
        },
        Field {
            name: "key".to_string(),

            optional: false,

            kind: PostgresTypes::Text,

            index: FieldIndex::Some(
                "key
 ASC, value ASC"
                    .to_string(),
            ),

            primary: false,
        },
        Field {
            name: "value".to_string(),

            optional: false,

            kind: PostgresTypes::TextTrigram,

            index: FieldIndex::Default,

            primary: false,
        },
        Field {
            name: "expiry_ts".to_string(),

            optional: false,

            kind: PostgresTypes::Timestamp,

            index: FieldIndex::None,

            primary: false,
        },
    ];

    Table {
        name: "metadata".to_owned(),
        fields,
    }
}

fn main() {
    // let connection_url: String = String::from("postgresql://9dc62c296bdc:5433/yugabyte?user=yugabyte&password=yugabyte&load_balance=true");

    let connection_url =
        String::from("postgresql://localhost:5433/yugabyte?user=yugabyte&password=yugabyte");

    let mut client = Client::connect(&connection_url, NoTls).unwrap();

    client
        .execute(
            "CREATE
 extension IF NOT EXISTS pg_trgm",
            &[],
        )
        .unwrap();

    let tables = vec![
        init_submission_table(),
        init_metadata_tag_table(),
        init_error_table(),
    ];

    for table in tables {
        println!("{table:?}");

        let (create_table, create_indices) = table.create_table_command();

        println!("{create_table}");

        for index in &create_indices {
            println!("{index}");
        }

        client
            .execute(
                &format!(
                    "drop
 table if exists {}",
                    table.name
                ),
                &[],
            )
            .unwrap();

        client.execute(&create_table, &[]).unwrap();

        for create_index in create_indices {
            client.execute(&create_index, &[]).unwrap();
        }
    }

    init_result_table();

    init_file_table();

    init_tag_table();

    todo!("impl enums");
}

// #[derive(Debug, Default, MetadataKind)]

// struct PostgresMetadata {

//     class: PostgresTypes,

//     index: Option<&'static str>,

//     primary: bool,

// }
fn classification_fields() -> Vec<Field> {
    vec![
        Field::new("classification".to_string(), PostgresTypes::Text),
        Field::new("__access_lvl__".to_string(), PostgresTypes::Int),
        Field::new("__access_req__".to_string(), PostgresTypes::TextArrayInvert),
        Field::new(
            "__access_grp1__".to_string(),
            PostgresTypes::TextArrayInvert,
        ),
        Field::new(
            "__access_grp2__".to_string(),
            PostgresTypes::TextArrayInvert,
        ),
    ]
}
