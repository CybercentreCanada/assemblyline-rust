use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

use anyhow::{Result, bail};
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::{ElasticMeta, datastore};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::{debug, info, warn};
use mysql_async::TxOpts;
use mysql_async::prelude::{Queryable, WithParams};
use parking_lot::Mutex;
use rand::distr::{Alphabetic, SampleString};
use serde::Serialize;
use struct_metadata::{Described, Descriptor};

use crate::tables::{Index, MetadataRow, RelationRow, Table, TableTypes, TagRow, init_error_table, init_file_relation_table, init_file_table, init_metadata_table, init_result_table, init_submission_table, init_tag_table};
// use crate::yugabyte::InsertBuilder;

static SUBMISSION_TABLE: LazyLock<Table> = LazyLock::new(init_submission_table);
static RESULT_TABLE: LazyLock<Table> = LazyLock::new(init_result_table);
static METADATA_TABLE: LazyLock<Table> = LazyLock::new(init_metadata_table);
static TAG_TABLE: LazyLock<Table> = LazyLock::new(init_tag_table);
static FILE_RELATION_TABLE: LazyLock<Table> = LazyLock::new(init_file_relation_table);
static ERROR_TABLE: LazyLock<Table> = LazyLock::new(init_error_table);
static FILE_TABLE: LazyLock<Table> = LazyLock::new(init_file_table);


#[derive(Clone)]
pub struct Titanium {
    client: mysql_async::Pool,
    ce: Arc<ClassificationParser>,
}

impl Titanium {
    pub async fn connect(url: &str, ce: Arc<ClassificationParser>) -> Result<Self> {
        let client = mysql_async::Pool::from_url(url)?;
        let mut conn = client.get_conn().await?;
        conn.ping().await?;
        Ok(Self {
            client,
            ce,
        })
    }

    pub async fn development(random_database: bool) -> Result<Self> {
        let config = assemblyline_markings::classification::sample_config();
        let parser = ClassificationParser::new(config)?;
        let parser = Arc::new(parser);
        assemblyline_models::set_global_classification(parser.clone());

        let default_url = "mysql://root@localhost:4000/";
        if !random_database {
            return Self::connect(default_url, parser.clone()).await
        }

        let client = mysql_async::Pool::from_url(default_url)?;
        let mut conn = client.get_conn().await?;

        let database = Alphabetic.sample_string(&mut rand::rng(), 16).to_lowercase();
        conn.exec_drop(format!("CREATE DATABASE {database}"), ()).await?;

        Self::connect(&format!("mysql://root@localhost:4000/{database}"), parser).await
    }

    pub fn create_table_command(table: &Table) -> (String, Vec<String>) {
        let mut fields = vec![];
        let mut indices = vec![];
        // let mut primary = None;

        for field in &table.fields {
            let mut string = field.name.clone();
            string += " ";
            string += &{
                let type_string = field.kind.titanium_type_string();
                match type_string.strip_prefix("enum ") {
                    Some(enum_name) => {
                        match ENUM_DETAILS.get(enum_name) {
                            Some(details) => {
                                let fields = details.values.iter().map(|row|format!("'{row}'")).join(", ");
                                format!("ENUM({fields})")
                            },
                            None => todo!("Unregistered enum type: {}", enum_name),
                        }
                    },
                    None => type_string,
                }
            };

            if !field.optional {
                string += " NOT NULL";
            }

            fields.push(string);
        }

        let primary = match &table.primary {
            Index::Custom(custom) => custom.clone(),
            Index::Default(name) => format!("{name} HASH"),
        };

        for index in &table.indices {
            match index {
                Index::Custom(custom) => {
                    indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{1} ON {0}({custom})", table.name, indices.len()));
                },
                Index::Default(name) => {

                    let field = match table.get_field(name) {
                        Some(field) => field,
                        None => {
                            warn!("Tried to build index on missing field: {name}");
                            continue
                        }
                    };

                    match field.kind {
                        // TableTypes::RandomId => {
                        //     panic!("id type used outside of primary key?");
                        // }

                        TableTypes::Text => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}({name}(128))", table.name));
                        }
                        TableTypes::Id
                        | TableTypes::Char(_)
                        | TableTypes::SmallInt
                        | TableTypes::Boolean
                        | TableTypes::Int
                        | TableTypes::BigInt
                        | TableTypes::Float
                        | TableTypes::Double
                        | TableTypes::Timestamp => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}({name} ASC)", table.name));
                        }

                        TableTypes::Enum(_) => { // | PostgresTypes::Uuid => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}({name})", table.name));
                        }

                        TableTypes::TextArrayInvert => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}((CAST({name} AS UNSIGNED ARRAY)))", table.name));
                            // indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0} USING ybgin({name})", table.name));
                        }

                        TableTypes::TextTrigram => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}({name} ASC)", table.name));
                        }

                        TableTypes::TextInvert => {
                            fields.push(format!("{name}_array json"));
                            // indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0} USING ybgin({name}_array)", table.name));
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}((CAST({name}_array AS UNSIGNED ARRAY)))", table.name));
                        },
                    }
                }
            }
        }

        let create = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {},\n    PRIMARY KEY({})\n) TTL = `expiry_ts` + INTERVAL 0 DAY;",
            table.name, fields.join(",\n    "), primary
        );

        (create, indices)
    }

    pub async fn create_table(&self, table: &Table, wipe: bool) -> Result<()> {
        info!("Creating table {} ...", table.name);
        let (create_table, create_indices) = Self::create_table_command(table);
        debug!("{create_table}");
        let mut conn = self.client.get_conn().await?;
        if wipe {
            conn.exec_drop(&format!("drop table if exists {}", table.name), ()).await?;
        }
        conn.exec_drop(&create_table, ()).await?;

        for create_index in create_indices {
            debug!("{create_index}");
            conn.exec_drop(&create_index, ()).await?;
        }
        Ok(())
    }

        pub async fn insert_submission(
        &mut self,
        sub: &datastore::Submission,
        results: &BTreeMap<String, datastore::Result>,
        errors: &BTreeMap<String, datastore::Error>,
        fileinfo: &BTreeMap<String, datastore::File>,
    ) -> Result<()> {
        // let mut metrics = InsertMetrics::default();
        loop {
            // let inserting_time = std::time::Instant::now();
            let err = match self.insert_submission_once(sub, results, errors, fileinfo).await {
                Ok(_) => {
                    // metrics.insert += inserting_time.elapsed();
                    // return Ok(metrics)
                    return Ok(())
                },
                Err(err) => err,
            };

            // let partitioning_time = std::time::Instant::now();
            // if err.downcast_ref::<PartitionMissing>().is_some() {
            //     self.create_partition_submissions(sub.expiry_ts).await?;
            //     self.create_partition_files(fileinfo.values()).await?;
            //     self.create_partition_results(results.values()).await?;
            //     self.create_partition_errors(errors.values()).await?;
            //     metrics.partition += partitioning_time.elapsed();
            //     continue
            // }

            // if let Some(err) = err.downcast_ref::<yb_tokio_postgres::Error>() {
            //     if let Some(err) = err.as_db_error() {
            //         if err.message().starts_with("no partition of relation") {
            //             self.create_partition_submissions(sub.expiry_ts).await?;
            //             self.create_partition_files(fileinfo.values()).await?;
            //             self.create_partition_results(results.values()).await?;
            //             self.create_partition_errors(errors.values()).await?;
            //             metrics.partition += partitioning_time.elapsed();
            //             continue
            //         }
            //     }
            // }

            // error!("{err}");
            // tokio::time::sleep(Duration::from_secs(5)).await;
            break Err(err)
        }
    }

    async fn insert_submission_once(
        &mut self,
        sub: &datastore::Submission,
        results: &BTreeMap<String, datastore::Result>,
        errors: &BTreeMap<String, datastore::Error>,
        fileinfo: &BTreeMap<String, datastore::File>
    ) -> Result<()> {
        let sid = sub.sid.to_string();
        let mut transaction = self.client.start_transaction(TxOpts::new()).await?;

        // submission
        let cmd = InsertBuilder::new(&SUBMISSION_TABLE, &sid, sub.expiry_ts)
            .build(&sub)?;
        transaction.exec_drop(&cmd.statement, cmd.parameters).await?;

        // metadata
        for (index, (key, value)) in sub.metadata.iter().sorted().enumerate() {
            let metadata = MetadataRow {
                counter: index as i32,
                sid: sub.sid.to_string(),
                name: key.clone(),
                value: value.to_string(),
                expiry_ts: sub.expiry_ts,
            };

            let cmd = InsertBuilder::new(&METADATA_TABLE, &sid, sub.expiry_ts)
                .build(&metadata)?;
            transaction.exec_drop(&cmd.statement, cmd.parameters).await?;
        }

        // results
        let mut tag_counter = 0;
        let mut relation_counter = 0;
        for (key, result) in results.iter() {
            let cmd = InsertBuilder::new(&RESULT_TABLE, &sid, result.expiry_ts)
                .key(key)
                // .return_id("id")
                .build(result)?;
            transaction.exec_drop(&cmd.statement, &cmd.parameters).await?;

            // tags
            for section in &result.result.sections {
                let mut tags = section.tags.to_list(None)?;
                tags.sort_unstable_by(|a, b| (&a.tag_type, &a.value).cmp(&(&b.tag_type, &b.value)));
                for tag in tags {
                    let row = TagRow {
                        expiry_ts: result.expiry_ts,
                        sid: &sid,
                        result: key,
                        name: &tag.tag_type,
                        score: tag.score,
                        heuristic: false,
                        value: &tag.value.to_string(),
                        counter: tag_counter,
                    };
                    tag_counter += 1;

                    let cmd = InsertBuilder::new(&TAG_TABLE, &sid, result.expiry_ts)
                        .build(&row)?;
                    transaction.exec_drop(&cmd.statement, &cmd.parameters).await?;
                }

                if let Some(heuristic) = &section.heuristic {
                    let row = TagRow {
                        expiry_ts: result.expiry_ts,
                        sid: &sid,
                        result: key,
                        name: &heuristic.heur_id,
                        score: heuristic.score,
                        heuristic: true,
                        value: "",
                        counter: tag_counter,
                    };
                    tag_counter += 1;

                    let cmd = InsertBuilder::new(&TAG_TABLE, &sid, result.expiry_ts)
                        .build(&row)?;
                    transaction.exec_drop(&cmd.statement, &cmd.parameters).await?;
                }
            }

            // file relations
            for (relations, supplementary) in [(result.response.extracted.iter(), false), (result.response.supplementary.iter(), true)] {
                for relation in relations {
                    let row = RelationRow {
                        expiry_ts: result.expiry_ts,
                        sid: Cow::Borrowed(&sid),
                        result: Cow::Borrowed(key),
                        parent: Cow::Borrowed(&result.sha256),
                        child: Cow::Borrowed(&relation.sha256),
                        name: Cow::Borrowed(&relation.name),
                        relation: Cow::Borrowed(relation.parent_relation.as_str()),
                        supplementary,
                        counter: relation_counter,
                    };
                    relation_counter += 1;

                    let cmd = InsertBuilder::new(&FILE_RELATION_TABLE, &sid, result.expiry_ts)
                        .build(&row)?;
                    transaction.exec_drop(&cmd.statement, &cmd.parameters).await?;
                }
            }

        }

        // errors
        for (key, error) in errors.iter() {

            let classification = sub.classification.classification.clone();

            let file_classification = match fileinfo.get(&*error.sha256) {
                Some(file) => &file.classification.classification,
                None => self.ce.restricted(),
            };

            let classification = self.ce.max_classification(&classification, file_classification, false)?;

            let cmd = InsertBuilder::new(&ERROR_TABLE, &sid, error.expiry_ts)
                .key(key)
                // .return_id("id")
                .build(error)?;

            // let (statement, params) = error.build_insert(ErrorInsertParams {
            //     classification: ExpandingClassification::new(classification, &self.ce)?,
            //     sid: sid.clone(),
            // })?;

            // params.validate(&ERROR_TABLE)?;
            transaction.exec_drop(&cmd.statement, &cmd.parameters).await?;
        }

        // files
        for file in fileinfo.values() {
            let cmd = InsertBuilder::new(&FILE_TABLE, &sid, file.expiry_ts)
                .build(file)?;

            transaction.exec_drop(&cmd.statement, &cmd.parameters).await?;
        }

        transaction.commit().await?;
        Ok(())
    }

}

#[derive(Debug, Default)]
pub struct InsertParameters {
    header: Vec<String>,
    row: Vec<String>,
    pub parameters: Vec<mysql_async::Value>,
}

impl InsertParameters {
    pub fn push(&mut self, name: &str, value: mysql_async::Value) {
        let index = self.parameters.len() + 1;
        self.header.push(name.to_string());
        self.row.push(format!("${index}"));
        self.parameters.push(value);
    }
}

pub struct InsertCommand {
    pub statement: String,
    pub parameters: Vec<mysql_async::Value>,
}

/// Utility struct to build insert commands for tables that have a relatively small amount of special casing
struct InsertBuilder<'a> {
    table: &'a Table,
    sid: &'a str,
    key: Option<&'a str>,
    expiry: Option<DateTime<Utc>>,
    json: Option<serde_json::Value>,
    // return_id: Option<&'a str>
}

impl<'a> InsertBuilder<'a> {
    fn new(table: &'a Table, sid: &'a str, expiry: Option<DateTime<Utc>>) -> Self {
        Self {
            table,
            sid,
            expiry,
            key: None,
            // return_id: None,
            json: None,
        }
    }

    // pub fn return_id(mut self, id: &'a str) -> Self {
    //     self.return_id = Some(id); self
    // }

    pub fn key(mut self, id: &'a str) -> Self {
        self.key = Some(id); self
    }

    pub fn json(mut self, json: serde_json::Value) -> Self {
        self.json = Some(json); self
    }

    // pub fn classification(mut self, classification: Value, classification_parts: Value) -> Self {
    //     self.classification = classification; self.classification_parts = classification_parts; self
    // }

    fn build(self, data: impl Serialize) -> Result<InsertCommand> {
        let json = match self.json {
            Some(value) => value,
            None => serde_json::to_value(&data)?,
        };
        let mut params = InsertParameters::default();

        for field in &self.table.fields {

            // auto generated ids should be left empty on inserts
            // if matches!(field.kind, TableTypes::RandomId) {
            //     continue
            // }

            // The raw field in every row is a dump of the full text of the underlying record
            // and they are all indexed as a tsvector
            if field.name == "raw" {
                params.push("raw", json.to_string().into());
                params.push("raw_array", self.raw_wordlist);
                continue
            }

            // // expiry is handled separately as it is part of the partition key
            // if field.name == "expiry_ts" {
            //     params.push("expiry_ts", normalize_expiry(&self.expiry).into());
            //     continue
            // }


            let value = match &field.extraction {
                Some(path) => {
                    match extract(path, &json) {
                        Some(value) => value,
                        None => {
                            if field.optional {
                                continue
                            }
                            todo!();
                        },
                    }
                },
                None if field.name == "sid" => {
                    &serde_json::json!(self.sid)
                }
                None if field.name == "key" => {
                    &serde_json::json!(self.key)
                }
                None => {
                    println!("{json:?}");
                    todo!("Could not extract field {} -> {}", self.table.name, field.name);
                },
            };

            if value.is_null() {
                if field.optional {
                    continue
                } else {
                    todo!()
                }
            }

            match field.kind {
                TableTypes::SmallInt => {
                    match value.as_i64() {
                        Some(num) => params.push(&field.name, (num as i16).into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                }
                TableTypes::Int => {
                    match value.as_i64() {
                        Some(num) => params.push(&field.name, (num as i32).into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                }
                TableTypes::BigInt => {
                    match value.as_i64() {
                        Some(num) => params.push(&field.name, num.into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                }

                TableTypes::Float => {
                    match value.as_f64() {
                        Some(num) => params.push(&field.name, (num as f32).into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                }

                TableTypes::Double => {
                    match value.as_f64() {
                        Some(num) => params.push(&field.name, num.into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                }
                TableTypes::Boolean => {
                    match value.as_bool() {
                        Some(num) => params.push(&field.name, num.into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                },
                // PostgresTypes::Uuid => {

                // },
                TableTypes::Timestamp => {
                    match value.as_str() {
                        Some(num) => params.push(&field.name, DateTime::parse_from_rfc3339(num)?.to_utc().into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                }
                TableTypes::Enum(label) => {
                    match label {
                        _ => bail!("Unhandled enumeration {label} | {}", field.name),
                    }
                },
                TableTypes::Char(_)
                | TableTypes::TextTrigram
                | TableTypes::Text => {
                    match value.as_str() {
                        Some(num) => params.push(&field.name, num.to_owned().into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                },
                TableTypes::TextArrayInvert => {
                    match as_string_array(value) {
                        Some(num) => params.push(&field.name, num.into()),
                        None => bail!("Unreadable field: {}", field.name),
                    }
                },
                TableTypes::TextInvert => {
                    match value.as_str() {
                        Some(num) => {
                            params.push(&field.name, wordlist(num))
                        },
                        None => bail!("Unreadable field: {}", field.name),
                    }
                },
                TableTypes::Id => {
                    match serde_json::from_value::<uuid::Uuid>(value.clone()) {
                        Ok(num) => params.push(&field.name, num.into()),
                        Err(err) => bail!("Unreadable uuid field: {} ({err})", field.name),
                    }
                }
                // TableTypes::RandomId => {
                //     bail!("May not insert values for serial fields")
                // },
            }
        }

        todo!()
    }
}

fn extract<'a, 'b>(path: &'a [String], data: &'b serde_json::Value) -> Option<&'b serde_json::Value> {
    if path.is_empty() {
        Some(data)
    } else {
        extract(&path[1..], data.get(&path[0])?)
    }
}


pub struct EnumDetails {
    pub values: Vec<String>,
}

impl EnumDetails {
    pub fn new<Enum: strum::IntoEnumIterator + Into<&'static str>>() -> Self {

        let mut values = vec![];
        for val in Enum::iter() {
            let string: &'static str = val.into();
            values.push(format!("{string}"));
        }

        Self { values }

        // // Create the type and exit if the creation suceeds
        // let command = format!("CREATE TYPE {name} AS ENUM ({})", values.join(", "));
        // info!("Registring enum type: {command}");
        // let mut conn = self.client.get_conn().await?;

        // match conn.exec_drop(&command, ()).await {
        //     Ok(_) => return Ok(()),
        //     Err(err) => {
        //         todo!("{err:?}");
        //         // if err.code() != Some(&SqlState::DUPLICATE_OBJECT) {
        //         //     return Err(err.into())
        //         // }
        //     }
        // }

        // todo!();

        // // Get all the values that already exist in this enum
        // let result = self.client.query("SELECT enumlabel FROM pg_enum INNER JOIN pg_type ON pg_enum.enumtypid = pg_type.oid WHERE pg_type.typname = $1", &[&name.to_lowercase()]).await?;
        // let mut exists = HashSet::new();
        // for row in result {
        //     exists.insert(row.get::<&str, String>("enumlabel"));
        // }

        // // add any new values that have been added to this enum
        // for val in Enum::iter() {
        //     let string: &'static str = val.into();
        //     if exists.contains(string) { continue }
        //     let command = format!("ALTER TYPE {name} ADD VALUE IF NOT EXISTS '{string}'");
        //     self.client.execute(&command, &[]).await?;
        // }
        // Ok(())
    }
}

static ENUM_DETAILS: LazyLock<HashMap<&'static str, EnumDetails>> = LazyLock::new(||{
    let mut enums = HashMap::new();
    enums.insert("error_status", EnumDetails::new::<assemblyline_models::datastore::error::StatusDiscriminants>());
    enums.insert("error_types", EnumDetails::new::<assemblyline_models::datastore::error::ErrorTypesDiscriminants>());
    enums.insert("error_severity", EnumDetails::new::<assemblyline_models::datastore::error::ErrorSeverityDiscriminants>());
    enums
});


// /// Interface for building insert commands for tables that have a great deal of special cased
// /// types such as enums or computed values
// trait BuildsInsert {
//     type Parameters;

//     fn build_insert(&self, params: Self::Parameters) -> Result<(String, InsertParameters)>;
// }


// macro_rules! merge_names {
//     ($name:ident | $($names:ident)|+) => {
//         concat!(stringify!($name), "_", merge_names!($($names)|+))
//     };
//     ($name:ident) => {
//         stringify!($name)
//     };
// }

// macro_rules! access_path {
//     ($root:expr, $name:ident | $($names:ident)|+) => {
//         access_path!($root.$name, $($names)|+)
//     };
//     ($root:expr, $name:ident) => {
//         $root.$name
//     };
// }

// macro_rules! insert_property {
//     ($self: ident, $params:ident, $($names:ident)|+) => {
//         $params.push(merge_names!($($names)|+), access_path!($self, $($names)|+).clone().into());
//     };
//     ($self: ident, $params:ident, $($names:ident)|+, $normalize:expr) => {
//         $params.push(merge_names!($($names)|+), $normalize(&access_path!($self, $($names)|+)).into());
//     };
// }

// struct ErrorInsertParams {
//     classification: ExpandingClassification,
//     sid: String,
// }

// impl BuildsInsert for datastore::Error {
//     type Parameters = ErrorInsertParams;

//     fn build_insert(&self, extra: ErrorInsertParams) -> Result<(String, InsertParameters)> {
//         let mut params = InsertParameters::default();

//         // Use macros to gaurentee that fields that should have related names in the struct
//         // and sql table always match
//         insert_property!(self, params, created);
//         insert_property!(self, params, expiry_ts, normalize_expiry);

//         insert_property!(self, params, response | message, ToString::to_string);
//         insert_property!(self, params, response | service_name, ToString::to_string);
//         insert_property!(self, params, response | service_version);
//         insert_property!(self, params, response | service_tool_version);
//         insert_property!(self, params, response | status);

//         insert_property!(self, params, sha256, ToString::to_string);
//         insert_property!(self, params, severity);

//         // insert rows that need to be computed or named explicitly
//         params.push("type", self.error_type.into());
//         params.push("raw", serde_json::to_string(self)?.into());

//         params.push("classification", extra.classification.classification.into());
//         params.push("__access_lvl__", extra.classification.__access_lvl__.into());
//         params.push("__access_req__", extra.classification.__access_req__.into());
//         params.push("__access_grp1__", extra.classification.__access_grp1__.into());
//         params.push("__access_grp2__", extra.classification.__access_grp2__.into());

//         params.push("sid", extra.sid.into());

//         let command = format!("INSERT INTO {} ({}) VALUES ({})", ANALYSIS_ERRORS_TABLE, params.header.join(", "), params.row.join(", "));
//         Ok((command, params))

//     }
// }





// A quick and dirty utility function
fn into_wordlist<Doc: Described<ElasticMeta>>(doc: &serde_json::Value) -> Vec<String> {
    let meta = Doc::metadata();
    let fields = extract_copyto_fields(doc, &meta, false);
    let mut words = vec![];
    for field in fields {
        words.extend(wordlist(field));
    }
    words.sort_unstable();
    words.dedup();
    words
}


fn extract_copyto_fields(data: &serde_json::Value, desc: &Descriptor<ElasticMeta>, copy: bool) -> Vec<String> {
    match &desc.kind {
        struct_metadata::Kind::Struct { name, children } => {
            let mut text = vec![];
            if let Some(data) = data.as_object() {
                for child in children {
                    if let Some(child_value) = data.get(child.label) {
                        text.extend(extract_copyto_fields(child_value, &child.type_info, copy || child.metadata.copyto.is_some() || desc.metadata.copyto.is_some()));
                    }
                }
            }
            text
        },
        struct_metadata::Kind::Aliased { name, kind } => {
            extract_copyto_fields(data, &kind, copy || desc.metadata.copyto.is_some())
        },
        struct_metadata::Kind::Sequence(descriptor) => {
            let mut text = vec![];
            if let Some(data) = data.as_array() {
                for item in data {
                    text.extend(extract_copyto_fields(item, &descriptor, copy || desc.metadata.copyto.is_some()));
                }
            }
            text
        },
        struct_metadata::Kind::Option(descriptor) => {
            extract_copyto_fields(data, &descriptor, copy || desc.metadata.copyto.is_some())
        },
        struct_metadata::Kind::Mapping(_descriptor, descriptor) => {
            let mut text = vec![];
            if let Some(data) = data.as_array() {
                for item in data {
                    text.extend(extract_copyto_fields(item, &*descriptor, copy || desc.metadata.copyto.is_some()));
                }
            }
            text
        },
        struct_metadata::Kind::Enum { .. } |
        struct_metadata::Kind::String => {
            let data = data.as_str().map(|r|r.to_string()).unwrap_or_else(|| data.to_string());
            if desc.metadata.copyto.is_some() {
                vec![data]
            } else {
                vec![]
            }
        },
        struct_metadata::Kind::Any |
        struct_metadata::Kind::JSON | struct_metadata::Kind::DateTime |
        struct_metadata::Kind::U128 | struct_metadata::Kind::I128 |
        struct_metadata::Kind::U64 | struct_metadata::Kind::I64 |
        struct_metadata::Kind::U32 | struct_metadata::Kind::I32 |
        struct_metadata::Kind::U16 | struct_metadata::Kind::I16 |
        struct_metadata::Kind::U8 | struct_metadata::Kind::I8 |
        struct_metadata::Kind::F64 | struct_metadata::Kind::F32 |
        struct_metadata::Kind::Bool => {
            if desc.metadata.copyto.is_some() {
                vec![data.to_string()]
            } else {
                vec![]
            }
        }
        _ => todo!()
    }
}