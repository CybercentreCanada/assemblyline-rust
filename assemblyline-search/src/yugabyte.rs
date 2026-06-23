use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore;
use assemblyline_models::datastore::tagging::{FlatTags, TagInformation, get_tag_information};
use assemblyline_models::types::{ExpandingClassification, Sha256, Sid};
use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, TimeZone, Utc};
use rand::distr::{Alphabetic, SampleString};
use serde::Serialize;
use serde_json::Value;
use yb_tokio_postgres::error::SqlState;
use yb_tokio_postgres::types::ToSql;
use yb_tokio_postgres::{Client, NoTls, Transaction, connect};
pub use bb8;

use anyhow::{Context, Result, bail};
use log::{debug, error, info, warn};

use crate::tables::{ALL_ANALYSIS_TABLES, ANALYSIS_ERRORS_TABLE, ANALYSIS_FILES_TABLE, ANALYSIS_METADATA_TABLE, ANALYSIS_RELATIONS_TABLE, ANALYSIS_RESULTS_TABLE, ANALYSIS_TAGS_TABLE, Index, MetadataRow, RelationRow, Table, TableTypes, TagRow, init_error_table, init_file_relation_table, init_file_table, init_metadata_table, init_result_table, init_submission_table, init_tag_table};
use crate::tables::ANALYSIS_SUBMISSIONS_TABLE;
use crate::yugabyte::PartitionScheme::Weekly;




pub struct Locks {
    locks: parking_lot::Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Locks {
    pub fn new() -> Self {
        Self {
            locks: parking_lot::Mutex::new(Default::default()),
        }
    }

    pub async fn lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock();
        locks.entry(name.to_owned()).or_default().clone()
    }
}

pub struct Yugabyte {
    pub (crate) client: Client,
    ce: Arc<ClassificationParser>,
    locks: Arc<Locks>,
    submission_table: Table,
    result_table: Table,
    metadata_table: Table,
    tag_table: Table,
    relation_table: Table,
    error_table: Table,
    file_table: Table,
    partition_scheme: PartitionScheme
}

#[derive(Debug, thiserror::Error)]
#[error("An operation failed due to a missing partition on {0} for period containing {1:?}")]
struct PartitionMissing(String, Option<DateTime<Utc>>);

impl Yugabyte {

    pub async fn connect(url: &str, ce: Arc<ClassificationParser>, locks: Arc<Locks>) -> Result<Self, yb_tokio_postgres::Error> {
        info!("Database connecting...");
        let (client, connection) = connect(url, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("yugabyte connection error: {e}");
            }
        });

        info!("Enabling database extensions...");
        client.execute("CREATE extension IF NOT EXISTS pg_trgm", &[]).await?;
        match client.execute("CREATE extension IF NOT EXISTS \"uuid-ossp\"", &[]).await {
            Ok(_) => {},
            Err(err) => {
                println!("{err}");
            }
        };

        info!("Database ready");
        Ok(Self{
            client,
            ce,
            locks,
            submission_table: init_submission_table(),
            result_table: init_result_table(),
            metadata_table: init_metadata_table(),
            tag_table: init_tag_table(),
            relation_table: init_file_relation_table(),
            error_table: init_error_table(),
            file_table: init_file_table(),
            partition_scheme: Weekly,
        })
    }

    pub async fn development(random_db: bool) -> Result<Self> {
        let config = assemblyline_markings::classification::sample_config();
        let parser = ClassificationParser::new(config)?;
        let parser = Arc::new(parser);
        assemblyline_models::set_global_classification(parser.clone());
        let db = Self::connect("postgresql://localhost:5433/yugabyte?user=yugabyte&password=yugabyte", parser.clone(), Arc::new(Locks::new())).await?;

        if random_db {
            let database = Alphabetic.sample_string(&mut rand::rng(), 16).to_lowercase();
            db.client.execute(&format!("CREATE DATABASE {database}"), &[]).await?;
            let db = Self::connect(&format!("postgresql://localhost:5433/{database}?user=yugabyte&password=yugabyte"), parser, Arc::new(Locks::new())).await?;
            // db.client.execute(&format!("CONNECT TO {database}"), &[]).await?;
            Ok(db)
        } else {
            Ok(db)
        }
    }

    pub async fn register_type<Enum: strum::IntoEnumIterator + Into<&'static str>>(&self, name: &str) -> Result<()> {
        let mut values = vec![];
        for val in Enum::iter() {
            let string: &'static str = val.into();
            values.push(format!("'{string}'"));
        }

        // Create the type and exit if the creation suceeds
        let command = format!("CREATE TYPE {name} AS ENUM ({})", values.join(", "));
        info!("Registring enum type: {command}");

        match self.client.execute(&command, &[]).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.code() != Some(&SqlState::DUPLICATE_OBJECT) {
                    return Err(err.into())
                }
            }
        }

        // Get all the values that already exist in this enum
        let result = self.client.query("SELECT enumlabel FROM pg_enum INNER JOIN pg_type ON pg_enum.enumtypid = pg_type.oid WHERE pg_type.typname = $1", &[&name.to_lowercase()]).await?;
        let mut exists = HashSet::new();
        for row in result {
            exists.insert(row.get::<&str, String>("enumlabel"));
        }

        // add any new values that have been added to this enum
        for val in Enum::iter() {
            let string: &'static str = val.into();
            if exists.contains(string) { continue }
            let command = format!("ALTER TYPE {name} ADD VALUE IF NOT EXISTS '{string}'");
            self.client.execute(&command, &[]).await?;
        }
        Ok(())
    }


    pub fn create_table_command(table: &Table) -> (String, Vec<String>) {
        let mut fields = vec![];
        let mut indices = vec![];
        // let mut primary = None;

        for field in &table.fields {
            let mut string = field.name.clone();
            string += " ";
            string += &field.kind.postgres_type_string();

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

                        TableTypes::Text
                        | TableTypes::Id
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
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}({name} HASH)", table.name));
                        }

                        TableTypes::TextArrayInvert => { //| PostgresTypes::JsonInverse => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0} USING ybgin({name})", table.name));
                        }

                        TableTypes::TextTrigram => {
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0}({name} ASC)", table.name));
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name}_tgram ON {0} USING ybgin({name} gin_trgm_ops)", table.name));
                        }

                        TableTypes::TextInvert => {
                            fields.push(format!("{name}_vectored tsvector"));
                            indices.push(format!("CREATE INDEX IF NOT EXISTS {0}_{name} ON {0} USING ybgin({name}_vectored)", table.name));
                        },
                    }
                }
            }
        }

        let create = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {},\n    PRIMARY KEY({})\n) PARTITION BY RANGE (expiry_ts);",
            table.name, fields.join(",\n    "), primary
        );

        (create, indices)
    }


    pub async fn create_table(&self, table: &Table, wipe: bool) -> Result<()> {
        info!("Creating table {} ...", table.name);
        let (create_table, create_indices) = Self::create_table_command(table);
        debug!("{create_table}");
        if wipe {
            self.client.execute(&format!("drop table if exists {}", table.name), &[]).await?;
        }
        self.client.execute(&create_table, &[]).await?;

        for create_index in create_indices {
            debug!("{create_index}");
            self.client.execute(&create_index, &[]).await?;
        }
        Ok(())
    }

    pub async fn count_submissions(&self) -> Result<i64> {
        let command = format!("SELECT COUNT(1) FROM {ANALYSIS_SUBMISSIONS_TABLE}");
        let rows = self.client.query_one(&command, &[]).await?;
        Ok(rows.try_get(0)?)
    }

    pub async fn submission_exists(&self, sub: Sid) -> Result<bool> {
        let command = format!("SELECT 1 FROM {ANALYSIS_SUBMISSIONS_TABLE} WHERE sid = $1 LIMIT 1");
        let rows = self.client.query_opt(&command, &[&sub.to_string()]).await?;
        Ok(rows.is_some())
    }

    pub async fn fetch_submission(&self, sub: Sid) -> Result<Option<datastore::Submission>> {
        let command = format!("SELECT raw FROM {ANALYSIS_SUBMISSIONS_TABLE} WHERE sid = $1 LIMIT 1");
        let rows = self.client.query_opt(&command, &[&sub.to_string()]).await?;
        match rows {
            Some(row) => {
                let json: &str = row.try_get("raw")?;
                Ok(Some(serde_json::from_str(json)?))
            },
            None => Ok(None)
        }
    }

    pub async fn fetch_submission_errors(&self, sub: Sid) -> Result<Vec<datastore::Error>> {
        let command = format!("SELECT raw FROM {ANALYSIS_ERRORS_TABLE} WHERE sid = $1");
        let rows = self.client.query(&command, &[&sub.to_string()]).await?;
        let mut output = vec![];
        for row in rows {
            let json: &str = row.try_get("raw")?;
            output.push(serde_json::from_str(json)?);
        }
        Ok(output)
    }

    pub async fn fetch_submission_files(&self, sub: Sid) -> Result<HashMap<String, datastore::File>> {
        let command = format!("SELECT raw FROM {ANALYSIS_FILES_TABLE} WHERE sid = $1");
        let rows = self.client.query(&command, &[&sub.to_string()]).await?;
        let mut output = HashMap::new();
        for row in rows {
            let json: &str = row.try_get("raw")?;
            let file: datastore::File = serde_json::from_str(json)?;
            output.insert(file.sha256.to_string(), file);
        }
        Ok(output)
    }

    pub async fn fetch_submission_results(&self, sub: Sid) -> Result<HashMap<String, datastore::Result>> {
        let command = format!("SELECT key, raw FROM {ANALYSIS_RESULTS_TABLE} WHERE sid = $1");
        let rows = self.client.query(&command, &[&sub.to_string()]).await?;
        let mut output = HashMap::new();
        for row in rows {
            let key: &str = row.try_get("key")?;
            let json: &str = row.try_get("raw")?;
            output.insert(key.to_owned(), serde_json::from_str(json)?);
        }
        Ok(output)
    }

    pub async fn fetch_submission_tags_merged(&self, sub: Sid) -> Result<FlatTags> {
        let command = format!("SELECT key, value FROM {ANALYSIS_TAGS_TABLE} WHERE sid = $1");
        let rows = self.client.query(&command, &[&sub.to_string()]).await?;
        let mut output = FlatTags::default();
        for row in rows {
            let key: &str = row.try_get("key")?;
            let value: &str = row.try_get("value")?;
            if let Some(key) = get_tag_information(key) {
                output.entry(key).or_default().push(value.into());
            }
        }
        Ok(output)
    }

    pub async fn fetch_submission_metadata(&self, sub: Sid) -> Result<HashMap<String, String>> {
        let command = format!("SELECT key, value FROM {ANALYSIS_METADATA_TABLE} WHERE sid = $1");
        let rows = self.client.query(&command, &[&sub.to_string()]).await?;
        let mut output = HashMap::new();
        for row in rows {
            let key: &str = row.try_get("key")?;
            let value: &str = row.try_get("value")?;
            output.insert(key.to_owned(), value.to_owned());
        }
        Ok(output)
    }

    pub async fn fetch_submission_relations(&self, sub: Sid) -> Result<Vec<RelationRow<'_>>> {
        let command = format!("SELECT rel.expiry_ts, rel.result, rel.parent, rel.child, rel.name, rel.relation, rel.supplementary FROM {ANALYSIS_RELATIONS_TABLE} rel INNER JOIN {ANALYSIS_RESULTS_TABLE} res ON rel.result = res.id WHERE res.sid = $1");
        let rows = self.client.query(&command, &[&sub.to_string()]).await?;
        let mut output = vec![];
        for row in rows {
            todo!()
            // output.push(RelationRow {
            //     expiry_ts: row.try_get("expiry_ts")?,
            //     sid: Cow::Owned(sub.to_string()),
            //     result: row.try_get("result")?,
            //     parent: Cow::Owned(row.try_get("parent")?),
            //     child: Cow::Owned(row.try_get("child")?),
            //     name: Cow::Owned(row.try_get("name")?),
            //     relation: Cow::Owned(row.try_get("relation")?),
            //     supplementary: row.try_get("supplementary")?,
            // });
        }
        Ok(output)
    }
}

#[derive(Debug, Default)]
pub struct InsertMetrics {
    pub partition: std::time::Duration,
    pub insert: std::time::Duration,
}

impl Yugabyte {
    pub async fn insert_submission(
        &mut self,
        sub: &datastore::Submission,
        results: &HashMap<String, datastore::Result>,
        errors: &HashMap<String, datastore::Error>,
        fileinfo: &HashMap<String, datastore::File>,
    ) -> Result<InsertMetrics> {
        let mut metrics = InsertMetrics::default();
        loop {
            let inserting_time = std::time::Instant::now();
            let err = match self.insert_submission_once(sub, results, errors, fileinfo).await {
                Ok(_) => {
                    metrics.insert += inserting_time.elapsed();
                    return Ok(metrics)
                },
                Err(err) => err,
            };

            let partitioning_time = std::time::Instant::now();
            if err.downcast_ref::<PartitionMissing>().is_some() {
                self.create_partition_submissions(sub.expiry_ts).await?;
                self.create_partition_files(fileinfo.values()).await?;
                self.create_partition_results(results.values()).await?;
                self.create_partition_errors(errors.values()).await?;
                metrics.partition += partitioning_time.elapsed();
                continue
            }

            if let Some(err) = err.downcast_ref::<yb_tokio_postgres::Error>() {
                if let Some(err) = err.as_db_error() {
                    if err.message().starts_with("no partition of relation") {
                        self.create_partition_submissions(sub.expiry_ts).await?;
                        self.create_partition_files(fileinfo.values()).await?;
                        self.create_partition_results(results.values()).await?;
                        self.create_partition_errors(errors.values()).await?;
                        metrics.partition += partitioning_time.elapsed();
                        continue
                    }
                }
            }

            // error!("{err}");
            // tokio::time::sleep(Duration::from_secs(5)).await;
            break Err(err)
        }
    }

    async fn insert_submission_once(
        &mut self,
        sub: &datastore::Submission,
        results: &HashMap<String, datastore::Result>,
        errors: &HashMap<String, datastore::Error>,
        fileinfo: &HashMap<String, datastore::File>
    ) -> Result<()> {
        let sid = sub.sid.to_string();
        let transaction = self.client.transaction().await?;

        // submission
        let cmd = InsertBuilder::new(&self.submission_table, &sid, sub.expiry_ts)
            .build(&sub)?;
        transaction.execute(&cmd.statement, &cmd.parameters.params()).await?;
        todo!();

        // metadata
        // for (key, value) in sub.metadata.iter() {
        //     let metadata = MetadataRow {
        //         sid: sub.sid.to_string(),
        //         name: key.clone(),
        //         value: value.to_string(),
        //         expiry_ts: sub.expiry_ts,
        //     };

        //     let cmd = InsertBuilder::new(&self.metadata_table, &sid, sub.expiry_ts)
        //         .build(&metadata)?;
        //     transaction.execute(&cmd.statement, &cmd.parameters.params()).await?;
        // }

        // // results
        // for (key, result) in results.iter() {
        //     let cmd = InsertBuilder::new(&self.result_table, &sid, result.expiry_ts)
        //         .key(key)
        //         .return_id("id")
        //         .build(result)?;
        //     let row = transaction.query_one(&cmd.statement, &cmd.parameters.params()).await?;

        //     let id: uuid::Uuid = row.try_get("id").context("id_as_uuid")?;

        //     // tags
        //     for section in &result.result.sections {
        //         let tags = section.tags.to_list(None)?;
        //         for tag in tags {
        //             let row = TagRow {
        //                 expiry_ts: result.expiry_ts,
        //                 sid: &sid,
        //                 result: id,
        //                 name: &tag.tag_type,
        //                 score: tag.score,
        //                 heuristic: false,
        //                 value: &tag.value.to_string(),
        //             };

        //             let cmd = InsertBuilder::new(&self.tag_table, &sid, result.expiry_ts)
        //                 .build(&row)?;
        //             transaction.execute(&cmd.statement, &cmd.parameters.params()).await?;
        //         }

        //         if let Some(heuristic) = &section.heuristic {
        //             let row = TagRow {
        //                 expiry_ts: result.expiry_ts,
        //                 sid: &sid,
        //                 result: id,
        //                 name: &heuristic.heur_id,
        //                 score: heuristic.score,
        //                 heuristic: true,
        //                 value: "",
        //             };

        //             let cmd = InsertBuilder::new(&self.tag_table, &sid, result.expiry_ts)
        //                 .build(&row)?;
        //             transaction.execute(&cmd.statement, &cmd.parameters.params()).await?;
        //         }
        //     }

        //     // file relations
        //     for (relations, supplementary) in [(result.response.extracted.iter(), false), (result.response.supplementary.iter(), true)] {
        //         for relation in relations {
        //             let row = RelationRow {
        //                 expiry_ts: result.expiry_ts,
        //                 sid: Cow::Borrowed(&sid),
        //                 result: id,
        //                 parent: Cow::Borrowed(&result.sha256),
        //                 child: Cow::Borrowed(&relation.sha256),
        //                 name: Cow::Borrowed(&relation.name),
        //                 relation: Cow::Borrowed(relation.parent_relation.as_str()),
        //                 supplementary,
        //             };

        //             let cmd = InsertBuilder::new(&self.relation_table, &sid, result.expiry_ts)
        //                 .build(&row)?;
        //             transaction.execute(&cmd.statement, &cmd.parameters.params()).await?;
        //         }
        //     }

        // }

        // errors
        for error in errors.values() {

            let classification = sub.classification.classification.clone();

            let file_classification = match fileinfo.get(&*error.sha256) {
                Some(file) => &file.classification.classification,
                None => self.ce.restricted(),
            };

            let classification = self.ce.max_classification(&classification, file_classification, false)?;

            let (statement, params) = error.build_insert(ErrorInsertParams {
                classification: ExpandingClassification::new(classification, &self.ce)?,
                sid: sid.clone(),
            })?;

            params.validate(&self.error_table)?;
            transaction.execute(&statement, &params.params()).await?;
        }

        // files
        for file in fileinfo.values() {

            let cmd = InsertBuilder::new(&self.file_table, &sid, file.expiry_ts)
                .build(file)?;

            transaction.execute(&cmd.statement, &cmd.parameters.params()).await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    // ANALYSIS_SUBMISSIONS_TABLE, ANALYSIS_METADATA_TABLE,
    async fn create_partition_submissions(&self, time: Option<DateTime<Utc>>) -> Result<()> {
        self.create_partition_on(ANALYSIS_SUBMISSIONS_TABLE, time).await?;
        self.create_partition_on(ANALYSIS_METADATA_TABLE, time).await?;
        Ok(())
    }

    // ANALYSIS_RESULTS_TABLE, ANALYSIS_TAGS_TABLE, ANALYSIS_RELATIONS_TABLE,
    async fn create_partition_results(&self, results: impl Iterator<Item=&datastore::Result>) -> Result<()> {
        let mut times = vec![];
        for result in results {
            times.push(result.expiry_ts);
        }
        times.sort_unstable();
        times.dedup_by(|a, b| self.partition_scheme.date_label(a) == self.partition_scheme.date_label(b));
        for time in times {
            self.create_partition_on(ANALYSIS_RESULTS_TABLE, time).await?;
            self.create_partition_on(ANALYSIS_TAGS_TABLE, time).await?;
            self.create_partition_on(ANALYSIS_RELATIONS_TABLE, time).await?;
        }
        Ok(())
    }

    // ANALYSIS_ERRORS_TABLE,
    async fn create_partition_errors(&self, errors: impl Iterator<Item=&datastore::Error>) -> Result<()> {
        let mut times = vec![];
        for error in errors {
            times.push(error.expiry_ts);
        }
        times.sort_unstable();
        times.dedup_by(|a, b| self.partition_scheme.date_label(a) == self.partition_scheme.date_label(b));
        for time in times {
            self.create_partition_on(ANALYSIS_ERRORS_TABLE, time).await?;
        }
        Ok(())
    }

    // ANALYSIS_FILES_TABLE,
    async fn create_partition_files(&self, files: impl Iterator<Item=&datastore::File>) -> Result<()> {
        let mut times = vec![];
        for file in files {
            times.push(file.expiry_ts);
        }
        times.sort_unstable();
        times.dedup_by(|a, b| self.partition_scheme.date_label(a) == self.partition_scheme.date_label(b));
        for time in times {
            self.create_partition_on(ANALYSIS_FILES_TABLE, time).await?;
        }
        Ok(())
    }

    async fn create_partition_on(&self, table: &str, time: Option<DateTime<Utc>>) -> Result<()> {
        loop {
            match self._create_partition_on(table, time).await {
                Ok(()) => break Ok(()),
                Err(err) => {
                    if err.to_string().contains("deleted while still in use") {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue
                    }
                    break Err(anyhow::anyhow!(err).context("create partition"))
                },
            }
        }
    }

    async fn _create_partition_on(&self, table: &str, time: Option<DateTime<Utc>>) -> Result<()> {
        let lock = self.locks.lock(table).await;
        let _guard = lock.lock().await;
        let label = self.partition_scheme.date_label(&time);
        let (start, end) = self.partition_scheme.window(&time).ok_or(anyhow::anyhow!("date error"))?;
        let command = format!("CREATE TABLE IF NOT EXISTS {table}_{label} PARTITION OF {table} FOR VALUES FROM ({start}) TO ({end})");
        info!("Creating partition on {table} for {label}: {command}");
        match self.client.execute(&command, &[]).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // the 'if not exists' clause doesn't stop errors from occuring when two create partition commands occur at once.
                if let Some(err) = err.as_db_error() {
                    if err.message().contains(&format!("\"{table}_{label}\" already exists")) {
                        return Ok(())
                    }
                }
                Err(err.into())
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PartitionScheme {
    Daily,
    Weekly,
    Monthly,
    None,
}

impl PartitionScheme {
    fn date_label(&self, time: &Option<DateTime<Utc>>) -> String {
        match time {
            None => "null".to_string(),
            Some(date) => match self {
                PartitionScheme::Daily => date.format("%Y_%m_%d").to_string(),
                PartitionScheme::Weekly => date.format("%G_%V").to_string(),
                PartitionScheme::Monthly => date.format("%Y_%m").to_string(),
                PartitionScheme::None => "hot".to_string(),
            }

        }
    }

    fn window(&self, time: &Option<DateTime<Utc>>) -> Option<(String, String)> {
        Some(match time {
            Some(date) => match self {
                PartitionScheme::Daily => {
                    (date.format("'%Y-%m-%d'").to_string(), (*date + TimeDelta::days(1)).format("'%Y-%m-%d'").to_string())
                },
                PartitionScheme::Weekly => {
                    let first = date.iso_week();
                    let mut later = *date;
                    let second = loop {
                        later += TimeDelta::days(1);
                        let second = later.iso_week();
                        if second != first {
                            break second
                        }
                        // println!("{date} -> {first}; {later} -> {second}");
                    };
                    let first = NaiveDate::from_isoywd_opt(first.year(), first.week(), chrono::Weekday::Mon)?;
                    let second = NaiveDate::from_isoywd_opt(second.year(), second.week(), chrono::Weekday::Mon)?;
                    (first.format("'%Y-%m-%d'").to_string(), second.format("'%Y-%m-%d'").to_string())
                },
                PartitionScheme::Monthly => {
                    let after = date.checked_add_months(chrono::Months::new(1))?;
                    (date.format("'%Y-%m-%d'").to_string(), after.format("'%Y-%m-%d'").to_string())
                },
                PartitionScheme::None => {
                    ("'1000-01-01'".to_string(), "'3000-01-01'".to_string())
                },
            },
            None => ("'9999-01-01'".to_string(), "'infinity'".to_string()),
        })
    }
}

fn extract<'a, 'b>(path: &'a [String], data: &'b Value) -> Option<&'b Value> {
    if path.is_empty() {
        Some(data)
    } else {
        extract(&path[1..], data.get(&path[0])?)
    }
}

// fn extract_i64<'a, 'b>(path: &'a [String], data: &'b Value) -> Option<i64> {
//     extract(path, data)?.as_number()?.as_i64()
// }

fn as_string_array(data: &Value) -> Option<Vec<String>> {
    let mut out = vec![];
    for obj in data.as_array()? {
        out.push(obj.as_str()?.to_owned());
    }
    Some(out)
}

// a concrete type used to capture parameters before casting them to the more generic
// trait reference of ToSql. Having a concrete type in the middle involves a bit more
// boilerplate, is faster (citation needed), but makes a lot of operations in
// testing simpler as well as being more flexable when used between insert and select.
#[derive(Debug, PartialEq, Clone)]
pub enum ParameterValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Uuid(uuid::Uuid),
    String(String),
    StringOptional(Option<String>),
    StringList(Vec<String>),
    DateTime(DateTime<Utc>),
    ErrorTypes(datastore::error::ErrorTypes),
    ErrorStatus(datastore::error::Status),
    ErrorSeverity(datastore::error::ErrorSeverity),
}

impl ParameterValue {
    fn to_sql_ref(&self) -> &(dyn ToSql + Sync) {
        match self {
            ParameterValue::Bool(value) => value,
            ParameterValue::I16(value) => value,
            ParameterValue::I32(value) => value,
            ParameterValue::I64(value) => value,
            ParameterValue::F32(value) => value,
            ParameterValue::F64(value) => value,
            ParameterValue::Uuid(value) => value,
            ParameterValue::String(value) => value,
            ParameterValue::StringOptional(value) => value,
            ParameterValue::StringList(value) => value,
            ParameterValue::DateTime(value) => value,
            ParameterValue::ErrorTypes(value) => value,
            ParameterValue::ErrorStatus(value) => value,
            ParameterValue::ErrorSeverity(value) => value,
        }
    }
}

impl From<bool> for ParameterValue {
    fn from(value: bool) -> Self { Self::Bool(value) }
}

impl From<i16> for ParameterValue {
    fn from(value: i16) -> Self { Self::I16(value) }
}

impl From<i32> for ParameterValue {
    fn from(value: i32) -> Self { Self::I32(value) }
}

impl From<i64> for ParameterValue {
    fn from(value: i64) -> Self { Self::I64(value) }
}

impl From<f32> for ParameterValue {
    fn from(value: f32) -> Self { Self::F32(value) }
}

impl From<f64> for ParameterValue {
    fn from(value: f64) -> Self { Self::F64(value) }
}

impl From<uuid::Uuid> for ParameterValue {
    fn from(value: uuid::Uuid) -> Self { Self::Uuid(value) }
}

impl From<String> for ParameterValue {
    fn from(value: String) -> Self { Self::String(value) }
}

impl From<Option<String>> for ParameterValue {
    fn from(value: Option<String>) -> Self { Self::StringOptional(value) }
}

impl From<Vec<String>> for ParameterValue {
    fn from(value: Vec<String>) -> Self { Self::StringList(value) }
}

impl From<DateTime<Utc>> for ParameterValue {
    fn from(value: DateTime<Utc>) -> Self { Self::DateTime(value) }
}

impl From<datastore::error::ErrorTypes> for ParameterValue {
    fn from(value: datastore::error::ErrorTypes) -> Self { Self::ErrorTypes(value) }
}

impl From<datastore::error::Status> for ParameterValue {
    fn from(value: datastore::error::Status) -> Self { Self::ErrorStatus(value) }
}

impl From<datastore::error::ErrorSeverity> for ParameterValue {
    fn from(value: datastore::error::ErrorSeverity) -> Self { Self::ErrorSeverity(value) }
}


#[derive(Debug, Default)]
pub struct InsertParameters {
    header: Vec<String>,
    row: Vec<String>,
    pub parameters: Vec<ParameterValue>,
}

impl InsertParameters {
    pub fn push(&mut self, name: &str, value: ParameterValue) {
        let index = self.parameters.len() + 1;
        self.header.push(name.to_string());
        self.row.push(format!("${index}"));
        self.parameters.push(value);
    }

    pub fn push_tsvector(&mut self, name: &str, value: ParameterValue) {
        let index = self.parameters.len() + 1;
        self.header.push(name.to_string());
        self.header.push(format!("{name}_vectored"));
        self.row.push(format!("${index}"));
        self.row.push(format!("to_tsvector(${index})"));
        self.parameters.push(value);
    }

    pub fn params(&self) -> Vec<&(dyn ToSql + Sync)> {
        let mut out: Vec<&(dyn ToSql + Sync)> = vec![];
        for p in &self.parameters {
            out.push(p.to_sql_ref());
        }
        out
    }

    pub fn validate(&self, table: &Table) -> Result<()> {
        todo!()
        // for field in &table.fields {
        //     if self.header.contains(&field.name) || field.kind.generated() {
        //         continue
        //     }
        //     bail!("Missing expected field: {} -> {}", table.name, field.name);
        // }
        // for name in &self.header {
        //     if table.fields.iter().any(|f| f.name == *name) {
        //         continue
        //     }
        //     bail!("Insert contains unexpected field: {name}");
        // }
        // Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Parameters {
    pub parameters: Vec<ParameterValue>,
}

impl Parameters {
    pub fn push(&mut self, value: ParameterValue) -> String {
        let index = self.parameters.len() + 1;
        self.parameters.push(value);
        format!("${index}")
    }

    pub fn params<'a>(&'a self) -> Vec<&'a (dyn ToSql + Sync)> {
        let mut out: Vec<&(dyn ToSql + Sync)> = vec![];
        for p in &self.parameters {
            out.push(p.to_sql_ref());
        }
        out
    }
}

pub struct SelectCommand {
    pub statement: String,
    pub parameters: Parameters
}


pub struct InsertCommand {
    pub statement: String,
    pub parameters: InsertParameters
}

/// Utility struct to build insert commands for tables that have a relatively small amount of special casing
struct InsertBuilder<'a> {
    table: &'a Table,
    sid: &'a str,
    key: Option<&'a str>,
    expiry: Option<DateTime<Utc>>,
    json: Option<Value>,
    return_id: Option<&'a str>
}

impl<'a> InsertBuilder<'a> {
    fn new(table: &'a Table, sid: &'a str, expiry: Option<DateTime<Utc>>) -> Self {
        Self {
            table,
            sid,
            expiry,
            key: None,
            return_id: None,
            json: None,
        }
    }

    pub fn return_id(mut self, id: &'a str) -> Self {
        self.return_id = Some(id); self
    }

    pub fn key(mut self, id: &'a str) -> Self {
        self.key = Some(id); self
    }

    pub fn json(mut self, json: Value) -> Self {
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
                params.push_tsvector("raw", serde_json::to_string(&data)?.into());
                continue
            }

            // expiry is handled separately as it is part of the partition key
            if field.name == "expiry_ts" {
                params.push("expiry_ts", normalize_expiry(&self.expiry).into());
                continue
            }


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
                        Some(num) => params.push_tsvector(&field.name, num.to_owned().into()),
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

        let return_clause = match self.return_id {
            Some(id) => format!("RETURNING {id}"),
            None => "".to_string(),
        };

        let command = format!("INSERT INTO {} ({}) VALUES ({}){return_clause}", self.table.name, params.header.join(", "), params.row.join(", "));
        Ok(InsertCommand { statement: command, parameters: params })
    }
}

fn normalize_expiry(value: &Option<DateTime<Utc>>) -> DateTime<Utc> {
    match value {
        Some(time) => *time,
        None => Utc::now() + TimeDelta::days(10000 * 365),
    }
}

/// Interface for building insert commands for tables that have a great deal of special cased
/// types such as enums or computed values
trait BuildsInsert {
    type Parameters;

    fn build_insert(&self, params: Self::Parameters) -> Result<(String, InsertParameters)>;
}


macro_rules! merge_names {
    ($name:ident | $($names:ident)|+) => {
        concat!(stringify!($name), "_", merge_names!($($names)|+))
    };
    ($name:ident) => {
        stringify!($name)
    };
}

macro_rules! access_path {
    ($root:expr, $name:ident | $($names:ident)|+) => {
        access_path!($root.$name, $($names)|+)
    };
    ($root:expr, $name:ident) => {
        $root.$name
    };
}

macro_rules! insert_property {
    ($self: ident, $params:ident, $($names:ident)|+) => {
        $params.push(merge_names!($($names)|+), access_path!($self, $($names)|+).clone().into());
    };
    ($self: ident, $params:ident, $($names:ident)|+, $normalize:expr) => {
        $params.push(merge_names!($($names)|+), $normalize(&access_path!($self, $($names)|+)).into());
    };
}

struct ErrorInsertParams {
    classification: ExpandingClassification,
    sid: String,
}

impl BuildsInsert for datastore::Error {
    type Parameters = ErrorInsertParams;

    fn build_insert(&self, extra: ErrorInsertParams) -> Result<(String, InsertParameters)> {
        let mut params = InsertParameters::default();

        // Use macros to gaurentee that fields that should have related names in the struct
        // and sql table always match
        insert_property!(self, params, created);
        insert_property!(self, params, expiry_ts, normalize_expiry);

        insert_property!(self, params, response | message, ToString::to_string);
        insert_property!(self, params, response | service_name, ToString::to_string);
        insert_property!(self, params, response | service_version);
        insert_property!(self, params, response | service_tool_version);
        insert_property!(self, params, response | status);

        insert_property!(self, params, sha256, ToString::to_string);
        insert_property!(self, params, severity);

        // insert rows that need to be computed or named explicitly
        params.push("type", self.error_type.into());
        params.push("raw", serde_json::to_string(self)?.into());

        params.push("classification", extra.classification.classification.into());
        params.push("__access_lvl__", extra.classification.__access_lvl__.into());
        params.push("__access_req__", extra.classification.__access_req__.into());
        params.push("__access_grp1__", extra.classification.__access_grp1__.into());
        params.push("__access_grp2__", extra.classification.__access_grp2__.into());

        params.push("sid", extra.sid.into());

        let command = format!("INSERT INTO {} ({}) VALUES ({})", ANALYSIS_ERRORS_TABLE, params.header.join(", "), params.row.join(", "));
        Ok((command, params))

    }
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("A connection was closed unexpectedly")]
    ConnectionClosed,
    #[error("database error: {0}")]
    LibraryError(yb_tokio_postgres::Error),
}

pub struct YugabyteConnectionManager {
    url: String,
    ce: Arc<ClassificationParser>,
    locks: Arc<Locks>,
}

pub type YugabytePool = bb8::Pool<YugabyteConnectionManager>;

impl YugabyteConnectionManager {
    pub fn new(url: String, ce: Arc<ClassificationParser>) -> Self {
        Self{url, ce, locks: Arc::new(Locks::new())}
    }
}

impl bb8::ManageConnection for YugabyteConnectionManager {
    type Connection = Yugabyte;
    type Error = PoolError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match Yugabyte::connect(&self.url, self.ce.clone(), self.locks.clone()).await {
            Ok(con) => Ok(con),
            Err(err) => Err(PoolError::LibraryError(err)),
        }
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        if conn.client.is_closed() {
            Err(PoolError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.client.is_closed()
    }
}