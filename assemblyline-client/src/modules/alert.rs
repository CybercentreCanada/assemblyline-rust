// from assemblyline_client.v4_client.common.utils import api_path_by_module, get_function_kwargs, api_path

use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::datastore::workflow::{Priorities, Statuses};
use assemblyline_models::datastore::alert::Alert as AlertModel;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_with::{SerializeDisplay, DeserializeFromStr};

use crate::JsonMap;
use crate::connection::{Connection, convert_api_output_obj, convert_api_output_map, Body};
use crate::types::Result;

use super::api_path;
use super::search::SearchResult;

const ALERT_INDEX: &str = "alert";

pub struct Alert {
    connection: Arc<Connection>,

    pub batch: Batch,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum Verdict {
    Malicious, NonMalicious
}

impl Alert {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {
            batch: Batch::new(connection.clone()),
            connection,
        }
    }


    /// Return the full alert for a given alert_id.
    ///
    /// Required:
    /// alert_id: Alert key. (string)
    ///
    /// Throws a Client exception if the alert does not exist.
    pub async fn get(&self, alert_id: &str) -> Result<AlertModel> {
        return self.connection.get(&api_path!(ALERT_INDEX, alert_id), convert_api_output_obj).await
    }

    /// List all alert grouped by a given field
    ///
    /// Required:
    /// field:    Field to group the alerts by
    pub fn grouped(&self, field: String) -> DetailedAlertQuery<GroupSearchResult<AlertModel>> {
        DetailedAlertQuery {
            connection: self.connection.clone(),
            path: api_path!(ALERT_INDEX, "grouped", field),
            _type: Default::default(),
            filters: vec![],
            query: None,
            no_delay: false,
            offset: 0,
            rows: 10,
            tc_start: None,
            tc: None,
            use_archive: false,
            track_total_hits: None,
        }
    }

    /// Add label(s) to the alert with the given alert_id.
    ///
    /// Required:
    /// alert_id: Alert key (string)
    /// *labels : One or more labels (variable argument list of strings)
    ///
    /// Throws a Client exception if the alert does not exist.
    pub async fn label(&self, alert_id: &str, labels: Vec<String>) -> Result<JsonMap> {
        return self.connection.post(&api_path!(ALERT_INDEX, "label", alert_id), Body::Json(labels), convert_api_output_map).await
    }

    /// Find the different labels matching the query.
    pub async fn labels(&self) -> AlertQuery<JsonMap> {
        AlertQuery::get(self.connection.clone(), api_path!(ALERT_INDEX, "labels"))
    }

    /// List all alerts in the system (per page)
    pub fn list(&self) -> DetailedAlertQuery<SearchResult<Alert>> {
        DetailedAlertQuery {
            connection: self.connection.clone(),
            path: api_path!(ALERT_INDEX, "list"),
            _type: Default::default(),
            filters: vec![],
            query: None,
            no_delay: false,
            offset: 0,
            rows: 10,
            tc_start: None,
            tc: None,
            use_archive: false,
            track_total_hits: None
        }
    }

    // Set the ownership of the alert with the given alert_id to the current user.
    // Required:
    // alert_id: Alert key (string)
    // Throws a Client exception if the alert does not exist.
    pub async fn ownership(&self, alert_id: String) -> Result<JsonMap> {
        return self.connection.get(&api_path!(ALERT_INDEX, "ownership", alert_id), convert_api_output_map).await
    }

    // Set the priority of the alert with the given alert_id.
    // Required:
    // alert_id: Alert key (string)
    // priority: Priority (enum: LOW, MEDIUM, HIGH, CRITICAL)
    // Throws a Client exception if the alert does not exist.
    pub async fn priority(&self, alert_id: String, priority: Priorities) -> Result<JsonMap> {
        return self.connection.post(&api_path!(ALERT_INDEX, "priority", alert_id), Body::Json(priority), convert_api_output_map).await
    }

    /// Find the different priorities matching the query.
    pub async fn priorities(&self) -> AlertQuery<JsonMap> {
        AlertQuery::get(self.connection.clone(), api_path!(ALERT_INDEX, "priorities"))
    }

    // Return the list of all IDs related to the currently selected query.
    pub async fn related(&self) -> AlertQuery<JsonMap> {
        AlertQuery::get(self.connection.clone(), api_path!(ALERT_INDEX, "related"))
    }

    // Find the different statistics for the alerts matching the query.
    pub async fn statistics(&self) -> AlertQuery<JsonMap> {
        AlertQuery::get(self.connection.clone(), api_path!(ALERT_INDEX, "statistics"))
    }

    // Set the status of the alert with the given alert_id.
    // Required:
    // alert_id: Alert key (string)
    // status  : Status (enum: MALICIOUS, NON-MALICIOUS, ASSESS)
    // Throws a Client exception if the alert does not exist.
    pub async fn status(&self, alert_id: String, status: Statuses) -> Result<JsonMap> {
        return self.connection.post(&api_path!(ALERT_INDEX, "status", alert_id), Body::Json(status), convert_api_output_map).await
    }

    /// Find the different statuses matching the query.
    pub async fn statuses(&self) -> AlertQuery<JsonMap> {
        AlertQuery::get(self.connection.clone(), api_path!(ALERT_INDEX, "statuses"))
    }

    /// Set the verdict of the alert with the given alert_id.
    pub async fn verdict(&self, alert_id: String, verdict: Verdict) -> Result<JsonMap> {
        return self.connection.post(&api_path!(ALERT_INDEX, "verdict", alert_id, verdict.to_string()), Body::<()>::None, convert_api_output_map).await
    }
}

pub struct Batch {
    connection: Arc<Connection>,
}

impl Batch {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
        }
    }

    // Add labels to alerts matching the search criteria.
    // Required:
    // q       : Query used to limit the scope of the data (string)
    // labels  : Labels to apply (list of strings)
    pub async fn label(&self, query: String, labels: Vec<String>) -> AlertQuery<JsonMap, Vec<String>> {
        AlertQuery::post(self.connection.clone(), api_path!(ALERT_INDEX, "label", "batch"), labels).query(query)
    }

    // Set ownership on alerts matching the search criteria.
    // Required:
    // q       : Query used to limit the scope of the data (string)
    pub async fn ownership(&self, query: String) -> AlertQuery<JsonMap, Vec<String>> {
        AlertQuery::get(self.connection.clone(), api_path!(ALERT_INDEX, "ownership", "batch")).query(query)
    }

    // Set the priority on alerts matching the search criteria.
    // Required:
    // q       : Query used to limit the scope of the data (string)
    // priority: Priority (enum: LOW, MEDIUM, HIGH, CRITICAL)
    pub async fn priority(&self, query: String, priority: Priorities) -> AlertQuery<JsonMap, Priorities> {
        AlertQuery::post(self.connection.clone(), api_path!(ALERT_INDEX, "priority", "batch"), priority).query(query)
    }

    // Set the status on alerts matching the search criteria.
    // Required:
    // q       : Query used to limit the scope of the data (string)
    // status  : Status (enum: MALICIOUS, NON-MALICIOUS, ASSESS)
    pub async fn status(&self, query: String, status: Statuses) -> AlertQuery<JsonMap, Statuses> {
        AlertQuery::post(self.connection.clone(), api_path!(ALERT_INDEX, "status", "batch"), status).query(query)
    }
}

#[derive(Deserialize)]
pub struct GroupSearchResult<Type> {

    pub counted_total: u64,

    pub tc_start: DateTime<Utc>,

    #[serde(flatten)]
    result: SearchResult<Type>,
}

impl<Type> std::ops::Deref for GroupSearchResult<Type> {
    type Target = SearchResult<Type>;

    fn deref(&self) -> &Self::Target {
        &self.result
    }
}

pub struct DetailedAlertQuery<Type> {
    connection: Arc<Connection>,
    path: String,
    _type: PhantomData<Type>,

    /// Post filter queries (you can have multiple of these)
    filters: Vec<String>,
    /// Query to apply to the alert list
    query: Option<String>,
    /// Do not delay alerts
    no_delay: bool,
    /// Offset at which we start giving alerts
    offset: u64,
    /// Number of alerts to return
    rows: u64,
    /// Time offset at which we start the time constraint
    tc_start: Option<DateTime<Utc>>,
    /// Time constraint applied to the API
    tc: Option<DateTime<Utc>>,
    /// Also query the archive
    use_archive: bool,
    /// Number of hits to track (default: 10k)
    track_total_hits: Option<u64>,
}

impl<Type: DeserializeOwned> DetailedAlertQuery<Type> {
    /// Post filter queries (you can have multiple of these)
    pub fn filters(mut self, value: Vec<String>) -> Self {
        self.filters.extend(value); self
    }
    /// Query to apply to the alert list
    pub fn query(mut self, value: String) -> Self {
        self.query = Some(value); self
    }
    /// Do not delay alerts
    pub fn no_delay(mut self, value: bool) -> Self {
        self.no_delay = value; self
    }
    /// Offset at which we start giving alerts
    pub fn offset(mut self, value: u64) -> Self {
        self.offset = value; self
    }
    /// Number of alerts to return
    pub fn rows(mut self, value: u64) -> Self {
        self.rows = value; self
    }
    /// Time offset at which we start the time constraint
    pub fn tc_start(mut self, value: DateTime<Utc>) -> Self {
        self.tc_start = Some(value); self
    }
    /// Time constraint applied to the API
    pub fn tc(mut self, value: DateTime<Utc>) -> Self {
        self.tc = Some(value); self
    }
    /// Also query the archive
    pub fn use_archive(mut self, value: bool) -> Self {
        self.use_archive = value; self
    }
    /// Number of hits to track (default: 10k)
    pub fn track_total_hits(mut self, value: u64) -> Self {
        self.track_total_hits = Some(value); self
    }

    pub async fn send(self) -> Result<Type> {

        let mut params: Vec<(String, String)> = vec![
            ("offset".to_owned(), self.offset.to_string()),
            ("rows".to_owned(), self.rows.to_string()),
        ];

        for x in self.filters {
            params.push(("fq".to_owned(), x));
        }
        if let Some(q) = self.query {
            params.push(("q".to_owned(), q));
        }
        if let Some(tc_start) = self.tc_start {
            params.push(("tc_start".to_owned(), tc_start.to_rfc3339()));
        }
        if let Some(tc) = self.tc {
            params.push(("tc".to_owned(), tc.to_rfc3339()));
        }

        if self.no_delay {
            params.push(("no_delay".to_owned(), "true".to_owned()));
        }
        if self.use_archive {
            params.push(("use_archive".to_owned(), "".to_owned()));
        }
        if let Some(track_total_hits) = self.track_total_hits {
            params.push(("track_total_hits".to_owned(), track_total_hits.to_string()));
        }

        return self.connection.get_params(&self.path, params, convert_api_output_obj).await
    }
}

pub struct AlertQuery<Type, Post=()> {
    connection: Arc<Connection>,
    post: Option<Post>,
    path: String,
    _type: PhantomData<Type>,

    /// Post filter queries (you can have multiple of these)
    filters: Vec<String>,
    /// Query to apply to the alert list
    query: Option<String>,
    /// Time offset at which we start the time constraint
    tc_start: Option<DateTime<Utc>>,
    /// Time constraint applied to the API
    tc: Option<DateTime<Utc>>,
    /// Do not delay alerts
    no_delay: bool,
}

impl<Type: DeserializeOwned, Post: Serialize> AlertQuery<Type, Post> {
    fn get(connection: Arc<Connection>, path: String) -> Self {
        AlertQuery {
            connection,
            post: None,
            path,
            _type: PhantomData,
            filters: vec![],
            query: None,
            tc_start: None,
            tc: None,
            no_delay: false,
        }
    }

    fn post(connection: Arc<Connection>, path: String, body: Post) -> Self {
        AlertQuery {
            connection,
            post: Some(body),
            path,
            _type: PhantomData,
            filters: vec![],
            query: None,
            tc_start: None,
            tc: None,
            no_delay: false,
        }
    }

    /// Post filter queries (you can have multiple of these)
    pub fn filters(mut self, value: Vec<String>) -> Self {
        self.filters.extend(value); self
    }
    /// Query to apply to the alert list
    pub fn query(mut self, value: String) -> Self {
        self.query = Some(value); self
    }
    /// Do not delay alerts
    pub fn no_delay(mut self, value: bool) -> Self {
        self.no_delay = value; self
    }
    /// Time offset at which we start the time constraint
    pub fn tc_start(mut self, value: DateTime<Utc>) -> Self {
        self.tc_start = Some(value); self
    }
    /// Time constraint applied to the API
    pub fn tc(mut self, value: DateTime<Utc>) -> Self {
        self.tc = Some(value); self
    }


    pub async fn send(self) -> Result<Type> {

        let mut params = vec![];

        for x in self.filters {
            params.push(("fq".to_owned(), x));
        }

        if let Some(q) = self.query {
            params.push(("q".to_owned(), q));
        }
        if let Some(tc_start) = self.tc_start {
            params.push(("tc_start".to_owned(), tc_start.to_rfc3339()));
        }
        if let Some(tc) = self.tc {
            params.push(("tc".to_owned(), tc.to_rfc3339()));
        }

        if self.no_delay {
            params.push(("no_delay".to_owned(), "true".to_owned()))
        }

        if let Some(body) = self.post {
            return self.connection.post_params(&self.path, Body::Json(body), params, convert_api_output_obj).await
        } else {
            return self.connection.get_params(&self.path, params, convert_api_output_obj).await
        }
    }

}

