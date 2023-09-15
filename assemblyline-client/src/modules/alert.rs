// from assemblyline_client.v4_client.common.utils import api_path_by_module, get_function_kwargs, api_path

use std::marker::PhantomData;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::JsonMap;
use crate::connection::{Connection, convert_api_output_obj, convert_api_output_map};
use crate::types::Result;
use crate::models::alert::Alert as AlertModel;

use super::api_path;
use super::search::SearchResult;

const ALERT_INDEX: &str = "alert";

pub struct Alert {
    connection: Arc<Connection>,

    pub batch: Batch,
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
        return self.connection.post(&api_path!(ALERT_INDEX, "label", alert_id), crate::connection::Body::Json(labels), convert_api_output_map).await
    }

    /// Find the different labels matching the query.
    pub async fn labels(&self) -> AlertQuery<JsonMap> {
        AlertQuery {
            connection: self.connection.clone(),
            path: api_path!(ALERT_INDEX, "labels"),
            _type: PhantomData,
            filters: vec![],
            query: None,
            tc_start: None,
            tc: None,
            no_delay: false,
        }
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

//     def ownership(self, alert_id):
//         """\
// Set the ownership of the alert with the given alert_id to the current user.

// Required:
// alert_id: Alert key (string)

// Throws a Client exception if the alert does not exist.
// """
//         return self._connection.get(api_path_by_module(self, alert_id))

//     def priority(self, alert_id, priority):
//         """\
// Set the priority of the alert with the given alert_id.

// Required:
// alert_id: Alert key (string)
// priority: Priority (enum: LOW, MEDIUM, HIGH, CRITICAL)

// Throws a Client exception if the alert does not exist.
// """
//         return self._connection.post(api_path_by_module(self, alert_id), json=priority)

//     def priorities(self, fq=[], q=None, tc_start=None, tc=None, no_delay=False):
//         """\
// Find the different priorities matching the query.

// Optional:
// fq      : Post filter queries (you can have multiple of these)
// q       : Query to apply to the alert list
// tc_start: Time offset at which we start the time constraint
// tc      : Time constraint applied to the API
// no_delay: Do not delay alerts
// """
//         params_tuples = [('fq', x) for x in fq]
//         kw = {
//             'params_tuples': params_tuples,
//             'q': q,
//             'tc_start': tc_start,
//             'tc': tc
//         }
//         if no_delay:
//             kw['no_delay'] = True

//         return self._connection.get(api_path_by_module(self, **kw))

//     def related(self, fq=[], q=None, tc_start=None, tc=None, no_delay=False):
//         """\
// Return the list of all IDs related to the currently selected query.

// Optional:
// fq      : Post filter queries (you can have multiple of these)
// q       : Query to apply to the alert list
// tc_start: Time offset at which we start the time constraint
// tc      : Time constraint applied to the API
// no_delay: Do not delay alerts
// """
//         params_tuples = [('fq', x) for x in fq]
//         kw = {
//             'params_tuples': params_tuples,
//             'q': q,
//             'tc_start': tc_start,
//             'tc': tc
//         }
//         if no_delay:
//             kw['no_delay'] = True

//         return self._connection.get(api_path_by_module(self, **kw))

//     def statistics(self, fq=[], q=None, tc_start=None, tc=None, no_delay=False):
//         """\
// Find the different statistics for the alerts matching the query.

// Optional:
// fq      : Post filter queries (you can have multiple of these)
// q       : Query to apply to the alert list
// tc_start: Time offset at which we start the time constraint
// tc      : Time constraint applied to the API
// no_delay: Do not delay alerts
// """
//         params_tuples = [('fq', x) for x in fq]
//         kw = {
//             'params_tuples': params_tuples,
//             'q': q,
//             'tc_start': tc_start,
//             'tc': tc
//         }
//         if no_delay:
//             kw['no_delay'] = True

//         return self._connection.get(api_path_by_module(self, **kw))

//     def status(self, alert_id, status):
//         """\
// Set the status of the alert with the given alert_id.

// Required:
// alert_id: Alert key (string)
// status  : Status (enum: MALICIOUS, NON-MALICIOUS, ASSESS)

// Throws a Client exception if the alert does not exist.
// """
//         return self._connection.post(api_path_by_module(self, alert_id), json=status)

//     def statuses(self, fq=[], q=None, tc_start=None, tc=None, no_delay=False):
//         """\
// Find the different statuses matching the query.

// Optional:
// fq      : Post filter queries (you can have multiple of these)
// q       : Query to apply to the alert list
// tc_start: Time offset at which we start the time constraint
// tc      : Time constraint applied to the API
// no_delay: Do not delay alerts
// """
//         params_tuples = [('fq', x) for x in fq]
//         kw = {
//             'params_tuples': params_tuples,
//             'q': q,
//             'tc_start': tc_start,
//             'tc': tc
//         }
//         if no_delay:
//             kw['no_delay'] = True

//         return self._connection.get(api_path_by_module(self, **kw))

//     def verdict(self, alert_id, verdict):
//         """\
// Set the verdict of the alert with the given alert_id.

// Required:
// alert_id: Alert key (string)
// verdict : Verdict (enum: malicious, non_malicious)

// Throws a Client exception if the alert does not exist.
// """
//         return self._connection.put(api_path_by_module(self, alert_id, verdict))
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

//     def label(self, q, labels, tc=None, tc_start=None, fq_list=None):
//         """\
// Add labels to alerts matching the search criteria.

// Required:
// q       : Query used to limit the scope of the data (string)
// labels  : Labels to apply (list of strings)

// Optional:
// tc         : Time constraint applied to the query (string)
// tc_start   : Date which the time constraint will be applied to [Default: NOW] (string)
// fq_list    : List of filter queries (list of strings)
// """
//         if not fq_list:
//             fq_list = []

//         kw = get_function_kwargs('self', 'fq_list', 'labels')
//         path = api_path('alert/label/batch', params_tuples=[('fq', fq) for fq in fq_list], **kw)

//         return self._connection.post(path, json=labels)

//     def ownership(self, q, tc=None, tc_start=None, fq_list=None):
//         """\
// Set ownership on alerts matching the search criteria.

// Required:
// q       : Query used to limit the scope of the data (string)

// Optional:
// tc         : Time constraint applied to the query (string)
// tc_start   : Date which the time constraint will be applied to [Default: NOW] (string)
// fq_list    : List of filter queries (list of strings)
// """
//         if not fq_list:
//             fq_list = []

//         kw = get_function_kwargs('self', 'fq_list', 'ownership')
//         path = api_path('alert/ownership/batch', params_tuples=[('fq', fq) for fq in fq_list], **kw)

//         return self._connection.get(path)

//     def priority(self, q, priority, tc=None, tc_start=None, fq_list=None):
//         """\
// Set the priority on alerts matching the search criteria.

// Required:
// q       : Query used to limit the scope of the data (string)
// priority: Priority (enum: LOW, MEDIUM, HIGH, CRITICAL)

// Optional:
// tc         : Time constraint applied to the query (string)
// tc_start   : Date which the time constraint will be applied to [Default: NOW] (string)
// fq_list    : List of filter queries (list of strings)
// """
//         if not fq_list:
//             fq_list = []

//         kw = get_function_kwargs('self', 'fq_list', 'priority')
//         path = api_path('alert/priority/batch', params_tuples=[('fq', fq) for fq in fq_list], **kw)

//         return self._connection.post(path, json=priority)

//     def status(self, q, status, tc=None, tc_start=None, fq_list=None):
//         """\
// Set the status on alerts matching the search criteria.

// Required:
// q       : Query used to limit the scope of the data (string)
// status  : Status (enum: MALICIOUS, NON-MALICIOUS, ASSESS)

// Optional:
// tc         : Time constraint applied to the query (string)
// tc_start   : Date which the time constraint will be applied to [Default: NOW] (string)
// fq_list    : List of filter queries (list of strings)
// """
//         if not fq_list:
//             fq_list = []

//         kw = get_function_kwargs('self', 'fq_list', 'status')
//         path = api_path('alert/status/batch', params_tuples=[('fq', fq) for fq in fq_list], **kw)

//         return self._connection.post(path, json=status)
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

    pub async fn get(self) -> Result<Type> {

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

pub struct AlertQuery<Type> {
    connection: Arc<Connection>,
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

impl<Type: DeserializeOwned> AlertQuery<Type> {
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


    pub async fn get(self) -> Result<Type> {

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

        return self.connection.get_params(&self.path, params, convert_api_output_obj).await
    } 
    
}

