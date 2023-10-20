
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::datastore::{File, Submission, User, Error, Service};
use chrono::{Duration, DateTime, Utc};
use elasticsearch::{Elasticsearch, http};
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::{Serialize};
use serde_json::json;

use crate::error::{Result, Error as ErrorKind};

enum Index {
    Hot = 1,
    Archive = 2,
    HotAndArchive = 3
}

const DEFAULT_SEARCH_FIELD: &str = "__text__";
const KEEP_ALIVE: &str = "5m";
const DEFAULT_SORT: [&str; 1] = ["_id: asc"];


// def parse_sort(sort, ret_list=True):
//     """
//     This function tries to do two things at once:
//         - convert AL sort syntax to elastic,
//         - convert any sorts on the key _id to id
//     """
//     if sort is None:
//         return sort

//     if isinstance(sort, list):
//         return [parse_sort(row, ret_list=False) for row in sort]
//     elif isinstance(sort, dict):
//         return {('id' if key == '_id' else key): value for key, value in sort.items()}
//     elif "," in sort:
//         return [parse_sort(row.strip(), ret_list=False) for row in sort.split(',')]

//     parts = sort.split(' ')
//     if len(parts) == 1:
//         if parts == '_id':
//             if ret_list:
//                 return ['id']
//             return 'id'
//         if ret_list:
//             return [parts]
//         return parts
//     elif len(parts) == 2:
//         if parts[1] not in ['asc', 'desc']:
//             raise SearchException('Unknown sort parameter ' + sort)
//         if parts[0] == '_id':
//             if ret_list:
//                 return [{'id': parts[1]}]
//             return {'id': parts[1]}
//         if ret_list:
//             return [{parts[0]: parts[1]}]
//         return {parts[0]: parts[1]}
//     raise SearchException('Unknown sort parameter ' + sort)

macro_rules! with_retries_on {
    ($helper:ident, $index_name:expr, $expression:expr) => {
        with_retries_detail!($helper, Some($index_name), false, $expression)
    }
}

macro_rules! with_retries_raise_confict {
    ($helper:ident, $index_name:expr, $expression:expr) => {{
        with_retries_detail!($helper, Some($index_name), true, $expression)
    }}
}

/// This function evaluates the passed expression and reconnect if it fails
macro_rules! with_retries_detail {
    ($helper:ident, $index_name:expr, $raise_conflicts:expr, $expression:expr) => {{
        let mut retries = 0;
        let updated = 0;
        let deleted = 0;
        loop {

            let result = $expression;

            // Unify server and client errors
            let response = match result {
                // valid response or server error
                Ok(response) => response.error_for_status_code(),
                // client error
                Err(err) => Err(err)
            };

            // handle non errors, extract errors
            let err = match result {
                Ok(response) => {
                    if retries > 0 {
                        info!("Reconnected to elasticsearch!");
                    }

                    //     if updated:
                    //         ret_val['updated'] += updated

                    //     if deleted:
                    //         ret_val['deleted'] += deleted
                    break response.json().await
                },
                Err(err) => err
            };

            // A timeout
            if err.is_timeout() {
                warn!("Elasticsearch connection timeout, server(s): {}, retrying...", " | ".join(helper.get_hosts(safe=True)));
            }

            // handle error types that have HTTP status codes associated
            else if let Some(status) = err.status_code() {
                if http::StatusCode::NOT_FOUND == status {
                    let err_message = err.to_string();

                    // Validate exception type
                    if $index_name.is_some() || !err_message.contains("No search context found") {
                        break Err(ErrorKind::SearchException(err_message))
                    }

                    warn!("Index {} was removed while a query was running, retrying...", $index_name)
                } else if http::StatusCode::CONFLICT == status {
                    if raise_conflicts:
                        // De-sync potential treads trying to write to the index
                        time.sleep(random() * 0.1)
                        raise VersionConflictException(str(ce))
                    updated += ce.info.get('updated', 0)
                    deleted += ce.info.get('deleted', 0)

                } else if http::StatusCode::FORBIDDEN == status {
                    index_name = kwargs.get('index', '').upper()
                    if not index_name:
                        raise

                    log.warning("Elasticsearch cluster is preventing writing operations "
                                f"on index {index_name}, retrying...")

                } else if http::StatusCode::SERVICE_UNAVAILABLE == status {
                    index_name = kwargs.get('index', '').upper()

                    # Display proper error message
                    log.warning(f"Looks like index {index_name} is not ready yet, retrying...")

                } else if http::StatusCode::TOO_MANY_REQUESTS == status {
                    index_name = kwargs.get('index', '').upper()
                    err_code = err.meta.status

                    log.warning("Elasticsearch is too busy to perform the requested "
                                f"task on index {index_name}, retrying...")

                } else {
                    break Err()
                }
            } else {
                break Err()
            }

            // retry
            tokio::time::sleep(min(retries, self.MAX_RETRY_BACKOFF)).await;
            $helper.connection_reset().await?;
            retries += 1;


            // except (elasticsearch.exceptions.ConnectionError,
            //         elasticsearch.exceptions.AuthenticationException) as e:
            //     log.warning(f"No connection to Elasticsearch server(s): "
            //                 f"{' | '.join(self.get_hosts(safe=True))}"
            //                 f", because [{e}] retrying {func.__name__}...")

            //     time.sleep(min(retries, self.MAX_RETRY_BACKOFF))
            //     self.connection_reset()
            //     retries += 1


        }
    }}
}



/// Wrapper around the elasticsearch client for helper methods used across contexts
/// This struct is deliberately private to this module
struct ElasticHelper {
    pub es: Elasticsearch
}

impl ElasticHelper {

    /// This function should perform a search through the datastore and stream
    /// all related results as a dictionary of key value pair where each keys
    /// are one of the field specified in the field list parameter.
    ///
    /// >>> # noinspection PyUnresolvedReferences
    /// >>> {
    /// >>>     fl[0]: value,
    /// >>>     ...
    /// >>>     fl[x]: value
    /// >>> }
    ///
    /// :param query: lucene query to search for
    /// :param fl: list of fields to return from the search
    /// :param filters: additional queries to run on the original query to reduce the scope
    /// :param access_control: access control parameters to run the query with
    /// :param item_buffer_size: number of items to buffer with each search call
    /// :param as_obj: Return objects instead of dictionaries
    /// :param index_type: Type of indices to target
    /// :return: a generator of dictionary of field list results
    fn stream_search_full<T>(&self,
        index: String,
        query: String,
        // fl: Vec<String>,
        mut filters: Vec<String>,
        access_control: Option<String>,
        item_buffer_size: Option<usize>,
        index_type: Option<Index>,
    ) -> Result<ScrollCursor<T>> {
        let item_buffer_size = item_buffer_size.unwrap_or(200);
        let index_type = index_type.unwrap_or(Index::Hot);

        if item_buffer_size > 2000 || item_buffer_size < 50 {
            return Err(ErrorKind::SearchException("Variable item_buffer_size must be between 50 and 2000.".to_string()))
        }

        let index = self.get_joined_index(index_type);

        if let Some(access_control) = access_control {
            filters.push(access_control);
        }

        let formatted_filters = vec![];
        for ff in filters {
            formatted_filters.push(json!({"query_string": {"query": ff}}))
        }

        let query_expression = json!({
            "bool": {
                "must": {
                    "query_string": {
                        "query": query,
                        "default_field": DEFAULT_SEARCH_FIELD
                    }
                },
                "filter": formatted_filters
            }
        });

        let sort = parse_sort(DEFAULT_SORT)?;
        // let source = match fl {
        //     Some(fl) => fl,
        //     None => list(self.stored_fields.keys())
        // };

        Ok(ScrollCursor::new())
    }


    async fn with_retries()
}


struct ScrollCursor<T> {
    helper: Arc<ElasticHelper>,
    _data: PhantomData<T>
}

impl<T> ScrollCursor<T> {

    async fn new(helper: Arc<ElasticHelper>,
        index: String,
        query: String,
        sort: Vec<String>,
        source: String,
        keep_alive: Option<String>,
        size: Option<usize>,
        // timeout=None
    ) -> Self {
        let keep_alive = keep_alive.unwrap_or(KEEP_ALIVE.to_owned());
        let size = size.unwrap_or(1000);

        let pit = with_retries_on!(helper, index, helper.es.open_point_in_time(elasticsearch::OpenPointInTimeParts::Index(&[&index])).keep_alive(&keep_alive).send().await);


    //     # Generate the point in time
    //     pit = {'id': self.with_retries(self.datastore.client.open_point_in_time,
    //                                    index=index, keep_alive=keep_alive)['id'],
    //            'keep_alive': keep_alive}


        todo!()
    }

    async fn next() -> Result<Option<T>> {
        // for value in self.scan_with_search_after(query=query_expression, sort=sort, source=source,
        //     index=index, size=item_buffer_size):
        // # Unpack the results, ensure the id is always set
        // yield self._format_output(value, fl, as_obj=as_obj)
            todo!()
    }

    //     # Add tie_breaker sort using _shard_doc ID
    //     sort.append({"_shard_doc": "desc"})

    //     # initial search
    //     resp = self.with_retries(self.datastore.client.search, query=query, pit=pit,
    //                              size=size, timeout=timeout, sort=sort, _source=source)
    //     try:
    //         while resp["hits"]["hits"]:
    //             search_after = resp['hits']['hits'][-1]['sort']
    //             for hit in resp["hits"]["hits"]:
    //                 yield hit

    //             resp = self.with_retries(self.datastore.client.search, query=query, pit=pit,
    //                                      size=size, timeout=timeout, sort=sort, _source=source,
    //                                      search_after=search_after)

    //     finally:
    //         try:
    //             self.with_retries(self.datastore.client.close_point_in_time, id=pit['id'])
    //         except elasticsearch.exceptions.NotFoundError:
    //             pass
    // }
}


pub struct Elastic {
    es: Arc<ElasticHelper>,
    pub file: Collection<File>,
    pub submission: Collection<Submission>,
    pub user: Collection<User>,
    pub error: Collection<Error>,
}

impl Elastic {
    fn connect(url: String) -> Arc<Self> {
        todo!()
    }

    pub async fn list_all_services(&self) -> Result<Vec<Service>> {
        // List all services from service delta (Return all fields if full is true)
        let service_deltas = self.es.stream_search_full("service_delta", "id:*", vec![], None, None, None).await?.collect();

        // service_delta = list(self.service_delta.stream_search("id:*", fl="*" if full else None))

        // Gather all matching services and apply a mask if we don't want the full source object
        service_data = [Service(s, mask=mask)
                        for s in self.service.multiget([f"{item.id}_{item.version}" for item in service_delta],
                                                       as_obj=False, as_dictionary=False)]

        // Recursively update the service data with the service delta while stripping nulls
        services = [recursive_update(data.as_primitives(strip_null=True), delta.as_primitives(strip_null=True),
                                     stop_keys=['config'])
                    for data, delta in zip(service_data, service_delta)]

        // Return as an objet if needs be...
        return [Service(s, mask=mask) for s in services]
    }

    pub async fn list_enabled_services(&self) -> Result<Vec<Service>> {
        let mut services = self.list_all_services().await?;
        services.retain(|service|service.enabled);
        Ok(services)
    }
}

pub struct Collection<T: Serialize + DeserializeOwned> {
    database: Arc<Elasticsearch>,
    name: String,
    _data: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Collection<T> {

    pub fn new(es: Arc<Elasticsearch>, name: String) -> Self {
        Collection {
            database: es,
            name,
            _data: Default::default()
        }
    }


    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        todo!();
    }

    pub async fn save(&self, key: &str, value: &T) -> Result<()> {
        todo!();
    }

    pub async fn save_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        todo!();
    }
}


// pub struct CachedCollection<T: Serialize + DeserializeOwned> {
//     database: Arc<Elastic>,
//     cache_time: Duration,
//     name: String,
//     cached: HashMap<String, (DateTime<Utc>, Arc<T>)>,
// }

// impl<T: Serialize + DeserializeOwned> CachedCollection<T> {

//     pub fn new(ds: Arc<Elastic>, name: String, expiry: Duration) -> Self {
//         Self {
//             database: ds,
//             name,
//             cache_time: expiry,
//             cached: Default::default(),
//         }
//     }


//     pub async fn get(&self, key: &str) -> Result<Option<Arc<T>>> {
//         todo!();
//     }

// }
