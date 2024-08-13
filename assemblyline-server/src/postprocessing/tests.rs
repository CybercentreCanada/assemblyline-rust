
use std::{collections::HashMap, sync::Arc};

use assemblyline_models::config::{NamedValue, PostprocessAction, Webhook};
use chrono::{Utc, Duration};
use poem::{listener::{Acceptor, Listener}, EndpointExt};
use rand::thread_rng;
use serde_json::json;
use rand::Rng;

use crate::{postprocessing::{search::CacheAvailabilityStatus, ActionWorker, ParsingError, SubmissionFilter}, Core};

use super::parse;


#[test]
fn test_simple_filters() {

    let mut sub = json!({"max_score": 100, "times": {"completed": Utc::now()}});

    // Build our test filter
    let fltr = parse("times.completed: [now-1d TO 2055-06-20T10:10:10.000] AND max_score: [10 TO 100]").unwrap();

    // Make sure our use of non-cached properties is detected
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::ErrorUsesForbiddenFields(vec!["times.completed".to_owned()]));

    // Make sure it works for our starting test data
    assert!(fltr.test(&sub).unwrap());

    // Test some different values
    sub.as_object_mut().unwrap().insert("max_score".to_owned(), json!(101));
    assert!(!fltr.test(&sub).unwrap());
    sub.as_object_mut().unwrap().insert("max_score".to_owned(), json!(11));
    assert!(fltr.test(&sub).unwrap(), "{sub}");
    sub = json!({"max_score": 11, "times": {"completed": Utc::now() - Duration::try_days(2).unwrap()}});
    assert!(!fltr.test(&sub).unwrap());


    // Ranges with numbers and strings
    let fltr = parse("max_score: {10 TO 100} OR metadata.stuff: {\"big cats\" TO cats}").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);

    assert!(fltr.test(&sub).unwrap());
    sub = json!({"max_score": 10, "times": {"completed": Utc::now() - Duration::try_days(2).unwrap()}});
    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"max_score": 10, "metadata": {"stuff": "cats"}});
    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"max_score": 10, "metadata": {"stuff": "big dogs"}});
    assert!(fltr.test(&sub).unwrap(), "{sub} {fltr:?}");
    sub = json!({"max_score": 10, "metadata": {"stuff": "aig dogs"}});
    assert!(!fltr.test(&sub).unwrap());

    // Try a prefix operator and wildcard matches
    let fltr = parse("max_score: >100 AND NOT results: *virus*").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::ErrorUsesForbiddenFields(vec!["results".to_owned()]));

    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"max_score": 101, "results": ["a-virus-service"]});
    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"max_score": 101, "results": ["a-something-service"]});
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("max_score: >100 AND results: *virus*").unwrap();
    sub = json!({"max_score": 101, "results": ["a-virus-service"]});
    assert!(fltr.test(&sub).unwrap());
    sub = json!({"max_score": 101, "results": ["a-something-service"]});
    assert!(!fltr.test(&sub).unwrap());

    // different prefix operator
    let fltr = parse("files.size:>100").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);

    sub = json!({"files": []});
    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"files": [{"name": "abc", "size": 100, "sha256": "0".repeat(64)}]});
    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"files": [{"name": "abc", "size": 100, "sha256": "0".repeat(64)}, {"name": "abc", "size": 101, "sha256": "0".repeat(64)}]});
    assert!(fltr.test(&sub).unwrap());

    // Include logic within a field, the cats should be a global search, and hit on the description
    let fltr = parse("metadata.stuff: (things OR stuff) AND cats").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::WarningUsesUnqualifiedSearch);

    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"params": {"description": "Full of cats."}});
    assert!(!fltr.test(&sub).unwrap());
    sub = json!({"params": {"description": "Full of cats."}, "metadata": {"stuff": "things"}});
    assert!(fltr.test(&sub).unwrap(), "{sub} {fltr:?}");

    // Try a bunch of different ways to format the same thing
    sub = json!({"metadata": {"stuff": "big-bad"}});
    let fltr = parse("metadata.stuff: \"big-bad\"").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("metadata.stuff: big-bad").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("metadata.stuff: big\\-bad").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("metadata.stuff: \"big bad\"").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);
    sub = json!({"metadata": {"stuff": "big bad"}});
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("metadata.stuff: big\\ bad").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);
    assert!(fltr.test(&sub).unwrap());
}


#[test]
fn test_date_boundaries() {

    let sub_below = json!({"times": { "completed": "2054-06-20T10:10:11.049Z" }});
    let sub_low = json!({"times": { "completed": "2054-06-20T10:10:11.050Z" }});
    let sub_inside = json!({"times": { "completed": "2054-07-20T10:10:10.000Z" }});
    let sub_high = json!({"times": { "completed": "2055-06-20T10:10:10.000Z" }});
    let sub_over = json!({"times": { "completed": "2055-06-20T10:10:10.001Z" }});

    // Build our test filter
    let fltr = parse("times.completed: [2054-06-20T10:10:11.050Z TO 2055-06-20T10:10:10.000Z]").unwrap();
    assert!(!fltr.test(&sub_below).unwrap());
    assert!(fltr.test(&sub_low).unwrap());
    assert!(fltr.test(&sub_inside).unwrap());
    assert!(fltr.test(&sub_high).unwrap());
    assert!(!fltr.test(&sub_over).unwrap());

    let fltr = parse("times.completed: {2054-06-20T10:10:11.050 TO 2055-06-20T10:10:10.000}").unwrap();
    assert!(!fltr.test(&sub_below).unwrap());
    assert!(!fltr.test(&sub_low).unwrap());
    assert!(fltr.test(&sub_inside).unwrap());
    assert!(!fltr.test(&sub_high).unwrap());
    assert!(!fltr.test(&sub_over).unwrap());
}

#[test]
fn test_bad_field_detection() {
    assert_eq!(parse("max_score.pain:found").unwrap_err(), ParsingError::SubmissionFilterUsesUnknownFields(vec!["max_score.pain".into()]));
    assert_eq!(parse("max_score_pain:found").unwrap_err(), ParsingError::SubmissionFilterUsesUnknownFields(vec!["max_score_pain".into()]));
}

#[test]
fn test_tag_filters() {
    let sub = json!({"max_score": 100});
    let sub_tags = json!({
        "max_score": 100,
        "tags": {
            "vector": ["things"],
            "technique": {"packer": ["giftwrap"]},
        }
    });
    // tags = [
    //     {'safelisted': False, 'type': '', 'value': 'things', 'short_type': 'vector'},
    //     {'safelisted': False, 'type': '', 'value': 'giftwrap', 'short_type': 'packer'}
    // ];

    let fltr = parse("max_score: >=100 AND tags.vector: *").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::ErrorUsesForbiddenFields(vec!["tags.vector".into()]));

    assert!(!fltr.test(&sub).unwrap());
    assert!(fltr.test(&sub_tags).unwrap());

    let fltr = parse("max_score: >=100 AND things").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::WarningUsesUnqualifiedSearch);

    assert!(!fltr.test(&sub).unwrap());
    assert!(fltr.test(&sub_tags).unwrap());

    let fltr = parse("max_score: >=100 AND things AND giftwrap").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::WarningUsesUnqualifiedSearch);

    assert!(!fltr.test(&sub).unwrap());
    assert!(fltr.test(&sub_tags).unwrap());

    let fltr = parse("max_score: >=100 AND tags.technique.packer: *wrap").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::ErrorUsesForbiddenFields(vec!["tags.technique.packer".into()]));

    assert!(!fltr.test(&sub).unwrap());
    assert!(fltr.test(&sub_tags).unwrap());
}

#[test]
fn test_message_filter() {
    let fltr = parse("max_score: [500 TO *] AND metadata.stream: (10 OR 99)").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);

    let sub = json!({"max_score": 100});
    assert!(!fltr.test(&sub).unwrap());

    let sub = json!({"max_score": 600});
    assert!(!fltr.test(&sub).unwrap());

    let sub = json!({"max_score": 100, "metadata": {"stream": 99}});
    assert!(!fltr.test(&sub).unwrap());

    let sub = json!({"max_score": 600, "metadata": {"stream": 99}});
    assert!(fltr.test(&sub).unwrap());
}

#[test]
fn test_regex_filter() {
    let sub = json!({});

    let fltr = parse("metadata.other: /ab+c/").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);

    assert!(!fltr.test(&sub).unwrap());

    let sub = json!({"metadata": {"other": "ac"}});
    assert!(!fltr.test(&sub).unwrap());

    let sub = json!({"metadata": {"other": "abbbc"}});
    assert!(fltr.test(&sub).unwrap());
}

#[test]
fn test_exists() {

    let fltr = parse("_exists_: metadata.other").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);

    assert_eq!(parse("_exists_: nteohusotehuos").unwrap_err(), ParsingError::SubmissionFilterUsesUnknownFields(vec!["nteohusotehuos".to_string()]));
    // assert_eq!(parse("_exists_: sid").unwrap_err(), PostprocessError::AlwaysTrue("_exists_: sid".to_string()));

    assert_eq!(parse("cats OR _exists_: nteohusotehuos").unwrap_err(), ParsingError::SubmissionFilterUsesUnknownFields(vec!["nteohusotehuos".to_string()]));
    // assert_eq!(parse("cats OR _exists_: sid").unwrap_err(), PostprocessError::AlwaysTrue("_exists_: sid".to_string()));

    let sub = json!({
        "metadata": {
            "cats": "good"
        }
    });

    let fltr = parse("_exists_: metadata.other").unwrap();
    assert_eq!(fltr.cache_safe(), CacheAvailabilityStatus::Ok);
    assert!(!fltr.test(&sub).unwrap());

    let fltr = parse("NOT _exists_: metadata.other").unwrap();
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("_exists_: metadata.cats").unwrap();
    assert!(fltr.test(&sub).unwrap());

    let fltr = parse("NOT _exists_: metadata.cats").unwrap();
    assert!(!fltr.test(&sub).unwrap());
}

#[test]
fn test_date_truncate() {
    let fltr = parse("times.completed: [2020-08-08T10:10:10.000Z TO 2020-08-09T10:10:10.000Z]").unwrap();
    let sub = json!({
        "times": {
            "completed": "2020-08-08T09:10:10.000Z"
        }
    });
    assert!(!fltr.test(&sub).unwrap());

    let fltr = parse("times.completed: [2020-08-08T10:10:10.000Z||/d TO 2020-08-09T10:10:10.000Z]").unwrap();
    assert!(fltr.test(&sub).unwrap());
}

type HitList = tokio::sync::mpsc::Receiver<(poem::http::HeaderMap, Vec<u8>)>;
type HitListSend = tokio::sync::mpsc::Sender<(poem::http::HeaderMap, Vec<u8>)>;

async fn run_server() -> (u16, HitList) {
    use poem::{handler, http::{StatusCode, HeaderMap}, Body, web::Data, Route, post, Server};

    #[handler]
    async fn hello(headers: &HeaderMap, body: Body, data: Data<&HitListSend>) -> StatusCode {
        let body = body.into_vec().await.unwrap();
        let _ = data.send((
            headers.clone(),
            body,
        )).await;
        StatusCode::OK
    }

    let (data, out) = tokio::sync::mpsc::channel(100);
    let listener = poem::listener::TcpListener::bind("0.0.0.0:0");
    let acceptor = listener.into_acceptor().await.unwrap();
    let port = acceptor.local_addr()[0].as_socket_addr().unwrap().port();

    tokio::spawn(async move {
        let app = Route::new().at("/", post(hello)).data(data);
        Server::new_with_acceptor(acceptor)
          .run(app)
          .await
    });

    (port, out)
}


#[tokio::test]
async fn test_hook() {
    use assemblyline_models::datastore::submission::Submission;

    let (port, mut hits) = run_server().await;
    
    let action = PostprocessAction{
        enabled: true,
        run_on_completed: true,
        filter: "metadata.do_hello: *".to_string(),
        webhook: Some(Webhook {
            uri: format!("http://localhost:{port}"),
            headers: vec![NamedValue{
                name: "care-of".to_string(), 
                value: "assemblyline".to_string()
            }],
            password: None,
            ca_cert: None,
            ssl_ignore_errors: false,
            ssl_ignore_hostname: false,
            proxy: None,
            method: "POST".to_string(),
            username: None,
            retries: None
        }),
        run_on_cache: false,
        raise_alert: false,
        resubmit: None,
        archive_submission: false
    };

    let (core, _redis_lock) = Core::test_setup().await;
    let worker = ActionWorker::new(false, &core).await.unwrap();

    {
        let mut actions = worker.actions.write().await;
        *actions = Arc::new(HashMap::from_iter([
            ("action".to_string(), (SubmissionFilter::new(&action.filter).unwrap(), action))
        ]));
    }

    {
        let mut sub: Submission = thread_rng().gen();
        sub.metadata.insert("ok".to_string(), serde_json::Value::String("bad".to_string()));
        worker.process(&sub, Default::default(), false).await.unwrap(); 
    }

    {
        let mut sub: Submission = thread_rng().gen();
        sub.metadata.insert("ok".to_string(), serde_json::Value::String("good".to_string()));
        sub.metadata.insert("do_hello".to_string(), serde_json::Value::String("yes".to_string()));
        worker.process(&sub, Default::default(), false).await.unwrap(); 
    }

    let (headers, body) = tokio::time::timeout(std::time::Duration::from_secs(3), hits.recv()).await.unwrap().unwrap();
    assert_eq!(headers.get("CARE-OF").unwrap(), "assemblyline");
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap().get("submission").unwrap().get("metadata").unwrap().get("ok").unwrap(), "good");

    // make sure no more messages are incoming
    assert!(tokio::time::timeout(std::time::Duration::from_secs(3), hits.recv()).await.is_err());
}


#[test]
fn test_webhook_match() {
    let webhook_first = json!({
        "uri": "http://api.interface.website"        
    });

    let webhook_second = json!({
        "uri": "http://api.interface.website",        
        "headers": [{
            "name": "APIKEY",
            "value": "1111111111111111111111111111111111111111111111111111111111111111"
        }]
    });

    let a: Webhook = serde_json::from_value(webhook_first.clone()).unwrap();
    let b: Webhook = serde_json::from_value(webhook_first).unwrap();
    let c: Webhook = serde_json::from_value(webhook_second).unwrap();

    assert_eq!(a, b);
    assert_ne!(a, c);
}