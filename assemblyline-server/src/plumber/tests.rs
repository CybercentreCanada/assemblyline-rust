use std::time::Duration;

use assemblyline_models::messages::task::Task;
use assemblyline_models::types::ServiceName;
use reqwest::Method;
use serde_json::json;

use crate::elastic::request::Request;
use crate::elastic::responses;
use crate::plumber::Plumber;
use crate::services::test::{dummy_service, setup_services_and_core};


#[tokio::test(flavor = "multi_thread")]
async fn test_expire_missing_service() {
    // Setup so that service 'a' is the only one
    let name = ServiceName::from("a");
    let services = [
        (name, dummy_service("a", "core", None, None, None, None))
    ].into();
    let (core, _guard) = setup_services_and_core(services).await;

    // enqueue a task for a service that isn't service a
    let task: Task = rand::random();
    let queue = core.get_service_queue("not-service-a");
    queue.push(0.0, &task).await.unwrap();

    // start the plumber
    let plumber = Plumber::new_mocked(core, Some(Duration::from_millis(100)), Some("plumber_1".to_string())).await.unwrap();
    let mut pool = tokio::task::JoinSet::new();
    plumber.start(&mut pool).await.unwrap();

    // wait for the plumber to cancel the task
    let failed = plumber.dispatch_client.failed().await;
    assert_eq!(queue.length().await.unwrap(), 0);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].sid, task.sid);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_flush_paused_queues() {
    // Setup so that service 'a' is the only one
    let name = ServiceName::from("a");
    let services = [
        (name, dummy_service("a", "core", None, None, None, None))
    ].into();
    let (core, _guard) = setup_services_and_core(services).await;

    // enqueue a task for a service that isn't service a
    let task: Task = rand::random();
    let queue = core.get_service_queue("a");
    queue.push(0.0, &task).await.unwrap();

    // start the plumber
    let plumber = Plumber::new_mocked(core.clone(), Some(Duration::from_millis(100)), Some("plumber_2".to_string())).await.unwrap();
    let mut pool = tokio::task::JoinSet::new();
    plumber.start(&mut pool).await.unwrap();

    // wait to make sure the plumber hasn't done anything yet
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(queue.length().await.unwrap(), 1);

    // disable the service
    core.services.get_service_stage_hash().set("a", &crate::constants::ServiceStage::Paused).await.unwrap();

    // wait for the plumber to clear the queue
    let failed = plumber.dispatch_client.failed().await;
    assert_eq!(queue.length().await.unwrap(), 0);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].sid, task.sid);
}

// Newer versions of elastic block writing to the .tasks index
#[tokio::test(flavor = "multi_thread")]
async fn test_cleanup_old_tasks() {
    let name = ServiceName::from("a");
    let services = [
        (name, dummy_service("a", "core", None, None, None, None))
    ].into();
    let (core, _guard) = setup_services_and_core(services).await;
    let connection = core.datastore.switch_to_new_user("plumber_test_task_cleanup").await.unwrap().connection();

    // helper to count the tasks in the database
    let count_results = async || {
        let request = Request::get_search(&connection.host, vec![
            ("index", ".tasks".into()),
            ("q", "task.start_time_in_millis:0".into()),
            ("size", "0".into()),
            ("track_total_hits", "1000".into()),
        ]).unwrap();
        let result = connection.make_request(&mut 0, &request).await.unwrap();
        let result: responses::Search<(), ()> = result.json().await.unwrap();
        result.hits.total.value
    };

    // Generate new documents in .tasks index
    let num_old_tasks = 10;
    let mut result_count = 0;
    while result_count < num_old_tasks {
        for _ in 0..num_old_tasks {
            let body = json!({
                "completed": true,
                "task": {
                    "start_time_in_millis": 0
                }
            });

            let mut url = connection.host.join(".tasks/_doc/").unwrap();
            url.set_query(Some("refresh=true"));
            let request = Request::new_on_document(Method::POST, url, ".tasks".to_string(), "".to_string());
            connection.make_request_json(&mut 0, &request, &body).await.unwrap();
        }
        result_count = count_results().await;
    }
    // Assert that these have been indeed committed to the tasks index
    assert!(result_count >= num_old_tasks, "{result_count} >= {num_old_tasks}");

    // Run task cleanup, we should return to no more "old" completed tasks
    println!("Starting plumber");
    let plumber = Plumber::new_mocked(core.clone(), Some(Duration::from_millis(100)), Some("plumber_test_task_cleanup2".to_string())).await.unwrap();
    let mut pool = tokio::task::JoinSet::new();
    plumber.start(&mut pool).await.unwrap();
    // tokio::time::sleep(Duration::from_millis(1000)).await;

    //
    let mut attempts = 0;
    let count = loop {
        println!("Checking for trailing tasks");
        let count = count_results().await;
        attempts += 1;
        if attempts > 100 || count == 0 {
            break count
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(count, 0);
    println!("finished");
}


// #[tokio::test]
// async fn test_user_setting_migrations() {
//     todo!()
    // from assemblyline.odm.models.config import SubmissionProfileParams

    // SubmissionProfileParams.fields().keys()
    // # Create a bunch of random "old" tasks and clean them up
    // redis = mock.MagicMock(spec=Redis)
    // redis_persist = mock.MagicMock(spec=Redis)
    // plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore_connection, delay=1)

    // # Create a user with old settings (format prior to 4.6)
    // settings = {'classification': 'TLP:CLEAR', 'deep_scan': False, 'description': '', 'download_encoding': 'cart', 'default_external_sources': ['Malware Bazaar', 'VirusTotal'], 'default_zip_password': 'zippy', 'executive_summary': False, 'expand_min_score': 500, 'generate_alert': False, 'ignore_cache': False, 'ignore_dynamic_recursion_prevention': False, 'ignore_recursion_prevention': False, 'ignore_filtering': False, 'malicious': False, 'priority': 369, 'profile': False, 'service_spec': {'AVClass': {'include_malpedia_dataset': False}}, 'services': {'selected': ['Extraction', 'ConfigExtractor', 'YARA'], 'excluded': [], 'rescan': [], 'resubmit': [], 'runtime_excluded': []}, 'submission_view': 'report', 'ttl': 0}

    // user_account = random_model_obj(User, as_json=True)
    // user_account['uname'] = "admin"
    // user_account['apikeys'] = {'test': random_model_obj(ApiKey, as_json=True)}

    // # Disable the dynamic mapping prevention to allow an old document to be inserted
    // datastore_connection.ds.client.indices.put_mapping(index='user_settings', dynamic='false')
    // datastore_connection.ds.client.index(index="user_settings", id="admin", document=settings)
    // datastore_connection.ds.client.index(index="user", id="admin", document=user_account)

    // datastore_connection.user_settings.commit()
    // datastore_connection.user.commit()

    // # Initiate the migration
    // plumber.user_apikey_cleanup()
    // plumber.migrate_user_settings()

    // # Check that the settings have been migrated
    // migrated_settings = datastore_connection.user_settings.get("admin", as_obj=False)

    // # Check to see if API keys for the user were transferred to the new index
    // assert datastore_connection.apikey.search('uname:admin', rows=0)['total'] > 0

    // # Deprecated settings should be removed
    // assert "ignore_dynamic_recursion_prevention" not in migrated_settings

    // # All former submission settings at the root-level should be moved to submission profiles
    // assert all([key not in migrated_settings for key in SubmissionProfileParams.fields().keys()] )

    // for settings in migrated_settings['submission_profiles'].values():
    //     assert settings['classification'] == 'TLP:C'
    //     assert settings['deep_scan'] is False
    //     assert settings['generate_alert'] is False
    //     assert settings['ignore_cache'] is False
    //     assert settings['priority'] == 369
    //     # Full service spec should be preserved in default profile (along with others by default if there's no restricted parameters)
    //     assert settings['service_spec'] == {'AVClass': {'include_malpedia_dataset': False}}
    //     assert settings['ttl'] == 0
// }