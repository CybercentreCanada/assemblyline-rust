use std::sync::Arc;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_markings::config::ready_classification;
use assemblyline_models::datastore::{File, Service};
use assemblyline_models::Sha256;
use log::debug;
use rand::Rng;

use crate::elastic::create_empty_result_from_key;

use super::Elastic;

fn create_service(name: &str) -> Service {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "enabled": true,
        "classification": "U",
        "default_result_classification": "U",
        "version": rand::random::<u8>().to_string(),
        "docker_config": {
            "image": "abc:123"
        },
    })).unwrap()
}

async fn init() -> Arc<Elastic> {
    let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Debug).try_init();
    let prefix = rand::rng().random::<u128>().to_string();
    Elastic::connect("http://elastic:devpass@localhost:9200", false, None, false, &prefix).await.unwrap()
}

#[tokio::test]
async fn list_services() {
    let elastic = init().await;
    use serde_json::json;
    // connect to database

    let mut aa = create_service("servicea");
    let bb = create_service("serviceb");

    debug!("save service");
    elastic.service.save(&aa.key(), &aa, None, None).await.unwrap();
    elastic.service.save(&bb.key(), &bb, None, None).await.unwrap();
    debug!("save service delta");
    elastic.service_delta.save_json(&aa.name, &mut [("version".to_owned(), json!(aa.version))].into_iter().collect(), None, None).await.unwrap();
    elastic.service_delta.save_json(&bb.name, &mut [("version".to_owned(), json!(bb.version))].into_iter().collect(), None, None).await.unwrap();
    debug!("commit service_delta");
    elastic.service_delta.commit(None).await.unwrap();

    // fetch the services without changes
    debug!("get service with delta");
    assert_eq!(elastic.get_service_with_delta(&aa.name, None).await.unwrap().unwrap(), aa);
    assert_eq!(elastic.get_service_with_delta(&bb.name, None).await.unwrap().unwrap(), bb);
    {
        debug!("list_all_services");
        let listed = elastic.list_all_services().await.unwrap();
        assert!(listed.contains(&aa));
        assert!(listed.contains(&bb));
    }

    // change one of the services
    aa.category = "DOGHOUSE".to_string();
    aa.enabled = false;
    {
        debug!("update delta");
        let mut delta = elastic.service_delta.get(&aa.name, None).await.unwrap().unwrap();
        delta.category = Some("DOGHOUSE".to_owned());
        delta.enabled = Some(false);
        elastic.service_delta.save(&aa.name, &delta, None, None).await.unwrap();
        elastic.service_delta.commit(None).await.unwrap();
    }

    // fetch them again and ensure the changes have been applied
    assert_eq!(elastic.get_service_with_delta(&aa.name, None).await.unwrap().unwrap(), aa);
    assert_eq!(elastic.get_service_with_delta(&bb.name, None).await.unwrap().unwrap(), bb);
    {
        let listed = elastic.list_all_services().await.unwrap();
        assert!(listed.contains(&aa));
        assert!(listed.contains(&bb));

        let listed = elastic.list_enabled_services().await.unwrap();
        assert!(!listed.contains(&aa));
        assert!(listed.contains(&bb));
    }
}

#[tokio::test]
async fn test_save_or_freshen_file() {
    let ds = init().await;

    let classification = assemblyline_markings::classification::sample_config();
    let ce = Arc::new(assemblyline_markings::classification::ClassificationParser::new(classification).unwrap());
    assemblyline_models::types::classification::set_global_classification(ce.clone());

    // Generate random data
    let mut data: Vec<u8> = vec![]; 
    for _ in 0..64 {
        data.extend(b"asfd");
    }
    let expiry_create = chrono::Utc::now() + chrono::Duration::days(14).to_std().unwrap();
    let expiry_freshen = chrono::Utc::now() + chrono::Duration::days(15).to_std().unwrap();

    // Generate file info for random file
    let mut f = File::gen_for_sample(&data, &mut rand::rng());
    f.expiry_ts = Some(chrono::Utc::now());

    // Make sure file does not exists
    ds.file.delete(&f.sha256.to_string(), None).await.unwrap();

    // Save the file
    let raw = if let serde_json::Value::Object(raw) = serde_json::to_value(&f).unwrap() { raw } else { panic!(); };
    ds.save_or_freshen_file(&f.sha256, raw.clone(), Some(expiry_create), ce.restricted().to_owned(), &ce).await.unwrap();

    // Validate created file
    let (saved_file, _) = ds.file.get_if_exists(&f.sha256.to_string(), None).await.unwrap().unwrap();
    assert_eq!(saved_file.sha256, f.sha256);
    assert_eq!(saved_file.sha1, f.sha1);
    assert_eq!(saved_file.md5, f.md5);
    assert_eq!(saved_file.expiry_ts, Some(expiry_create));
    assert_eq!(saved_file.seen.count, 1);
    assert_eq!(saved_file.seen.first, saved_file.seen.last);
    assert_eq!(saved_file.classification, ce.restricted());

    // Freshen the file
    ds.save_or_freshen_file(&f.sha256, raw, Some(expiry_freshen), ce.unrestricted().to_owned(), &ce).await.unwrap();

    // Validate freshened file
    let (freshened_file, _) = ds.file.get_if_exists(&f.sha256.to_string(), None).await.unwrap().unwrap();
    assert_eq!(freshened_file.sha256, f.sha256);
    assert_eq!(freshened_file.sha1, f.sha1);
    assert_eq!(freshened_file.md5, f.md5);
    assert_eq!(freshened_file.expiry_ts, Some(expiry_freshen));
    assert_eq!(freshened_file.seen.count, 2);
    assert!(freshened_file.seen.first < freshened_file.seen.last);
    assert_eq!(freshened_file.classification, ce.unrestricted());
}

// import hashlib
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.odm.models.file import File
// import pytest
// import random

// from retrying import retry

// from assemblyline.common import forge
// from assemblyline.datastore.helper import AssemblylineDatastore, MetadataValidator
// from assemblyline.odm.base import DATEFORMAT, KeyMaskException
// from assemblyline.odm.models.config import Config, Metadata
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.service import Service
// from assemblyline.odm.models.submission import Submission
// from assemblyline.odm.randomizer import SERVICES, random_minimal_obj
// from assemblyline.odm.random_data import create_signatures, create_submission, create_heuristics, create_services


// class SetupException(Exception):
//     pass


// @retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=500)
// def setup_store(al_datastore: AssemblylineDatastore, request):
//     try:
//         ret_val = al_datastore.ds.ping()
//         if ret_val:

//             # Create data
//             fs = forge.get_filestore()
//             for _ in range(3):
//                 create_submission(al_datastore, fs)
//             create_heuristics(al_datastore)
//             create_signatures(al_datastore)
//             create_services(al_datastore)

//             # Wipe all on finalize
//             def cleanup():
//                 for index_name in al_datastore.ds.get_models():
//                     al_datastore.enable_archive_access()
//                     collection = al_datastore.get_collection(index_name)
//                     collection.wipe(recreate=False)
//             request.addfinalizer(cleanup)

//             return al_datastore
//     except ConnectionError:
//         pass
//     raise SetupException("Could not setup Datastore: %s" % al_datastore)


// @pytest.fixture(scope='module')
// def config():
//     config = forge.get_config()
//     config.datastore.archive.enabled = True
//     return config


// @pytest.fixture(scope='module')
// def ds(request, config):
//     try:
//         return setup_store(forge.get_datastore(config=config), request)
//     except SetupException:
//         pass

//     return pytest.skip("Connection to the Elasticsearch server failed. This test cannot be performed...")


// def test_index_archive_status(ds: AssemblylineDatastore, config: Config):
//     """Save a new document atomically, then try to save it again and detect the failure."""
//     ds.enable_archive_access()
//     try:
//         indices = ds.ds.get_models()
//         archiveable_indices = config.datastore.archive.indices

//         for index in indices:
//             collection = ds.get_collection(index)
//             if index in archiveable_indices:
//                 assert collection.archive_name == f"{index}-ma"
//             else:
//                 assert collection.archive_name is None

//     finally:
//         ds.disable_archive_access()


// def test_get_stats(ds: AssemblylineDatastore):
//     stats = ds.get_stats()
//     assert "cluster" in stats
//     assert "nodes" in stats
//     assert "indices" in stats
//     assert stats['cluster']['status'] in ["green", "yellow"]


#[tokio::test]
async fn test_create_empty_result() {
    let cl_engine = ClassificationParser::new(ready_classification(None).unwrap()).unwrap();

    // Set expected values
    let classification = cl_engine.unrestricted();
    let svc_name = "TEST";
    let svc_version = "4";
    let sha256: Sha256 = "a123".repeat(16).parse().unwrap();

    // Build result key
    let result_key = assemblyline_models::datastore::Result::help_build_key(
        &sha256, 
        svc_name, 
        svc_version, 
        true,
        false,
        None,
        None
    ).unwrap();

    // Create an empty result from the key
    let empty_result = create_empty_result_from_key(&result_key, 5, &cl_engine).unwrap();

    // Test the empty result
    assert!(empty_result.is_empty());
    assert_eq!(empty_result.response.service_name, svc_name);
    assert_eq!(empty_result.response.service_version, svc_version);
    assert_eq!(empty_result.sha256, sha256);
    assert_eq!(empty_result.classification.as_str(), classification);
}

// DELETE_TREE_PARAMS = [
//     (True, "bulk"),
//     (False, "direct"),
// ]


// # noinspection PyShadowingNames
// @pytest.mark.parametrize("bulk", [f[0] for f in DELETE_TREE_PARAMS], ids=[f[1] for f in DELETE_TREE_PARAMS])
// def test_delete_submission_tree(ds: AssemblylineDatastore, bulk):
//     # Reset the data
//     fs = forge.get_filestore()

//     # Create a random submission
//     submission: Submission = create_submission(ds, fs)
//     files = set({submission.files[0].sha256})
//     files = files.union([x[:64] for x in submission.results])
//     files = files.union([x[:64] for x in submission.errors])
//     # Validate the submission is there
//     assert ds.submission.exists(submission.sid)
//     for f in files:
//         assert ds.file.exists(f)
//     for r in submission.results:
//         if r.endswith(".e"):
//             assert ds.emptyresult.exists(r)
//         else:
//             assert ds.result.exists(r)
//     for e in submission.errors:
//         assert ds.error.exists(e)

//     # Delete the submission
//     if bulk:
//         ds.delete_submission_tree_bulk(submission.sid, transport=fs)
//     else:
//         ds.delete_submission_tree(submission.sid, transport=fs)

//     # Make sure delete operation is reflected in the DB
//     ds.submission.commit()
//     ds.error.commit()
//     ds.emptyresult.commit()
//     ds.result.commit()
//     ds.file.commit()

//     # Make sure submission is completely gone
//     assert not ds.submission.exists(submission.sid)
//     for f in files:
//         assert not ds.file.exists(f)
//     for r in submission.results:
//         if r.endswith(".e"):
//             assert not ds.emptyresult.exists(r)
//         else:
//             assert not ds.result.exists(r)
//     for e in submission.errors:
//         assert not ds.error.exists(e)


// def test_get_all_heuristics(ds: AssemblylineDatastore):
//     # Get a list of all services
//     all_services = set([x.upper() for x in SERVICES.keys()])

//     # List all heuristics
//     heuristics = ds.get_all_heuristics()

//     # Test each heuristics
//     for heur in heuristics.values():
//         assert heur['heur_id'].split(".")[0] in all_services


// def test_get_results(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get all results for that submission
//     results = ds.get_multiple_results(submission.results)
//     assert len(results) == len(submission.results)

//     # Get results one by one
//     single_res = {}
//     for r in submission.results:
//         single_res[r] = ds.get_single_result(r)

//     # Compare results
//     for r_key in results:
//         assert r_key in single_res
//         if not r_key.endswith(".e"):
//             assert single_res[r_key] == results[r_key]


// def test_get_file_submission_meta(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get submission meta
//     submission_meta = ds.get_file_submission_meta(submission.files[0].sha256, ['params.submitter'])

//     # check if current submission values are in submission meta
//     assert submission.params.submitter in submission_meta['submitter']


// def test_get_file_list_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get related file list
//     file_list = [sha256 for sha256, supplementary, in ds.get_file_list_from_keys(submission.results)]

//     # Check if all files that are obvious from the results are there
//     for f in submission.files:
//         if not [r for r in submission.results if r.startswith(f.sha256) and not r.endswith('.e')]:
//             # If this file has no actual results, we can't this file to show up in the file list
//             continue
//         assert f.sha256 in file_list
//     for r in submission.results:
//         if r.endswith('.e'):
//             # We can't expect a file tied to be in the file list
//             continue
//         assert r[:64] in file_list


// def test_get_file_scores_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get scores
//     file_scores = ds.get_file_scores_from_keys(submission.results)

//     # Check if all files that are obvious from the results are there
//     for f in submission.files:
//         if not [r for r in submission.results if r.startswith(f.sha256) and not r.endswith('.e')]:
//             # If this file has no actual results, we can't expect there to be a file score
//             continue
//         assert f.sha256 in file_scores
//     for r in submission.results:
//         if r.endswith('.e'):
//             # We can't expect a file tied to an empty_result to have a file score
//             continue
//         assert r[:64] in file_scores

//     for s in file_scores.values():
//         assert isinstance(s, int)


// def test_get_signature_last_modified(ds: AssemblylineDatastore):
//     last_mod = ds.get_signature_last_modified()

//     assert isinstance(last_mod, str)
//     assert "T" in last_mod
//     assert last_mod.endswith("Z")


// def test_get_or_create_file_tree(ds: AssemblylineDatastore, config: Config):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get file tree
//     tree = ds.get_or_create_file_tree(submission, config.submission.max_extraction_depth)

//     # Check if all files that are obvious from the results are there
//     for x in ['tree', 'classification', 'filtered', 'partial', 'supplementary']:
//         assert x in tree

//     for f in submission.files:
//         assert f.sha256 in tree['tree']


// def test_get_summary_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get the summary
//     summary = ds.get_summary_from_keys(submission.results)

//     # Get the summary with heuristics
//     summary_heur = ds.get_summary_from_keys(submission.results, keep_heuristic_sections=True)

//     assert summary['tags'] == summary_heur['tags']
//     assert summary['attack_matrix'] == summary_heur['attack_matrix']
//     assert summary['heuristics'] == summary_heur['heuristics']
//     assert summary['classification'] == summary_heur['classification']
//     assert summary['filtered'] == summary_heur['filtered']
//     assert summary['heuristic_sections'] == {}
//     assert summary['heuristic_name_map'] == {}

//     heuristics = ds.get_all_heuristics()

//     for h in summary_heur['heuristic_sections']:
//         assert h in heuristics

//     for heur_list in summary_heur['heuristic_name_map'].values():
//         for h in heur_list:
//             assert h in heuristics


// def test_get_tag_list_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get the list of tags
//     tags = ds.get_tag_list_from_keys(submission.results)

//     assert len(tags) > 0
//     for t in tags:
//         assert t['key'] in submission.results


// def test_get_attack_matrix_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get the list of tags
//     attacks = ds.get_attack_matrix_from_keys(submission.results)

//     for a in attacks:
//         assert a['key'] in submission.results


// def test_get_service_with_delta(ds: AssemblylineDatastore):
//     # Get a random service delta
//     service_delta: Service = ds.service_delta.search("id:*", rows=1, fl="*")['items'][0]
//     service_key = f"{service_delta.id}_{service_delta.version}"
//     service_delta.category = "TEST"

//     # Save fake service category
//     ds.service_delta.save(service_delta.id, service_delta)
//     ds.service_delta.commit()

//     # Get the associated service
//     service: Service = ds.service.get(service_key)

//     # Get the full service with its delta
//     full_service = ds.get_service_with_delta(service_delta.id)

//     assert full_service.as_primitives() != service.as_primitives()
//     assert full_service.category == "TEST"


// def test_calculate_heuristic_stats(ds: AssemblylineDatastore):
//     default_stats = {'count': 0, 'min': 0, 'max': 0, 'avg': 0, 'sum': 0, 'first_hit': None, 'last_hit': None}

//     # Reset original heuristics stats
//     for heur_id in ds.get_all_heuristics():
//         ds.heuristic.update(heur_id, [(ds.heuristic.UPDATE_SET, 'stats', default_stats)])
//     ds.heuristic.commit()

//     # Make sure stats did get reset
//     heuristics = ds.get_all_heuristics()
//     assert all([heur['stats'] == default_stats for heur in heuristics.values()])

//     # Do heuristics stat calculation for all
//     ds.calculate_heuristic_stats()
//     ds.heuristic.commit()

//     # Get heuristics with calculated stats
//     updated_heuristics = ds.get_all_heuristics()

//     assert heuristics != updated_heuristics
//     assert any([heur['stats'] != default_stats for heur in updated_heuristics.values()])


// def test_calculate_signature_stats(ds: AssemblylineDatastore):
//     default_stats = {'count': 0, 'min': 0, 'max': 0, 'avg': 0, 'sum': 0, 'first_hit': None, 'last_hit': None}

//     def get_all_signatures():
//         return {s['id']: s for s in ds.signature.stream_search("id:*", as_obj=False)}

//     # Reset original signature stats
//     for sig_id in get_all_signatures():
//         ds.signature.update(sig_id, [(ds.signature.UPDATE_SET, 'stats', default_stats)])
//     ds.signature.commit()

//     # Make sure stats did get reset
//     signatures = get_all_signatures()
//     assert all([sig['stats'] == default_stats for sig in signatures.values()])

//     # Do signature stat calculation for all
//     ds.calculate_signature_stats(lookback_time="now-1y")
//     ds.signature.commit()

//     # Get signatures with calculated stats
//     updated_signatures = get_all_signatures()

//     assert signatures != updated_signatures
//     assert any([sig['stats'] != default_stats for sig in updated_signatures.values()])


// def test_task_cleanup(ds: AssemblylineDatastore):
//     assert ds.ds.client.search(index='.tasks',
//                                q="completed:true",
//                                track_total_hits=True,
//                                size=0)['hits']['total']['value'] != 0

//     if ds.ds.es_version.major == 7:
//         # Superusers are allowed to interact with .tasks index
//         assert ds.task_cleanup()

//     elif ds.ds.es_version.major == 8:
//         # Superusers are NOT allowed to interact with .tasks index because it's a restricted index

//         # Attempt cleanup using the default user, assert that the cleanup didn't happen
//         assert ds.task_cleanup() == 0

//         # Switch to user that can perform task cleanup
//         ds.ds.switch_user("plumber")

//         assert ds.task_cleanup()


// def test_list_all_services(ds: AssemblylineDatastore):
//     all_svc: Service = ds.list_all_services()
//     all_svc_full: Service = ds.list_all_services(full=True)

//     # Make sure service lists are different
//     assert all_svc != all_svc_full

//     # Check that all services are there in the normal list
//     for svc in all_svc:
//         assert svc.name in SERVICES

//     # Check that all services are there in the full list
//     for svc in all_svc_full:
//         assert svc.name in SERVICES

//     # Make sure non full list raises exceptions
//     for svc in all_svc:
//         with pytest.raises(KeyMaskException):
//             svc.timeout

//     # Make sure the full list does not
//     for svc in all_svc_full:
//         assert svc.timeout is not None


// def test_list_service_heuristics(ds: AssemblylineDatastore):
//     # Get a random service
//     svc_name = random.choice(list(SERVICES.keys()))

//     # Get the service heuristics
//     heuristics = ds.list_service_heuristics(svc_name)

//     # Validate the heuristics
//     for heur in heuristics:
//         assert heur.heur_id.startswith(svc_name.upper())


// def test_list_all_heuristics(ds: AssemblylineDatastore):
//     # Get a list of all services
//     all_services = set([x.upper() for x in SERVICES.keys()])

//     # List all heuristics
//     heuristics = ds.list_all_heuristics()

//     # Test each heuristics
//     for heur in heuristics:
//         assert heur.heur_id.split(".")[0] in all_services

// def test_metadata_validation(ds: AssemblylineDatastore):
//     validator = MetadataValidator(ds)

//     # Run validator with no submission metadata validation configured
//     assert not validator.check_metadata({'blah': 'blee'}, validation_scheme={})

//     # Run validation using validator parameters
//     meta_config = {
//         'blah': Metadata({
//             'required': True,
//             'validator_type': 'regex',
//             'validator_params': {
//                 'validation_regex': 'blee'
//             }
//         })
//     }
//     assert not validator.check_metadata({'blah': 'blee'}, validation_scheme=meta_config)

//     # Run validator with validation configured but is missing metadata
//     assert validator.check_metadata({'bloo': 'blee'}, validation_scheme=meta_config)

//     # Run validation using invalid metadata
//     assert validator.check_metadata({'blah': 'blee'}, validation_scheme={
//         'blah': Metadata({
//             'required': True,
//             'validator_type': 'integer',
//         })
//     })

//     # Run validation on field that's not required (but still provided and is invalid)
//     assert validator.check_metadata({'blah': 'blee'}, validation_scheme={
//         'blah': Metadata({
//             'validator_type': 'integer',
//         })
//     })


// def test_switch_user(ds: AssemblylineDatastore):
//     # Attempt to switch to another random user
//     ds.ds.switch_user("test")

//     # Confirm that user switch didn't happen
//     assert list(ds.ds.client.security.get_user().keys()) != ["test"]

//     # Switch to recognized plumber user
//     ds.ds.switch_user("plumber")

//     # Confirm that user switch did happen
//     assert list(ds.ds.client.security.get_user().keys()) != ["plumber"]

