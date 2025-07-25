//! Tool that submits files for processing in a consistant manner between API server and ingester

use std::sync::Arc;

use anyhow::Result;
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::config::Config;
use assemblyline_models::ExpandingClassification;
use chrono::{Utc, Duration};

use assemblyline_models::messages::submission::Submission as MessageSubmission;
use assemblyline_models::datastore::submission::{Submission as DatastoreSubmission, SubmissionState};
use assemblyline_models::messages::dispatching::SubmissionDispatchMessage;
use redis_objects::Queue;

use crate::constants::SUBMISSION_QUEUE;
use crate::elastic::Elastic;
use crate::Core;


pub struct SubmitManager {
    datastore: Arc<Elastic>,
    config: Arc<Config>,
    classification_parser: Arc<ClassificationParser>,
    pub dispatch_submission_queue: Queue<SubmissionDispatchMessage>,
}

impl SubmitManager {

    pub fn new(core: &Core) -> Self {
        Self {
            dispatch_submission_queue: core.redis_volatile.queue(SUBMISSION_QUEUE.to_owned(), None),
            config: core.config.clone(),
            classification_parser: core.classification_parser.clone(),
            datastore: core.datastore.clone()
        }
    }

    /// Start a submission that has already been prepared
    pub async fn submit_prepared(&self, mut submission_obj: MessageSubmission, completed_queue: Option<String>) -> Result<()> {
        // Figure out the expiry for the submission
        // Enforce maximum DTL
        if self.config.submission.max_dtl > 0 {
            if submission_obj.params.ttl > 0 {
                submission_obj.params.ttl = submission_obj.params.ttl.min(self.config.submission.max_dtl as i32);
            } else {
                submission_obj.params.ttl = self.config.submission.max_dtl as i32;
            }
        }
        let expiry = if submission_obj.params.ttl > 0 {
            Some(Utc::now() + Duration::days(submission_obj.params.ttl as i64))
        } else {
            None
        };

        // We should now have all the information we need to construct a submission object
        let mut sub = DatastoreSubmission{
            archive_ts: None,
            archived: false,
            classification: ExpandingClassification::new(submission_obj.params.classification.as_str().to_owned(), self.classification_parser.as_ref())?,
            error_count: 0,
            errors: vec![],
            expiry_ts: expiry,
            file_count: submission_obj.files.len() as i32,
            files: submission_obj.files,
            max_score: 0,
            metadata: submission_obj.metadata,
            params: submission_obj.params,
            tracing_events: vec![],
            results: vec![],
            sid: submission_obj.sid,
            state: SubmissionState::Submitted,
            scan_key: submission_obj.scan_key,
            to_be_deleted: false,
            times: Default::default(),
            verdict: Default::default(),
            from_archive: false,
        };

        if self.config.ui.allow_malicious_hinting && sub.params.malicious {
            sub.verdict.malicious.push(sub.params.submitter.clone());
        }

        self.datastore.submission.save(&sub.sid.to_string(), &sub, None, None).await?;

        self.dispatch_submission_queue.push(&SubmissionDispatchMessage::simple(sub, completed_queue)).await?;
        Ok(())
    }
}



// """ File Submission Interfaces.

// TODO update this

// The Submission service encapsulates the core functionality of accepting,
// triaging and forwarding a submission to the dispatcher.

// There are three primary modes of submission:

//   two-phase (presubmit + submit)
//   inline  (submit file)
//   existing (submit a file that is already in the file cache/SAN)

// In two-phase mode, submission is a presubmit followed by a submit.
// A 'presubmit' is sent to the submission service first. If the server already
// has a copy of the sample it indicates as such to the client which saves the
// client from copying the file again. Once the client has copied the file
// (if required) it then issues a final 'submit'.

// """
// from assemblyline.common.classification import InvalidClassification
// import elasticapm
// import logging
// import os
// from typing import List, Optional, Tuple, Dict

// from assemblyline.common import forge
// from assemblyline.common.codec import decode_file
// from assemblyline.common.dict_utils import flatten
// from assemblyline.common.isotime import epoch_to_iso, now
// from assemblyline.common.str_utils import safe_str
// from assemblyline.datastore.helper import AssemblylineDatastore
// from assemblyline.filestore import FileStore
// from assemblyline.odm.messages.submission import Submission as SubmissionObject
// from assemblyline.odm.models.file import File as FileInfo
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.submission import File, Submission
// from assemblyline.odm.models.config import Config
// from assemblyline_core.dispatching.client import DispatchClient

// Classification = forge.get_classification()
// SECONDS_PER_DAY = 24 * 60 * 60


// def assert_valid_sha256(sha256):
//     if len(sha256) != 64:
//         raise ValueError('Invalid SHA256: %s' % sha256)


// class SubmissionException(Exception):
//     pass


// class SubmissionClient:
//     """A helper class to simplify submitting files from internal or external sources.

//     This tool helps take care of interactions between the filestore,
//     datastore, dispatcher, and any sources of files to be processed.
//     """

//     def __init__(self, datastore: AssemblylineDatastore = None, filestore: FileStore = None,
//                  config=None, redis=None, identify=None):
//         self.log = logging.getLogger('assemblyline.submission_client')
//         self.config: Config = config or forge.CachedObject(forge.get_config)
//         self.datastore = datastore or forge.get_datastore(self.config)
//         self.filestore = filestore or forge.get_filestore(self.config)
//         if identify:
//             self.cleanup = False
//         else:
//             self.cleanup = True
//         self.identify = identify or forge.get_identify(config=self.config, datastore=self.datastore, use_cache=True)

//         # A client for interacting with the dispatcher
//         self.dispatcher = DispatchClient(datastore, redis)

//     def __enter__(self):
//         return self

//     def __exit__(self, *_):
//         self.stop()

//     def stop(self):
//         if self.cleanup:
//             self.identify.stop()

//     @elasticapm.capture_span(span_type='submission_client')
//     def rescan(self, submission: Submission, results: Dict[str, Result], file_infos: Dict[str, FileInfo],
//                file_tree, errors: List[str], rescan_services: List[str]):
//         """
//         Rescan a submission started on another system.
//         """
//         # Reset submission processing data
//         submission['times'].pop('completed')
//         submission['state'] = 'submitted'

//         # Set the list of service to rescan
//         submission['params']['services']['rescan'] = rescan_services

//         # Create the submission object
//         submission_obj = Submission(submission)

//         if len(submission_obj.files) == 0:
//             raise SubmissionException("No files found to submit.")

//         for f in submission_obj.files:
//             if not self.datastore.file.exists(f.sha256):
//                 raise SubmissionException(f"File {f.sha256} does not exist, cannot continue submission.")

//         # Set the new expiry
//         if submission_obj.params.ttl:
//             submission_obj.expiry_ts = epoch_to_iso(now() + submission_obj.params.ttl * SECONDS_PER_DAY)

//         # Save the submission
//         self.datastore.submission.save(submission_obj.sid, submission_obj)

//         # Dispatch the submission
//         self.log.debug("Submission complete. Dispatching: %s", submission_obj.sid)
//         self.dispatcher.dispatch_bundle(submission_obj, results, file_infos, file_tree, errors)

//         return submission

//     @elasticapm.capture_span(span_type='submission_client')
//     def submit(self, submission_obj: SubmissionObject, local_files: Optional[List] = None,
//                completed_queue: Optional[str] = None, expiry: Optional[str] = None,
//                allow_description_overwrite: bool = False):
//         """Submit several files in a single submission.

//         After this method runs, there should be no local copies of the file left.
//         """
//         if local_files is None:
//             local_files = []

//         if len(submission_obj.files) == 0 and len(local_files) == 0:
//             raise SubmissionException("No files found to submit...")

//         # Figure out the expiry for the submission if none was provided
//         if expiry is None:
//             # Enforce maximum DTL
//             if self.config.submission.max_dtl > 0:
//                 if int(submission_obj.params.ttl):
//                     submission_obj.params.ttl = min(int(submission_obj.params.ttl), self.config.submission.max_dtl)
//                 else:
//                     submission_obj.params.ttl = self.config.submission.max_dtl

//             if submission_obj.params.ttl:
//                 expiry = epoch_to_iso(submission_obj.time.timestamp() + submission_obj.params.ttl * SECONDS_PER_DAY)

//         max_size = self.config.submission.max_file_size

//         for local_file in local_files:
//             if isinstance(local_file, tuple):
//                 fname, local_file = local_file
//             else:
//                 fname = safe_str(os.path.basename(local_file))

//             # Upload/download, extract, analyze files
//             original_classification = str(submission_obj.params.classification)
//             file_hash, size, new_metadata = self._ready_file(local_file, expiry, original_classification)
//             new_name = new_metadata.pop('name', fname)
//             new_description = new_metadata.pop("description", None)
//             if allow_description_overwrite and new_description is not None:
//                 submission_obj.params.description = new_description
//             meta_classification = new_metadata.pop('classification', original_classification)
//             if meta_classification != original_classification:
//                 try:
//                     submission_obj.params.classification = Classification.max_classification(
//                         meta_classification, original_classification)
//                 except InvalidClassification as ic:
//                     raise SubmissionException("The classification found inside the cart file cannot be merged with "
//                                               f"the classification the file was submitted as: {str(ic)}")

//             submission_obj.metadata.update(**flatten(new_metadata))

//             # Check that after we have resolved exactly what to pass on, that it
//             # remains a valid target for scanning
//             if size > max_size and not submission_obj.params.ignore_size:
//                 msg = "File too large (%d > %d). Submission failed" % (size, max_size)
//                 raise SubmissionException(msg)
//             elif size == 0:
//                 msg = "File empty. Submission failed"
//                 raise SubmissionException(msg)

//             submission_obj.files.append(File({
//                 'name': new_name,
//                 'size': size,
//                 'sha256': file_hash,
//             }))

//         # We should now have all the information we need to construct a submission object
//         sub = Submission(dict(
//             archive_ts=None,
//             classification=submission_obj.params.classification,
//             error_count=0,
//             errors=[],
//             expiry_ts=expiry,
//             file_count=len(submission_obj.files),
//             files=submission_obj.files,
//             max_score=0,
//             metadata=submission_obj.metadata,
//             params=submission_obj.params,
//             results=[],
//             sid=submission_obj.sid,
//             state='submitted',
//             scan_key=submission_obj.scan_key,
//         ))

//         if self.config.ui.allow_malicious_hinting and submission_obj.params.malicious:
//             sub.verdict = {"malicious": [submission_obj.params.submitter]}

//         self.datastore.submission.save(sub.sid, sub)

//         self.log.debug("Submission complete. Dispatching: %s", sub.sid)
//         self.dispatcher.dispatch_submission(sub, completed_queue=completed_queue)

//         return sub

//     def _ready_file(self, local_path: str, expiry, classification) -> Tuple[str, int, dict]:
//         """Take a file from local storage and prepare it for submission.

//         After this method finished the file will ONLY exist on the filestore, not locally.
//         """
//         extracted_path = None
//         try:
//             # Analyze the file and make sure the file table is up to date
//             fileinfo = self.identify.fileinfo(local_path)

//             if fileinfo['size'] == 0:
//                 raise SubmissionException("File empty. Submission failed")

//             # Check if there is an integrated decode process for this file
//             # eg. files that are packaged, and the contained file (not the package
//             # that local_path points to) should be passed into the system.
//             extracted_path, fileinfo, al_meta = decode_file(local_path, fileinfo, self.identify)
//             al_meta['classification'] = al_meta.get('classification', classification)
//             if not Classification.is_valid(al_meta['classification']):
//                 raise SubmissionException(f"{al_meta['classification']} is not a valid classification for this system"
//                                           ", submission is cancelled...")

//             if extracted_path:
//                 local_path = extracted_path

//             self.datastore.save_or_freshen_file(fileinfo['sha256'], fileinfo, expiry,
//                                                 al_meta['classification'])
//             self.filestore.upload(local_path, fileinfo['sha256'])
//             if fileinfo["type"].startswith("uri/") and "uri_info" in fileinfo and "uri" in fileinfo["uri_info"]:
//                 al_meta["name"] = fileinfo["uri_info"]["uri"]
//                 al_meta["description"] = f"Inspection of URL: {fileinfo['uri_info']['uri']}"
//             return fileinfo['sha256'], fileinfo['size'], al_meta

//         finally:
//             # If we extracted anything delete it
//             if extracted_path:
//                 if os.path.exists(extracted_path):
//                     os.unlink(extracted_path)
