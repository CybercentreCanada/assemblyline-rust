//! 

use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use assemblyline_models::config::Config;
use assemblyline_models::messages::{submission::{Submission, SubmissionMessage}, ArchivedMessage};
use log::warn;
use rand::{thread_rng, Rng};
use redis_objects::{Publisher, Queue};

use crate::elastic::collection::OperationBatch;
use crate::elastic::Elastic;
use crate::{constants::ARCHIVE_QUEUE_NAME, services::ServiceHelper, submit::SubmitManager, Core};


pub struct ArchiveManager {
    config: Arc<Config>,
    datastore: Arc<Elastic>,
    submit: SubmitManager,
    services: ServiceHelper,
    archive_queue: Queue<(String, String, bool)>,
    submission_traffic: Publisher,
}


impl ArchiveManager {
    pub fn new(core: &Core) -> Self {
        Self {
            config: core.config.clone(),
            datastore: core.datastore.clone(),
            submit: SubmitManager::new(core),
            services: core.services.clone(), 
            archive_queue: core.redis_persistant.queue(ARCHIVE_QUEUE_NAME.to_owned(), None),
            submission_traffic: core.redis_volatile.publisher("submissions".to_owned())
        }
    }

    pub async fn archive_submission(&self, submission: &Submission, delete_after: Option<bool>) -> Result<Option<ArchivedMessage>> {
        if !self.config.datastore.archive.enabled {
            warn!("Trying to archive a submission when archiving is disabled.");
            return Ok(None)
        }
        let delete_after = delete_after.unwrap_or(false);

        let sub_selected = self.services.expand_categories(submission.params.services.selected.clone());
        let min_selected = self.services.expand_categories(self.config.core.archiver.minimum_required_services.clone());

        if HashSet::from_iter(min_selected.iter()).is_subset(&HashSet::from_iter(sub_selected.iter())) {
            self.archive_queue.push(&("submission".to_owned(), submission.sid.to_string(), delete_after)).await?;
            return Ok(Some(ArchivedMessage::archive()))
        } else {
            sub_selected.extend(min_selected);
            sub_selected.sort_unstable();
            sub_selected.dedup();

            let mut params = submission.params.clone();
            params.auto_archive = true;
            params.delete_after_archive = delete_after;
            params.services.selected = sub_selected;

            let submission_obj = Submission{
                files: submission.files,
                metadata: submission.metadata,
                params,
                sid: thread_rng().gen(),
                time: Default::default(),
                notification: Default::default(),
                scan_key: Default::default(),
            };
            let sid = submission_obj.sid;

            self.submit.submit_prepared(submission_obj.clone(), None).await?;
            // except (ValueError, KeyError) as e:
            //     raise SubmissionException(f"Could not generate re-submission message: {str(e)}").with_traceback()

            self.submission_traffic.publish(&SubmissionMessage::received(submission_obj, "archive".to_owned())).await?;

            // Update current record
            let mut batch = OperationBatch::default();
            batch.set("archived".to_owned(), serde_json::Value::Bool(true));
            self.datastore.submission.update(&submission.sid.to_string(), batch, None).await?;

            return Ok(Some(ArchivedMessage::resubmit(sid)))
        }
    }
}

