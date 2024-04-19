//! 

use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use assemblyline_models::{config::Config, messages::{submission::{Submission, SubmissionMessage}, ArchivedMessage}};
use log::warn;
use rand::{thread_rng, Rng};
use redis_objects::Queue;

use crate::{constants::ARCHIVE_QUEUE_NAME, services::ServiceHelper, submit::SubmitManager, Core};


struct ArchiveManager {
    config: Arc<Config>,
    submit: SubmitManager,
    services: ServiceHelper,
    archive_queue: Queue<(String, String, bool)>,
    //             self.datastore = datastore or forge.get_datastore(self.config)
//             redis_persistent = get_client(self.config.core.redis.persistent.host,
//                                           self.config.core.redis.persistent.port, False)
//             redis = get_client(self.config.core.redis.nonpersistent.host,
//                                self.config.core.redis.nonpersistent.port, False)
//             self.submission_traffic = CommsQueue('submissions', host=redis)
}


impl ArchiveManager {
    fn new(core: &Core) -> Self {
        Self {
            config: core.config.clone(),
            submit: SubmitManager::new(core),
            services: core.services.clone(), 
            archive_queue: core.redis_persistant.queue(ARCHIVE_QUEUE_NAME.to_owned(), None),
        }
    }

    pub async fn archive_submission(&self, submission: Submission, delete_after: Option<bool>) -> Result<Option<ArchivedMessage>> {
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
            self.datastore.submission.update(submission['sid'], [(ESCollection.UPDATE_SET, 'archived', True)])

            return Ok(Some(ArchivedMessage::resubmit(sid)))
        }
    }
}

