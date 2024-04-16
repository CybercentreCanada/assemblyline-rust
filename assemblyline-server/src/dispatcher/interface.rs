//! Interface for controlling submission processing
//!

use std::sync::Arc;

use assemblyline_models::datastore::submission::SubmissionState;
use assemblyline_models::messages::task::TagItem;
use assemblyline_models::{Sid, Sha256, JsonMap};
use assemblyline_models::datastore::Submission;
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::web::{Data, Json, Path};
use poem::http::StatusCode;
use poem::{post, get, Server, delete, Response, handler, EndpointExt, IntoResponse};
use serde::{Serialize, Deserialize};

use crate::dispatcher::submission::process_submission;
use crate::dispatcher::{TaskSignature, ResultMessage, ResultResponse};
use crate::logging::LoggerMiddleware;
use crate::tls::random_tls_certificate;
use crate::error::Error;

use super::{DispatcherSession, ResultSummary, TagCollection};

pub async fn start_interface(session: Arc<DispatcherSession>) -> Result<(), Error> {

    let config = &session.config.core.dispatcher.broker_bind;

    let app = poem::Route::new()
        .at("/submission/", post(post_submission))
        .at("/submission/:sid", get(get_submission))
        .at("/task/start", post(post_start_task))
        .at("/task/result", post(post_result_task))
        .at("/task/error", post(post_error_task))
        .at("/health/", get(get_health))
        .at("/stats/", get(ws_stats));

    let app = match &config.path {
        Some(route) => poem::Route::new().nest(route, app),
        None => app,
    };

    let app = app
        .data(session.clone())
        .with(LoggerMiddleware);

    let listener = TcpListener::bind(config.address);
    let tls_config = match &config.tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem.clone())
                .key_from_data(tls.key_pem.clone())
        },
        None => random_tls_certificate()?
    };
    let listener = listener.openssl_tls(tls_config);

    let exit = tokio::spawn(async move {
        while session.flags.read().await.running {
            session.flags_changes.notified().await
        }
    });

    Server::new(listener)
        .run_with_graceful_shutdown(app, async { exit.await; } , None)
        .await?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
enum PostSubmissionStatus {
    Running,
    Finished,
}

#[derive(Serialize, Deserialize)]
pub struct PostSubmissionResponse {
    pub status: PostSubmissionStatus
}

#[handler]
async fn post_submission(
    Data(session): Data<&Arc<DispatcherSession>>,
    Json(submission): Json<Submission>
) -> poem::Result<Json<PostSubmissionResponse>> {
    todo!()
//     // Check if its in the finished table
//     if session.finished.read().await.contains_key(&submission.sid) {
//         return Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Finished }))
//     }

//     // Test if the submission is already finished in elasticsearch
//     if let Some(sub) = session.elastic.submission.get(&submission.sid.to_string()).await? {
//         if let SubmissionState::Completed = sub.state {
//             session.finished.write().await.insert(sub.sid, sub);
//             return Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Finished }))
//         }
//     }

//     // See if we can launch this as a new submission
//     let mut active = session.active.write().await;
//     match active.entry(submission.sid) {
//         std::collections::hash_map::Entry::Occupied(_) => {
//             Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Running }))
//         },
//         std::collections::hash_map::Entry::Vacant(entry) => {
//             entry.insert(tokio::spawn(process_submission(session.clone(), submission)));
//             Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Running }))
//         },
//     }
}

#[derive(Serialize, Deserialize)]
pub struct GetSubmissionResponse {
    pub submission: Submission
}

#[handler]
async fn get_submission(
    Data(session): Data<&Arc<DispatcherSession>>,
    Path(sid): Path<Sid>,
) -> poem::Result<Json<GetSubmissionResponse>> {
    todo!();
//     if session.active.read().await.contains_key(&sid) {
//         return Ok(StatusCode::NO_CONTENT.into())
//     }
//     if let Some(submission) = session.finished.read().await.get(&sid) {
//         return Ok(Json(submission).into_response())
//     }
//     return Ok(StatusCode::NOT_FOUND.into())
}

// #[handler]
// async fn delete_submission(
//     Data(session): Data<&Arc<Session>>,
//     Path(sid): Path<Sid>
// ) {
//     session.finished.write().await.remove(&sid);
// }

#[derive(Serialize, Deserialize)]
pub struct LoadInfo {
}

impl LoadInfo {
    pub fn stale() -> Self {
        Self {

        }
    }
}


#[derive(Serialize, Deserialize)]
pub enum DispatchStatusMessage {
    LoadInfo(LoadInfo),
}

// #[handler]
// fn ws_finishing_submission_and_status() -> Response {
//     todo!()
// }

// #[handler]
// fn post_start_task() -> Response {
//     todo!()
// }


#[derive(Serialize, Deserialize)]
pub struct TaskResult {
    pub sid: Sid,
    pub sha256: Sha256,
    pub task_id: u64,

    pub service_name: String,
    pub service_version: String,
    pub service_tool_version: String,
    // pub expiry_ts: 
//             expiry_ts = data['expiry_ts']

    pub result_summary: ResultSummary,
    pub tags: Vec<TagItem>,
    pub temporary_data: JsonMap,
    pub dynamic_recursion_bypass: Vec<Sha256>,
}

// #[handler]
// async fn post_result_task(
//     Data(session): Data<&Arc<Session>>,
//     Json(result): Json<TaskResult>
// ) {
//     let sig = TaskSignature {
//         hash: result.sha256.clone(),
//         service: result.service_name.clone(),
//         sid: result.sid,
//     };
//     session.result_messages.send(ResultMessage::Response(sig, result.worker.clone(), ResultResponse::Result(result))).await;
// }

#[derive(Serialize, Deserialize)]
pub struct TaskError {
    pub task_id: u64,
    pub sid: Sid,
    pub service_name: String,
    pub sha256: Sha256,

    pub worker: String,
}

// #[handler]
// async fn post_error_task(
//     Data(session): Data<&Arc<Session>>,
//     Json(result): Json<TaskError>
// ) {
//     let sig = TaskSignature {
//         hash: result.sha256.clone(),
//         service: result.service_name.clone(),
//         sid: result.sid,
//     };
//     session.result_messages.send(ResultMessage::Response(sig, result.worker.clone(), ResultResponse::Error(result))).await;
// }

// #[handler]
// fn get_health() -> Response {
//     todo!()
// }

// #[handler]
// fn get_stats() -> Response {
//     todo!()
// }




//     def handle_commands(self):
//         while self.running:

//             message = self.command_queue.pop(timeout=3)
//             if not message:
//                 continue

//             cpu_mark = time.process_time()
//             time_mark = time.time()

//             # Start of process dispatcher transaction
//             with apm_span(self.apm_client, 'command_message'):

//                 command = DispatcherCommandMessage(message)
//                 if command.kind == CREATE_WATCH:
//                     watch_payload: CreateWatch = command.payload()
//                     self.setup_watch_queue(watch_payload.submission, watch_payload.queue_name)
//                 elif command.kind == LIST_OUTSTANDING:
//                     payload: ListOutstanding = command.payload()
//                     self.list_outstanding(payload.submission, payload.response_queue)
//                 elif command.kind == UPDATE_BAD_SID:
//                     self.update_bad_sids()
//                     NamedQueue(command.payload_data, host=self.redis).push(self.instance_id)
//                 else:
//                     self.log.warning(f"Unknown command code: {command.kind}")

//                 self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//                 self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//     @elasticapm.capture_span(span_type='dispatcher')
//     def setup_watch_queue(self, sid, queue_name):
//         # Create a unique queue
//         watch_queue = NamedQueue(queue_name, ttl=30)
//         watch_queue.push(WatchQueueMessage({'status': 'START'}).as_primitives())

//         #
//         task = self.tasks.get(sid)
//         if not task:
//             watch_queue.push(WatchQueueMessage({"status": "STOP"}).as_primitives())
//             return

//         # Add the newly created queue to the list of queues for the given submission
//         self._watcher_list(sid).add(queue_name)

//         # Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
//         for result_data in list(task.service_results.values()):
//             watch_queue.push(WatchQueueMessage({"status": "OK", "cache_key": result_data.key}).as_primitives())

//         for error_key in list(task.service_errors.values()):
//             watch_queue.push(WatchQueueMessage({"status": "FAIL", "cache_key": error_key}).as_primitives())

//     @elasticapm.capture_span(span_type='dispatcher')
//     def list_outstanding(self, sid: str, queue_name: str):
//         response_queue: NamedQueue[dict] = NamedQueue(queue_name, host=self.redis)
//         outstanding: defaultdict[str, int] = defaultdict(int)
//         task = self.tasks.get(sid)
//         if task:
//             for _sha, service_name in list(task.queue_keys.keys()):
//                 outstanding[service_name] += 1
//             for _sha, service_name in list(task.running_services):
//                 outstanding[service_name] += 1
//         response_queue.push(outstanding)


//     def update_bad_sids(self):
//         # Pull new sid list
//         remote_sid_list = set(self.redis_bad_sids.members())
//         new_sid_events = []

//         # Kick off updates for any new sids
//         for bad_sid in remote_sid_list - self.bad_sids:
//             self.bad_sids.add(bad_sid)
//             event = threading.Event()
//             self.find_process_queue(bad_sid).put(DispatchAction(kind=Action.bad_sid, sid=bad_sid, event=event))
//             new_sid_events.append(event)

//         # Wait for those updates to finish
//         for event in new_sid_events:
//             event.wait()
