//! Extension to the core module for accessing information about dispatcher instances

use std::str::FromStr;

use anyhow::Result;

use assemblyline_models::messages::task::Task;
use assemblyline_models::messages::dispatching::SubmissionDispatchMessage;
use assemblyline_models::Sid;

use crate::constants::service_queue_name;
use crate::Core;

pub const DISPATCH_DIRECTORY: &str = "dispatchers-directory";
pub const DISPATCH_TASK_ASSIGNMENT: &str = "dispatcher-tasks-assigned-to-";


impl Core {

    pub async fn dispatcher_instances(&self) -> Result<Vec<String>, redis_objects::ErrorTypes> {
        self.redis_persistant.hashmap::<i64>(DISPATCH_DIRECTORY.to_owned(), None).keys().await
    }

    pub async fn dispatcher_assignment_size(&self, instance_id: &str) -> Result<u64, redis_objects::ErrorTypes> {
        self.redis_persistant.hashmap::<SubmissionDispatchMessage>(DISPATCH_TASK_ASSIGNMENT.to_owned() + instance_id, None).length().await
    }

    pub async fn dispatcher_assignment(&self, instance_id: &str) -> Result<Vec<Sid>> {
        Ok(self.redis_persistant.hashmap::<SubmissionDispatchMessage>(DISPATCH_TASK_ASSIGNMENT.to_owned() + instance_id, None).keys().await?
            .into_iter().map(|str|Sid::from_str(&str))
            .collect::<Result<Vec<Sid>, _>>()?
        )
    }

    pub fn get_service_queue(&self, service: &str) -> redis_objects::PriorityQueue<Task> {
        self.redis_persistant.priority_queue(service_queue_name(service))
    }

    // def dispatcher_queue_lengths(redis, instance_id):
    //     return {
    //         'start': NamedQueue(DISPATCH_START_EVENTS + instance_id, host=redis).length(),
    //         'result': NamedQueue(DISPATCH_RESULT_QUEUE + instance_id, host=redis).length(),
    //         'command': NamedQueue(DISPATCH_COMMAND_QUEUE + instance_id, host=redis).length()
    //     }

}