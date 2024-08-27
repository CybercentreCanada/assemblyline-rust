
        
// import redis
// from assemblyline.remote.datatypes import get_client, retry_call

use std::sync::Arc;
use std::time::Duration;

use redis::AsyncCommands;

use crate::{retry_call, ErrorTypes, RedisObjects};


pub struct QuotaGuard {

}

impl QuotaGuard {
    pub fn new() -> Self {
        todo!()
    }
}

        // if submission.params.quota_item and submission.params.submitter:
        //     self.log.info(f"[{sid}] Submission no longer counts toward {submission.params.submitter.upper()} quota")
        //     self.quota_tracker.end(submission.params.submitter)

       
const BEGIN_SCRIPT: &str = r#"
local t = redis.call('time')
local key = tonumber(t[1] .. string.format("%06d", t[2]))

local name = ARGV[1]
local max = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3] .. "000000")

redis.call('zremrangebyscore', name, 0, key - timeout)
if redis.call('zcard', name) < max then
    redis.call('zadd', name, key, key)
    return true
else
    return false
end
"#;

#[derive(Clone)]
pub struct UserQuotaTracker {
    store: Arc<RedisObjects>,
    prefix: String,
    begin: redis::Script,
    timeout: Duration,
}

impl UserQuotaTracker {
    pub (crate) fn new(store: Arc<RedisObjects>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            begin: redis::Script::new(BEGIN_SCRIPT),
            timeout: Duration::from_secs(120)
        }
    }
    
    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn queue_name(&self, user: &str) -> String {
        format!("{}-{user}", self.prefix)
    }

    pub async fn begin(&self, user: &str, max_quota: u32) -> Result<bool, ErrorTypes> {
        let mut call = self.begin.key(self.queue_name(user));
        let call = call.arg(max_quota).arg(self.timeout.as_secs());
        Ok(retry_call!(method, self.store.pool, call, invoke_async)?)
    }

    pub async fn end(&self, user: &str) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, zpopmin, &self.queue_name(user), 1)?;
        Ok(())
    }
}