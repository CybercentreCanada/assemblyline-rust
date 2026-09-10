use std::sync::Arc;

use log::error;
use serde_json::json;

use super::{responses, ElasticHelper, Request, Result};

pub (super) const PIT_KEEP_ALIVE: &str = "5m";

pub(super) fn close_body(backend: super::Backend, id: String) -> serde_json::Value {
    match backend {
        super::Backend::Elasticsearch => json!({"id": id}),
        super::Backend::Opensearch => json!({"pit_id": [id]}),
    }
}

pub (super) struct PitGuard {
    helper: Arc<ElasticHelper>,
    pub (super) id: String,
}

impl PitGuard {
    pub async fn open(helper: Arc<ElasticHelper>, index: &str) -> Result<Self> {
        let response = helper.make_request(&mut 0, &Request::create_pit(&helper.host, index, PIT_KEEP_ALIVE, helper.backend)?).await?;
        let pit: responses::OpenPit = response.json().await?;
        Ok(Self { helper, id: pit.id })
    }

    async fn close(helper: Arc<ElasticHelper>, id: String) -> Result<()> {
        let body = close_body(helper.backend, id);
        let response = helper.make_request_json(&mut 0, &Request::delete_pit(&helper.host, helper.backend)?, &body).await?;
        let _body: responses::ClosePit = response.json().await?;
        Ok(())
    }
}

impl Drop for PitGuard {
    fn drop(&mut self) {
        let mut id = String::new();
        std::mem::swap(&mut self.id, &mut id);
        if !id.is_empty() {
            let helper = self.helper.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::close(helper, id).await {
                    error!("Error closing pit: {err}");
                }
            });
        }
    }
}
