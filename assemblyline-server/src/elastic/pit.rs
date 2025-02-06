use std::sync::Arc;

use log::error;
use serde_json::json;

use super::{responses, ElasticHelper, Request, Result};

pub (super) const PIT_KEEP_ALIVE: &str = "5m";


pub (super) struct PitGuard {
    helper: Arc<ElasticHelper>,
    pub (super) id: String,
}

impl PitGuard {
    pub async fn open(helper: Arc<ElasticHelper>, index: &str) -> Result<Self> {
        let response = helper.make_request(&mut 0, &Request::create_pit(&helper.host, index, PIT_KEEP_ALIVE)?).await?;
        let pit: responses::OpenPit = response.json().await?;
        Ok(Self { helper, id: pit.id })
    }

    async fn close(helper: Arc<ElasticHelper>, id: String) -> Result<()> {
        let response = helper.make_request_json(&mut 0, &Request::delete_pit(&helper.host)?, &json!({
            "id": id
        })).await?;
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
