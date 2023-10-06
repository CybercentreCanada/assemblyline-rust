use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use assemblyline_models::Sid;
use assemblyline_models::config::Config;
use assemblyline_models::datastore::Submission;
use chrono::{DateTime, Utc};
use log::error;
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock;

use crate::error::Result;

#[derive(Clone)]
struct CacheEntry {
    status: Status,
    sid: Sid,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Status {
    Assigned(SocketAddr, bool, DateTime<Utc>),
    Finished(DateTime<Utc>)
}

#[derive(Serialize, Deserialize)]
struct DiskEntry {
    status: Status,
    submission: Arc<Submission>,
}

pub struct Database {
    path: PathBuf,
    cache: RwLock<HashMap<Sid, CacheEntry>>
}

const TEMP_PREFIX: &str = ".tmp.";

impl Database {
    pub async fn open(config: &Config) -> Result<Self> {
        let path = config.core.dispatcher.broker_storage_path.clone();
        let db = Database {
            path: PathBuf::from(path),
            cache: RwLock::new(Default::default()),
        };
        let mut cache = db.cache.write().await;

        let mut listing = tokio::fs::read_dir(&db.path).await?;
        for item in listing.next_entry().await {
            let item = match item {
                Some(item) => item,
                None => { error!("Directory listing error."); continue },
            };
            let path = item.file_name();
            let path = match path.to_str() {
                Some(path) => path,
                None => continue,
            };

            if path.starts_with(TEMP_PREFIX) {
                // clean up cache files
                tokio::fs::remove_file(path).await;
            } else {
                // catalog the items
                let item = match tokio::fs::read(path).await {
                    Ok(buffer) => buffer,
                    Err(_) => continue,
                };

                let item: DiskEntry = match serde_json::from_slice(&item) {
                    Ok(item) => item,
                    Err(_) => continue,
                };

                cache.insert(item.submission.sid, CacheEntry {
                    status: item.status,
                    sid: item.submission.sid,
                });
            }
        }
        drop(cache);

        return Ok(db)
    }

    pub async fn have_submission(&self, sid: &Sid) -> Result<bool> {
        Ok(self.cache.read().await.contains_key(sid))
    }

    pub async fn get_submission(&self, sid: &Sid) -> Result<Option<Submission>> {
        let path = self.path.join(sid.to_string());
        if path.exists() {
            Ok(Some(serde_json::from_slice(&tokio::fs::read(path).await?)?))
        } else {
            Ok(None)
        }
    }

    pub async fn get_status(&self, sid: &Sid) -> Result<Option<Status>> {
        Ok(self.cache.read().await.get(sid).map(|value|value.status.clone()))
    }

    pub async fn get_assignment(&self, sid: &Sid) -> Result<Option<SocketAddr>> {
        match self.cache.read().await.get(sid) {
            Some(entry) => match &entry.status {
                Status::Assigned(assign, _, _) => Ok(Some(assign.clone())),
                Status::Finished(_) => Ok(None),
            },
            None => Ok(None),
        }
    }

    pub async fn assign_submission(&self, submission: Arc<Submission>, dispatcher: SocketAddr, retain: bool) -> Result<()> {
        let temp = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(&self.path)?;
        let sid = submission.sid;

        let entry = DiskEntry {
            status: Status::Assigned(dispatcher, retain, Utc::now()),
            submission,
        };
        tokio::fs::write(temp.path(), serde_json::to_vec(&entry)?).await?;

        temp.persist(self.path.join(sid.to_string()));
        return Ok(())
    }

    pub async fn list_all_submissions(&self) -> Result<Vec<(Sid, Status)>> {
        Ok(self.cache.read().await.iter().map(|(sid, cache)|{
            (*sid, cache.status.clone())
        }).collect())
    }

    pub async fn list_assigned_submissions(&self) -> Result<Vec<(Sid, SocketAddr, bool)>> {
        Ok(self.cache.read().await.iter().filter_map(|(sid, cache)|{
            match &cache.status {
                Status::Assigned(dispatcher, retain, _) => Some((*sid, dispatcher.clone(), *retain)),
                Status::Finished(_) => None,
            }
        }).collect())
    }

    pub async fn delete_submission(&self, sid: &Sid) -> Result<()> {
        self.cache.write().await.remove(sid);
        match tokio::fs::remove_file(self.path.join(sid.to_string())).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let kind = err.kind();
                match kind {
                    std::io::ErrorKind::NotFound => Ok(()),
                    _ => Err(err.into()),
                }
            },
        }
    }

    pub async fn delete(&self, sid: Sid) -> Result<()> {
        todo!();
    }

    pub async fn finish_submission(&self, submission: Arc<Submission>) -> Result<()> {
        todo!("Drop if retain is false");
        let temp = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(&self.path)?;
        let sid = submission.sid;
        let finished = submission.times.completed.unwrap_or(Utc::now());

        let entry = DiskEntry {
            status: Status::Finished(finished),
            submission,
        };
        tokio::fs::write(temp.path(), serde_json::to_vec(&entry)?).await?;

        temp.persist(self.path.join(sid.to_string()));
        return Ok(())
    }

    pub async fn clear_abandoned(&self) -> Result<()> {
        let mut cache = self.cache.write().await;
        for (sid, entry) in cache.clone().into_iter() {
            if let Status::Finished(finished) = entry.status {
                if (Utc::now() - finished) > chrono::Duration::hours(2) {
                    cache.remove(&sid);
                    _ = tokio::fs::remove_file(self.path.join(sid.to_string())).await;
                }
            }
        }
        return Ok(())
    }
}
