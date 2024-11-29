
// import hashlib
// import logging

// from assemblyline.common import forge
// from assemblyline.common.chunk import chunk
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.datastore.helper import AssemblylineDatastore
// from assemblyline.odm.models.user import ROLES
// from assemblyline.remote.datatypes.lock import Lock


const CHUNK_SIZE: u64 = 1000;
// CLASSIFICATION = forge.get_classification()

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::config::Config;
use assemblyline_models::datastore::badlist;
use assemblyline_models::datastore::badlist::Badlist;
use assemblyline_models::datastore::user::{User, UserRole};
use assemblyline_models::ExpandingClassification;
use chrono::{TimeDelta, Utc};
use itertools::Itertools;
use log::warn;
use digest::Digest;
use serde::Deserialize;
use sha2::Sha256;
use anyhow::{bail, Result};
use thiserror::Error;

use crate::common::PermissionError;
use crate::elastic::Elastic;


#[derive(Debug, Error)]
#[error("{0}")]
pub struct InvalidBadhash(String);


/// A helper class to simplify badlisting for privileged services and service-server.
pub struct BadlistClient {
    config: Arc<Config>,
    datastore: Arc<Elastic>,
    ce: Arc<ClassificationParser>,
}

impl BadlistClient {
    pub fn new(datastore: Arc<Elastic>, config: Arc<Config>, ce: Arc<ClassificationParser>) -> Self {
        Self { datastore, ce, config }
    }

    pub fn _preprocess_object(&self, data: RequestBadlist) -> Result<(String, Badlist)> {
        // get defaults
        let cln = match &data.body().classification {
            Some(cln) => cln.clone(),
            None => self.ce.unrestricted().to_string()
        };
        let classification = ExpandingClassification::new(cln, &self.ce)?;

        // Ensure expiry_ts is set on tag-related items
        let expiry_ts = match data.body().dtl {
            Some(days) => if days > 0 { 
                Some(Utc::now() + TimeDelta::days(days.into())) 
            } else {
                None
            },
            None => Some(Utc::now() + TimeDelta::days(self.config.core.expiry.badlisted_tag_dtl.into())),
        };

        // convert the different inputs to a badlist object
        let badlist = match data {
            RequestBadlist::Tag{ body, tag } => {
                // tag_data = data.get("tag", None)
                // if tag_data is None or "type" not in tag_data or "value" not in tag_data {
                //     raise ValueError("Tag data not found")
                // }

                let mut hasher = sha2::Sha256::new();
                hasher.write_all(format!("{}: {}", tag.tag_type, tag.value).as_bytes())?;
                let hashes = badlist::Hashes {
                    sha256: Some(hasher.finalize().as_slice().try_into()?),
                    ..Default::default()
                };
                Badlist {
                    added: Utc::now(),
                    attribution: body.attribution,
                    classification,
                    enabled: body.enabled,
                    expiry_ts,
                    file: None,
                    hash_type: badlist::BadhashTypes::Tag,
                    hashes,
                    sources: body.sources,
                    tag: Some(tag),
                    updated: Utc::now(),    
                }
            },
            RequestBadlist::File{ body, file, hashes} => {
                Badlist {
                    added: Utc::now(),
                    attribution: body.attribution,
                    classification,
                    enabled: body.enabled,
                    expiry_ts,
                    file: Some(file),
                    hash_type: badlist::BadhashTypes::File,
                    hashes,
                    sources: body.sources,
                    tag: None,
                    updated: Utc::now(),
                }
            }
        };
       
        // Find the best hash to use for the key
        let qhash = badlist.hashes.label_hash();

        // Validate hash length
        let qhash = match qhash {
            Some(hash) => hash,
            None => bail!("No valid hash found")
        };

        Ok((qhash, badlist))
    }

    pub async fn add_update(&self, badlist_object: RequestBadlist, user: Option<User>) -> Result<(String, &'static str)> {
        let (qhash, mut badlist_object) = self._preprocess_object(badlist_object)?;

        // Validate sources
        let mut src_map = HashMap::<String, badlist::Source>::new();
        let mut classification = badlist_object.classification.classification;
        for src in badlist_object.sources {
            if let Some(user) = &user {
                if src.source_type == badlist::SourceTypes::User {
                    if src.name != user.uname {
                        bail!("You cannot add a source for another user. {} != {}", src.name, user.uname);
                    }
                } else {
                    if !user.roles.contains(&UserRole::SignatureImport) {
                        return Err(PermissionError("You do not have sufficient priviledges to add an external source.".to_owned()).into())
                    }
                }
            }

            // Find the highest classification of all sources
            classification = self.ce.max_classification(&classification, src.classification.as_str(), None)?;
            src_map.insert(src.name.clone(), src);
        }
        badlist_object.classification = ExpandingClassification::new(classification, &self.ce)?;
        badlist_object.sources = src_map.into_values().collect();

        // Save data to the DB
        loop {
            if let Some((old, version)) = self.datastore.badlist.get_if_exists(&qhash, None).await? {
                let old = self.merge_hashes(badlist_object.clone(), old)?;
                if let Err(err) = self.datastore.badlist.save(&qhash, &old, Some(version), None).await {
                    if err.is_version_conflict() { continue }
                    return Err(err.into())
                }
                return Ok((qhash, "update"))
            } else {
                if let Err(err) = self.datastore.badlist.save(&qhash, &badlist_object, Some(crate::elastic::Version::Create), None).await {
                    if err.is_version_conflict() { continue }
                    return Err(err.into())
                }
                return Ok((qhash, "add"))
            }
        }
    }

    // def add_update_many(self, list_of_badlist_objects: list):
    //     if not isinstance(list_of_badlist_objects, list):
    //         raise ValueError("Could not get the list of hashes")

    //     new_data = {}
    //     for badlist_object in list_of_badlist_objects:
    //         qhash = self._preprocess_object(badlist_object)
    //         new_data[qhash] = badlist_object

    //     # Get already existing hashes
    //     old_data = self.datastore.badlist.multiget(list(new_data.keys()), as_dictionary=True, as_obj=False,
    //                                                error_on_missing=False)

    //     # Test signature names
    //     plan = self.datastore.badlist.get_bulk_plan()
    //     for key, val in new_data.items():
    //         # Use maximum classification
    //         old_val = old_data.get(key, {'classification': CLASSIFICATION.UNRESTRICTED, 'attribution': {},
    //                                      'hashes': {}, 'sources': [], 'type': val['type']})

    //         # Add upsert operation
    //         plan.add_upsert_operation(key, BadlistClient._merge_hashes(val, old_val))

    //     if not plan.empty:
    //         # Execute plan
    //         res = self.datastore.badlist.bulk(plan)
    //         return {"success": len(res['items']), "errors": res['errors']}

    //     return {"success": 0, "errors": []}

    pub async fn exists(&self, qhash: &str) -> Result<Option<Badlist>> {
        Ok(self.datastore.badlist.get_if_exists(qhash, None).await?.map(|(obj, _)| obj))
    }

    pub async fn exists_tags(&self, tag_map: HashMap<String, Vec<String>>) -> Result<Vec<Badlist>> {
        let mut lookup_keys = vec![];
        for (tag_type, tag_values) in tag_map {
            for tag_value in tag_values {
                let mut hasher = Sha256::new();
                hasher.update(format!("{tag_type}: {tag_value}").as_bytes());
                lookup_keys.push(hex::encode(hasher.finalize().as_slice()));
            }
        }

        // Elasticsearch's result window can't be more than 10000 rows
        // we will query for matches in chunks
        let mut results = vec![];
        for key_chunk in lookup_keys.chunks(CHUNK_SIZE as usize) {
            let mut res = self.datastore.badlist.search("*")
                .fields("*")
                .rows(CHUNK_SIZE)
                .key_space(key_chunk)
                .execute::<()>().await?;

            results.append(&mut res.source_items);
            // results += self.datastore.badlist.search("*", fl="*", rows=CHUNK_SIZE,
            //                                          as_obj=False, key_space=key_chunk)['items']
        }
        Ok(results)
    }

    pub async fn find_similar_tlsh(&self, tlsh: &str) -> Result<Vec<Badlist>> {
        Ok(self.datastore.badlist.search(&format!("hashes.tlsh:{tlsh}"))
            .fields("*")
            .execute::<()>().await?.source_items)
    }

    pub async fn find_similar_ssdeep(&self, ssdeep: &str) -> Result<Vec<Badlist>> {
        let ssdeep = ssdeep.replace('/', "\\/");
        let long = match ssdeep.split(":").collect_tuple() {
            Some((_, long, _)) => long,
            _ => {
                warn!("This is not a valid SSDeep hash: {ssdeep}");
                return Ok(vec![])
            }
        };

        Ok(self.datastore.badlist.search(&format!("hashes.ssdeep:{long}~"))
            .fields("*")
            .execute::<()>().await?.source_items)
    }

    fn merge_hashes(&self, new: Badlist, mut old: Badlist) -> Result<Badlist> {
        // Account for the possibility of merging with null types
        // if not (new or old) {
        //     # Both are null
        //     raise ValueError("New and old are both null")
        // } elif not (new and old):
        //     # Only one is null, in which case return the other
        //     return new or old

        // Check if hash types match
        if new.hash_type != old.hash_type {
            return Err(InvalidBadhash(format!("Bad hash type mismatch: {} != {}", new.hash_type, old.hash_type)).into())
        }

        // Use the new classification but we will recompute it later anyway
        old.classification = new.classification;

        // Update updated time
        old.updated = new.updated;

        // Update hashes
        // old.hashes.update({k: v for k, v in new["hashes"].items() if v})
        old.hashes.update(new.hashes);

        // Merge attributions
        if let Some(old_attr) = &mut old.attribution {
            if let Some(new_attr) = new.attribution {
                old_attr.update(new_attr);
            }
        } else {
            old.attribution = new.attribution;
        }
        
        // if let Some(attr) = old.attribution {
        //     old["attribution"] = {key: value for key, value in old["attribution"].items() if value}
        // }

        // Update type specific info
        match old.hash_type { 
            badlist::BadhashTypes::File => {
                if let Some(file) = &mut old.file {
                    if let Some(mut new_file) = new.file {
                        file.name.append(&mut new_file.name);
                        file.name.sort_unstable();
                        file.name.dedup();

                        file.file_type = new_file.file_type.or(file.file_type.take());
                        file.size = new_file.size.or(file.size);
                    }
                } else {
                    old.file = new.file;
                } 
            },
            badlist::BadhashTypes::Tag => {
                old.tag = new.tag;
            }
        }        

        // Merge sources
        if new.sources.is_empty() {
            return Err(InvalidBadhash("No valid source found".to_owned()).into())
        }

        let mut old_src_map: HashMap<String, _> = old.sources.into_iter().map(|src|(src.name.clone(), src)).collect();
        for src in new.sources {
            match old_src_map.get_mut(&src.name) {
                Some(old_src) => {
                    if old_src.source_type != src.source_type {
                        return Err(InvalidBadhash(format!("Source {} has a type conflict: {} != {}", src.name, old_src.source_type, src.source_type)).into())
                    }
    
                    for reason in src.reason {
                        if !old_src.reason.contains(&reason) {
                            old_src.reason.push(reason)
                        }
                    }
                    old_src.classification = src.classification;
                },
                None => {
                    old_src_map.insert(src.name.clone(), src);
                }
            }
        }
        old.sources = old_src_map.into_values().collect();

        // Calculate the new classification
        let mut classification = old.classification.classification;
        for src in &old.sources {
            classification = self.ce.max_classification(&classification, src.classification.as_str(), None)?;
        }
        old.classification = ExpandingClassification::new(classification, &self.ce)?;

        // Set the expiry
        old.expiry_ts = match (old.expiry_ts, new.expiry_ts) {
            (None, _) | (_, None) => None,
            (Some(a), Some(b)) => Some(a.max(b)),
        };
        Ok(old)
    }
}

#[derive(Deserialize)]
#[serde(tag="type", rename_all="lowercase")]
pub enum RequestBadlist {
    File {
        #[serde(flatten)]
        body: CommonRequestBadlist,
        file: badlist::File,
        hashes: badlist::Hashes,
    },
    Tag {
        #[serde(flatten)]
        body: CommonRequestBadlist,
        tag: badlist::Tag,
    }
}

#[derive(Deserialize)]
pub struct CommonRequestBadlist {
    pub classification: Option<String>,
    pub enabled: bool,
    pub dtl: Option<u32>,
    pub attribution: Option<badlist::Attribution>,
    pub sources: Vec<badlist::Source>,
}

impl RequestBadlist {
    fn body(&self) -> &CommonRequestBadlist {
        match self {
            RequestBadlist::Tag { body, .. } => body,
            RequestBadlist::File { body, .. } => body,
        }
    }
}
