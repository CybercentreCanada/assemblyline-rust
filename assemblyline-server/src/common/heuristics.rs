// from __future__ import annotations

// import logging
// import typing

// from assemblyline.common.attack_map import attack_map, software_map, group_map, revoke_map
// from assemblyline.common.forge import CachedObject

// heur_logger = logging.getLogger("assemblyline.heuristics")

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use assemblyline_models::datastore::heuristic::Heuristic as DatastoreHeuristic;
use crate::service_api::v1::task::models::Heuristic as ServiceHeuristic;
use assemblyline_models::datastore::result::Heuristic as ResultHeuristic;
use assemblyline_models::{ExpandingClassification, Readable};
use log::{error, info, warn};
use parking_lot::Mutex;
use serde::Deserialize;

use crate::elastic::Elastic;

use super::attack_map::load_attack_map;



fn get_safelist_key(t_type: &str, t_value: &str) -> String {
    format!("{t_type}__{t_value}")
}


pub async fn get_safelist_signatures(ds: &Elastic) -> Result<HashSet<String>> {

    #[derive(Debug, Deserialize)]
    struct PartialSafelist {
        signature: PartialSignature,
    }
    impl Readable for PartialSafelist { fn set_from_archive(&mut self, _from_archive: bool) { } }

    #[derive(Debug, Deserialize)]
    struct PartialSignature {
        name: String,
    }

    let mut out: HashSet<String> = Default::default();
    let mut cursor = ds.safelist.stream_search::<PartialSafelist>("type:signature AND enabled:true", "signature.name".to_owned(), vec![], None, None, None).await?;
    while let Some(sl) = cursor.next().await? {
        out.insert(get_safelist_key("signature", &sl.signature.name));
    }
    Ok(out)
}

pub async fn safelist_watcher(ds: Arc<Elastic>) -> Result<Arc<Mutex<HashSet<String>>>> {
    let data = Arc::new(Mutex::new(get_safelist_signatures(&ds).await?));
    let input = data.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            info!("Load safelist signatures");

            if Arc::strong_count(&input) == 1 {
                return;
            }

            match get_safelist_signatures(&ds).await {
                Ok(value) => {
                    *input.lock() = value;
                },
                Err(err) => {
                    error!("Could not update signature safelist: {err}");
                },
            }
        }
    });
    Ok(data)
}


pub struct  HeuristicHandler {
    datastore: Arc<Elastic>,
    safelist: Arc<Mutex<HashSet<String>>>,
}

impl HeuristicHandler {
    pub async fn new(datastore: Arc<Elastic>) -> Result<Self> {
        Ok(Self {
            safelist: safelist_watcher(datastore.clone()).await?,
            datastore,
        })
    }

    pub fn service_heuristic_to_result_heuristic(&self, 
        srv_heuristic: ServiceHeuristic,
        heuristics: Arc<Mutex<HashMap<String, DatastoreHeuristic>>>, 
        zerioize_on_sig_safe: bool // =True
    ) -> Result<(ResultHeuristic, Vec<(String, String)>)> {
        // let heur_id = srv_heuristic.heur_id;
        // let attack_ids = srv_heuristic.attack_ids;
        // let signatures = srv_heuristic.signatures;
        // let frequency = srv_heuristic.frequency;
        // let score_map = srv_heuristic.score_map;

        // Validate the heuristic and recalculate its score
        let heuristic = Heuristic::new(srv_heuristic, heuristics)?;

        // Assign the newly computed heuristic to the section
        let mut output = ResultHeuristic {
            heur_id: heuristic.heur_id,
            score: heuristic.score,
            name: heuristic.name,
            attack: vec![],
            signature: vec![]
        };

        let attack = load_attack_map()?;
        let attack_map = &attack.attack_map;
        let revoke_map = &attack.revoke_map;
        let software_map = &attack.software_map;
        let group_map = &attack.group_map;

        // Assign the multiple attack IDs to the heuristic
        for attack_id in heuristic.attack_ids {
            let attack_item = if let Some(attack) = attack_map.get(&attack_id) {
                assemblyline_models::datastore::result::Attack{
                    attack_id,
                    pattern: attack.name.clone(),
                    categories: attack.categories.clone()
                }
            } else if let Some(soft) = software_map.get(&attack_id) {
                assemblyline_models::datastore::result::Attack{
                    pattern: if soft.name.is_empty() { attack_id.clone() } else { soft.name.clone() },
                    categories: vec!["software".to_owned()],
                    attack_id,
                }
            } else if let Some(group) = group_map.get(&attack_id) {
                assemblyline_models::datastore::result::Attack{
                    pattern: if group.name.is_empty() { attack_id.clone() } else { group.name.clone() },
                    categories: vec!["group".to_owned()],
                    attack_id,
                }
            } else {
                warn!("Could not generate Att&ck output for ID: {attack_id}");
                continue
            };

            output.attack.push(attack_item);
        }

        // Assign the multiple signatures to the heuristic
        {
            let safelist = self.safelist.lock();
            for (sig_name, freq) in &heuristic.signatures {
                let safelist_key = get_safelist_key("signature", sig_name);
                let signature_item = assemblyline_models::datastore::result::Signature{
                    name: sig_name.to_owned(),
                    frequency: *freq,
                    safe: safelist.get(&safelist_key).is_some()
                };
                output.signature.push(signature_item);
            }
        }

        let all_safe = output.signature.iter().all(|b| b.safe);
        // sig_safe_status = [s['safe'] for s in output['signature']]
        if !output.signature.is_empty() && all_safe {
            output.score = 0;
        }

        return Ok((output, heuristic.associated_tags))
        // except InvalidHeuristicException as e:
        //     heur_logger.warning(str(e))
        //     raise
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid heuristic. A heuristic with ID: {heur_id}, must be added to the service manifest before using it.")]
pub struct InvalidHeuristicException {
    heur_id: String
}


struct Heuristic {
    heur_id: String,
    name: String,
    attack_ids: Vec<String>,
    classification: ExpandingClassification,
    associated_tags: Vec<(String, String)>,
    signatures: HashMap<String, i32>,
    score: i32,
}

impl Heuristic {
    fn new(
        srv: ServiceHeuristic,
        heuristics: Arc<Mutex<HashMap<String, DatastoreHeuristic>>>,
    ) -> Result<Self> {
        // Validate heuristic
        let heur_id = srv.heur_id;
        let definition = match heuristics.lock().get(&heur_id) {
            Some(def) => def.clone(),
            None => {
                return Err(InvalidHeuristicException{heur_id}.into())
            }
        };

        let attack = load_attack_map()?;
        let attack_map = &attack.attack_map;
        let revoke_map = &attack.revoke_map;
        let group_map = &attack.group_map;

        // Set defaults
        let mut new = Self {
            heur_id: heur_id.clone(),
            name: definition.name,
            attack_ids: vec![],
            classification: definition.classification,
            associated_tags: vec![],
            signatures: Default::default(),
            score: 0,
        };

        // Show only attack_ids that are valid
        let mut attack_ids = srv.attack_ids;
        for id in attack_ids.iter_mut() {
            if let Some(new_id) = revoke_map.get(id) {
                *id = new_id.clone();
            }
        }
        for a_id in attack_ids {
            if attack_map.contains_key(&a_id) {
                new.attack_ids.push(a_id);
            } else if let Some(software_def) = attack.software_map.get(&a_id) {
                new.attack_ids.push(a_id.clone());
                let implant_name = software_def.name.clone();
                if !implant_name.is_empty() && software_def.type_ == "malware" {
                    new.associated_tags.push(("attribution.implant".to_string(), implant_name.to_uppercase()))
                }

                for s_a_id in &software_def.attack_ids {
                    if attack_map.contains_key(s_a_id) {
                        new.attack_ids.push(s_a_id.clone());
                    } else if let Some(replacement) = revoke_map.get(s_a_id) {
                        new.attack_ids.push(replacement.clone());
                    } else {
                        warn!("Invalid related attack_id '{s_a_id}' for software '{a_id}' in heuristic '{heur_id}'. Ignoring it.")
                    }
                }
            } else if let Some(group) = group_map.get(&a_id) {
                new.attack_ids.push(a_id);
                if !group.name.is_empty() {
                    new.associated_tags.push(("attribution.actor".to_owned(), group.name.to_uppercase()));
                }
            } else {
                warn!("Invalid attack_id '{a_id}' in heuristic '{heur_id}'. Ignoring it.");
            }
        }
        new.attack_ids.sort_unstable();
        new.attack_ids.dedup();

        // Calculate the score for the signatures
        new.signatures = srv.signatures;
        if new.signatures.len() > 0 {
            new.score = 0;
            for (sig_name, freq) in &new.signatures {
                let sig_score = match definition.signature_score_map.get(sig_name) {
                    Some(score) => *score,
                    None => match srv.score_map.get(sig_name) {
                        Some(score) => *score,
                        None => definition.score
                    }
                };
                new.score += sig_score * freq;
            }
        } else {
            // Calculate the score for the heuristic frequency
            new.score = definition.score * srv.frequency;
        }

        // Check scoring boundaries
        if let Some(max_score) = definition.max_score {
            new.score = new.score.min(max_score);
        }

        Ok(new)
    }
}