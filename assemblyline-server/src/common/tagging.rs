use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use assemblyline_models::config::Config;
use assemblyline_models::datastore::tagging::{FlatTags, TagInformation, TagValue};
use assemblyline_models::datastore::Tagging;
use assemblyline_models::Readable;
use log::{debug, error, info};
use parking_lot::Mutex;
use regex::Regex;
use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::core_util::get_tag_safelist_data;
use crate::elastic::Elastic;

use super::odm::flat_fields;

// from __future__ import annotations
// import re

// from assemblyline.common.forge import CachedObject, get_datastore
// from assemblyline.odm.models.tagging import Tagging


// def tag_list_to_dict(tag_list: list[dict]) -> dict:
//     tag_dict = {}
//     for t in tag_list:
//         if t['type'] not in tag_dict:
//             tag_dict[t['type']] = []
//         tag_dict[t['type']].append(t['value'])

//     return tag_dict


// def tag_dict_to_list(tag_dict: dict, safelisted: bool = False) -> list[dict]:
//     return [
//         {'safelisted': safelisted, 'type': k, 'value': t, 'short_type': k.rsplit(".", 1)[-1]}
//         for k, v in tag_dict.items()
//         if v is not None
//         for t in v
//     ]


// def tag_dict_to_ai_list(tag_dict: dict) -> list[dict]:
//     return [
//         {'type': k, 'value': t}
//         for k, v in tag_dict.items()
//         if v is not None
//         for t in v
//     ]


// def get_safelist_key(t_type: str, t_value: str) -> str:
//     return f"{t_type}__{t_value}"



fn get_safelist_key(tag_type: &str, tag_value: &str) -> String {
    format!("{tag_type}__{tag_value}")
}


pub async fn get_safelist(ds: &Elastic) -> Result<HashSet<String>> {

    #[derive(Debug, Deserialize)]
    struct PartialSafelist {
        tag: PartialTag,
    }
    impl Readable for PartialSafelist { fn set_from_archive(&mut self, _from_archive: bool) { } }

    #[derive(Debug, Deserialize)]
    struct PartialTag {
        #[serde(rename="type")]
        type_: String,
        value: String,
    }

    let mut out: HashSet<String> = Default::default();
    let mut cursor = ds.safelist.stream_search::<PartialSafelist>("type:tag AND enabled:true", "tag.type,tag.value".to_owned(), vec![], None, None, None).await?;
    while let Some(sl) = cursor.next().await? {
        out.insert(get_safelist_key(&sl.tag.type_, &sl.tag.value));
    }
    Ok(out)
}

pub async fn safelist_watcher(ds: Arc<Elastic>) -> Result<Arc<Mutex<HashSet<String>>>> {
    let data = Arc::new(Mutex::new(get_safelist(&ds).await?));
    let input = data.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            debug!("Loading the safelist from datastore");

            if Arc::strong_count(&input) == 1 {
                return;
            }

            match get_safelist(&ds).await {
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

#[derive(Debug, thiserror::Error)]
#[error("Key ({key}) in the {section} section is not a valid tag.")]
pub struct InvalidSafelist {
    key: String,
    section: String,
}


#[derive(Serialize, Deserialize)]
pub struct SafelistFile {
    #[serde(rename="match")]
    pub match_: HashMap<String, Vec<String>>,
    pub regex: HashMap<String, Vec<String>>,
}

pub struct TagSafelister {
    datastore: Arc<Elastic>,
    safelist: Arc<Mutex<HashSet<String>>>,
    matched: HashMap<String, Vec<String>>,
    regex: HashMap<String, Vec<Regex>>,
}

impl TagSafelister {
    pub async fn new(datastore: Arc<Elastic>, data: SafelistFile) -> Result<Self> {
        let mut new = Self {
            safelist: safelist_watcher(datastore.clone()).await?,
            datastore,
            matched: Default::default(),
            regex: Default::default()
        };

        let valid_tags = flat_fields(Tagging::metadata())?;
        
        // Validate matches and regex
        for (section, item) in [("match", data.match_), ("regex", data.regex)] {
            for (k, v) in item {
                if !valid_tags.contains_key(&k) {
                    return Err(InvalidSafelist{ key: k, section: section.to_owned()}.into());
                }

                if section == "regex" {
                    let mut regexes = vec![];
                    for exp in v {
                        regexes.push(Regex::new(&exp)?);
                    }
                    new.regex.insert(k, regexes);
                } else {
                    new.matched.insert(k, v);
                }
            }
        }
        Ok(new)
    }

    pub fn _is_safelisted(&self, safelist: &HashSet<String>, t_type: &'static TagInformation, t_value: &TagValue) -> bool {
        let tag_label = t_type.full_path();
        let tag_value_string = t_value.to_string();

        if safelist.contains(&get_safelist_key(&tag_label, &tag_value_string)) {
            info!("Tag '{tag_label}' with value '{tag_value_string}' was safelisted.");
            return true
        }

        if let Some(possible_matches) = self.matched.get(&tag_label) {
            for possible_match in possible_matches {
                if tag_value_string == *possible_match {
                    info!("Tag '{tag_label}' with value '{tag_value_string}' was safelisted by match rule.");
                    return true
                }
            }
        }

        if let Some(possible_regexes) = self.regex.get(&tag_label) {
            for regex in possible_regexes {
                if regex.is_match(&tag_value_string) {
                    info!("Tag '{tag_label}' with value '{tag_value_string}' was safelisted by regex '{}'.", regex.as_str());
                    return true
                }
            }
        }

        return false
    }

    pub fn safelist_many(&self, t_type: &'static TagInformation, t_values: Vec<TagValue>) -> (Vec<TagValue>, Vec<TagValue>) {
        let mut tags = vec![];
        let mut safelisted_tags = vec![];
        let safelist = self.safelist.lock();
        for x in t_values {
            if self._is_safelisted(&safelist, t_type, &x) {
                safelisted_tags.push(x)
            } else {
                tags.push(x)
            }
        }
        (tags, safelisted_tags)
    }

    pub fn get_validated_tag_map(&self, tag_map: FlatTags) -> (FlatTags, FlatTags) {
        let mut tags = FlatTags::default();
        let mut safelisted_tags = FlatTags::default();
        for (tag, values) in tag_map {
            if values.is_empty() { continue }

            let (kept_tags, tags_marked_safe) = self.safelist_many(tag, values);
            if !kept_tags.is_empty() {
                tags.insert(tag, kept_tags);
            }
            if !tags_marked_safe.is_empty() {
                safelisted_tags.insert(tag, tags_marked_safe);
            }
        }

        (tags, safelisted_tags)
    }
}

pub async fn tag_safelist_watcher(config: Arc<Config>, ds: Arc<Elastic>, config_path: Option<PathBuf>) -> Result<Arc<Mutex<Arc<TagSafelister>>>> {
    let safelist_data = get_tag_safelist_data(&config, ds.clone(), config_path.clone()).await?;
    let data = Arc::new(Mutex::new(Arc::new(TagSafelister::new(ds.clone(), safelist_data).await?)));
    let input = data.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            debug!("Loading the safelist from cache and datastore");

            if Arc::strong_count(&input) == 1 {
                return;
            }

            let new_data = match get_tag_safelist_data(&config, ds.clone(), config_path.clone()).await {
                Ok(value) => value,
                Err(err) => {
                    error!("Could not update tag safelist: {err}");
                    continue
                },
            };

            match TagSafelister::new(ds.clone(), new_data).await {
                Ok(value) => {
                    *input.lock() = Arc::new(value);
                },
                Err(err) => {
                    error!("Could not update tag safelist: {err}");
                    continue
                },
            }
        }
    });
    Ok(data)
}