
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
use std::sync::Arc;

use assemblyline_models::datastore::badlist::Badlist;
use itertools::Itertools;
use log::warn;
use md5::Digest;
use sha2::Sha256;
use anyhow::Result;
use thiserror::Error;

use crate::elastic::Elastic;


#[derive(Debug, Error)]
#[error("{0}")]
pub struct InvalidBadhash(String);


/// A helper class to simplify badlisting for privileged services and service-server.
pub struct BadlistClient {
    datastore: Arc<Elastic>,
}

impl BadlistClient {
    pub fn new(datastore: Arc<Elastic>) -> Self {
        Self { datastore }
    }

    // pub fn _preprocess_object(&self, data: JsonMap) -> String {
    //     # Set defaults
    //     data.setdefault('classification', CLASSIFICATION.UNRESTRICTED)
    //     data.setdefault('hashes', {})
    //     data.setdefault('expiry_ts', None)
    //     if data['type'] == 'tag':
    //         # Remove file related fields
    //         data.pop('file', None)
    //         data.pop('hashes', None)

    //         tag_data = data.get('tag', None)
    //         if tag_data is None or 'type' not in tag_data or 'value' not in tag_data:
    //             raise ValueError("Tag data not found")

    //         hashed_value = f"{tag_data['type']}: {tag_data['value']}".encode('utf8')
    //         data['hashes'] = {
    //             'sha256': hashlib.sha256(hashed_value).hexdigest()
    //         }

    //     elif data['type'] == 'file':
    //         data.pop('tag', None)
    //         data.setdefault('file', {})

    //     # Ensure expiry_ts is set on tag-related items
    //     dtl = data.pop('dtl', None) or self.config.core.expiry.badlisted_tag_dtl
    //     if dtl:
    //         data['expiry_ts'] = now_as_iso(dtl * 24 * 3600)

    //     # Set last updated
    //     data['added'] = data['updated'] = now_as_iso()

    //     # Find the best hash to use for the key
    //     for hash_key in ['sha256', 'sha1', 'md5', 'tlsh', 'ssdeep']:
    //         qhash = data['hashes'].get(hash_key, None)
    //         if qhash:
    //             break

    //     # Validate hash length
    //     if not qhash:
    //         raise ValueError("No valid hash found")

    //     return qhash
    // }

    // def add_update(self, badlist_object: dict, user: dict = None):
    //     qhash = self._preprocess_object(badlist_object)

    //     # Validate sources
    //     src_map = {}
    //     for src in badlist_object['sources']:
    //         if user:
    //             if src['type'] == 'user':
    //                 if src['name'] != user['uname']:
    //                     raise ValueError(f"You cannot add a source for another user. {src['name']} != {user['uname']}")
    //             else:
    //                 if ROLES.signature_import not in user['roles']:
    //                     raise PermissionError("You do not have sufficient priviledges to add an external source.")

    //         # Find the highest classification of all sources
    //         badlist_object['classification'] = CLASSIFICATION.max_classification(
    //             badlist_object['classification'], src.get('classification', None))

    //         src_map[src['name']] = src

    //     with Lock(f'add_or_update-badlist-{qhash}', 30):
    //         old = self.datastore.badlist.get_if_exists(qhash, as_obj=False)
    //         if old:
    //             # Save data to the DB
    //             self.datastore.badlist.save(qhash, BadlistClient._merge_hashes(badlist_object, old))
    //             return qhash, "update"
    //         else:
    //             try:
    //                 badlist_object['sources'] = list(src_map.values())
    //                 self.datastore.badlist.save(qhash, badlist_object)
    //                 return qhash, "add"
    //             except Exception as e:
    //                 return ValueError(f"Invalid data provided: {str(e)}")

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

    // @staticmethod
    // def _merge_hashes(new, old):
    //     # Account for the possibility of merging with null types
    //     if not (new or old):
    //         # Both are null
    //         raise ValueError("New and old are both null")
    //     elif not (new and old):
    //         # Only one is null, in which case return the other
    //         return new or old

    //     try:
    //         # Check if hash types match
    //         if new['type'] != old['type']:
    //             raise InvalidBadhash(f"Bad hash type mismatch: {new['type']} != {old['type']}")

    //         # Use the new classification but we will recompute it later anyway
    //         old['classification'] = new['classification']

    //         # Update updated time
    //         old['updated'] = new.get('updated', now_as_iso())

    //         # Update hashes
    //         old['hashes'].update({k: v for k, v in new['hashes'].items() if v})

    //         # Merge attributions
    //         if not old['attribution']:
    //             old['attribution'] = new.get('attribution', None)
    //         elif new.get('attribution', None):
    //             for key in ["actor", 'campaign', 'category', 'exploit', 'implant', 'family', 'network']:
    //                 old_value = old['attribution'].get(key, []) or []
    //                 new_value = new['attribution'].get(key, []) or []
    //                 old['attribution'][key] = list(set(old_value + new_value)) or None

    //         if old['attribution'] is not None:
    //             old['attribution'] = {key: value for key, value in old['attribution'].items() if value}

    //         # Update type specific info
    //         if old['type'] == 'file':
    //             old.setdefault('file', {})
    //             new_names = new.get('file', {}).pop('name', [])
    //             if 'name' in old['file']:
    //                 for name in new_names:
    //                     if name not in old['file']['name']:
    //                         old['file']['name'].append(name)
    //             elif new_names:
    //                 old['file']['name'] = new_names
    //             old['file'].update({k: v for k, v in new.get('file', {}).items() if v})
    //         elif old['type'] == 'tag':
    //             old['tag'] = new['tag']

    //         # Merge sources
    //         src_map = {x['name']: x for x in new['sources']}
    //         if not src_map:
    //             raise InvalidBadhash("No valid source found")

    //         old_src_map = {x['name']: x for x in old['sources']}
    //         for name, src in src_map.items():
    //             if name not in old_src_map:
    //                 old_src_map[name] = src
    //             else:
    //                 old_src = old_src_map[name]
    //                 if old_src['type'] != src['type']:
    //                     raise InvalidBadhash(f"Source {name} has a type conflict: {old_src['type']} != {src['type']}")

    //                 for reason in src['reason']:
    //                     if reason not in old_src['reason']:
    //                         old_src['reason'].append(reason)
    //                 old_src['classification'] = src.get('classification', old_src['classification'])
    //         old['sources'] = list(old_src_map.values())

    //         # Calculate the new classification
    //         for src in old['sources']:
    //             old['classification'] = CLASSIFICATION.max_classification(
    //                 old['classification'], src.get('classification', None))

    //         # Set the expiry
    //         old['expiry_ts'] = new.get('expiry_ts', None)
    //         return old
    //     except Exception as e:
    //         raise InvalidBadhash(f"Invalid data provided: {str(e)}")
}