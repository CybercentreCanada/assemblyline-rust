//! Tools for identifying files
//! 
//! TODO. Right now it uses tokio fs, probably faster if the entire thing is in a blocking thread

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use assemblyline_models::datastore::file::URIInfo;
use assemblyline_models::{SSDeepHash, Sha1, Sha256, MD5};
use digests::{get_digests_for_file_blocking, Digests, DEFAULT_BLOCKSIZE};
use itertools::Itertools;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use pyo3::ffi::c_str;
use pyo3::intern;
use pyo3::types::{IntoPyDict, PyAnyMethods};
use redis_objects::RedisObjects;
use serde::Serialize;
use tokio::sync::mpsc;
use zip::unstable::LittleEndianReadExt;

use magic_sys as _;

use crate::cachestore::CacheStore;
use crate::string_utils::{dotdump, dotdump_bytes, find_subsequence};
use crate::IBool;

use defaults::{ole_clsid_guids, untrusted_mimes, MAGIC_PATTERNS as default_magic_patterns, TRUSTED_MIMES};

// import json
// import logging
// import os
// import re
// import struct
// import subprocess
// import sys
// import threading
// import uuid
// import zipfile
// from binascii import hexlify
// from tempfile import NamedTemporaryFile
// from typing import Dict, Optional, Tuple, Union
// from urllib.parse import unquote, urlparse

// import magic
// import msoffcrypto
// import yaml
// import yara

// from assemblyline.common.digests import DEFAULT_BLOCKSIZE, get_digests_for_file
// from assemblyline.common.forge import get_cachestore, get_config, get_constants, get_datastore
// from assemblyline.common.identify_defaults import OLE_CLSID_GUIDs
// from assemblyline.common.identify_defaults import trusted_mimes as default_trusted_mimes
// from assemblyline.common.str_utils import dotdump, safe_str
// from assemblyline.filestore import FileStoreException
// from assemblyline.remote.datatypes.events import EventWatcher
// from cart import get_metadata_only, unpack_file
use cart_container::cart::unpack_header;

// constants = get_constants()

mod defaults;
mod digests;
mod entropy;

#[cfg(test)]
mod test;

const SYSTEM_DEFAULT_MAGIC: &str = "/usr/share/file/magic.mgc";
const UNKNOWN: &str = "unknown";
const TEXT_PLAIN: &str = "text/plain";
const MSOFFCRYPTO_SRC: &std::ffi::CStr = c_str!(r#"
import msoffcrypto
from msoffcrypto.exceptions import FileFormatError
file_type = None
try:
    msoffcrypto_obj = msoffcrypto.OfficeFile(open(path, "rb"))
    if msoffcrypto_obj and msoffcrypto_obj.is_encrypted():
        file_type = "document/office/passwordprotected"
except FileFormatError:
    pass
"#);


/// An incomplete version of the File object stored in the datastore produced by identify.
/// Non-observed information like classification/viewcount etc are not here.
#[derive(Debug, Serialize)]
pub struct FileIdentity {
    /// Dotted ASCII representation of the first 64 bytes of the file
    pub ascii: String,
    /// Entropy of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entropy: Option<f32>,
    /// Hex dump of the first 64 bytes of the file
    pub hex: String,
    /// MD5 of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<MD5>,
    /// Output from libmagic related to the file
    pub magic: String,
    /// MIME type of the file as identified by libmagic
    pub mime: Option<String>,
    /// SHA1 hash of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha1: Option<Sha1>,
    /// SHA256 hash of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<Sha256>,
    /// Size of the file in bytes
    pub size: u64,
    /// SSDEEP hash of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssdeep: Option<SSDeepHash>,
    /// Type of file as identified by Assemblyline
    #[serde(rename = "type")]
    pub file_type: String,
    /// TLSH hash of the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tlsh: Option<String>,
    /// URI structure to speed up specialty file searching
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri_info: Option<URIInfo>,
}


// # These headers are found in the custom.magic file to assist with identification, and are imported by services
// # that can create files with a high-confidence type
// CUSTOM_PS1_ID = b"#!/usr/bin/env pwsh\n"
// CUSTOM_BATCH_ID = b"REM Batch extracted by Assemblyline\n"
// CUSTOM_URI_ID = "# Assemblyline URI file\n"
const YARA_DEFAULT_EXTERNALS: [(&str, &str); 3] = [("mime", ""), ("magic", ""), ("type", "")];

/// A wrapper around the underlying libmagic resources.
struct Magic(pub magic::Cookie<magic::cookie::Load>);

/// We are moving raw c pointers around, this line indicates we know it is an unsafe operation
/// but want the compiler to do it anyway. This should be safe given they are behind mutexes
/// and AFAIK the _kind_ of thread unsafeness libmagic has can be handled with that.
unsafe impl Send for Magic {}

pub struct Identify {
    file_type: Mutex<Magic>,
    mime_type: Mutex<Magic>,
    yara_rules: Mutex<yara::Rules>,
    compiled_magic_patterns: Mutex<Arc<Vec<(String, regex::Regex)>>>,
    trusted_mimes: Mutex<Arc<HashMap<String, String>>>,
    cache: Option<CacheStore>,
    // custom: regex::Regex,
    pdf_encrypted: regex::bytes::Regex,
    pdf_portfolio: regex::bytes::Regex,
}

impl Identify {
    pub async fn new_with_cache(cache: CacheStore, redis_volatile: Arc<RedisObjects>) -> Result<Arc<Self>> {
        Self::new(Some(cache), Some(redis_volatile)).await
    }
    
    pub async fn new_without_cache() -> Result<Arc<Self>> {
        Self::new(None, None).await
    }

    async fn new(cache: Option<CacheStore>, redis_volatile: Option<Arc<RedisObjects>>) -> Result<Arc<Self>> {
        // Load all data for the first time
        let (file_type, mime_type) = Self::_load_magic_file(&cache).await.context("_load_bagic_file")?;
        let yara_rules = Self::_load_yara_file(&cache).await.context("_load_yara_file")?;
        let compiled_magic_patterns = Self::_load_magic_patterns(&cache).await.context("_load_magic_patterns")?;
        let trusted_mimes = Self::_load_trusted_mimes(&cache).await.context("_load_trusted_mimes")?;

        let obj = Arc::new(Self {
            file_type: Mutex::new(file_type),
            mime_type: Mutex::new(mime_type),
            yara_rules: Mutex::new(yara_rules),
            compiled_magic_patterns: Mutex::new(compiled_magic_patterns),
            trusted_mimes: Mutex::new(trusted_mimes),
            // custom: regex::RegexBuilder::new("^custom: ").ignore_whitespace(true).build()?,
            pdf_encrypted: regex::bytes::Regex::new("/Encrypt")?,
            pdf_portfolio: regex::bytes::Regex::new("/Type/Catalog/Collection")?,
            cache
        });

        // If cache is use, load the config and datastore objects to load potential items from cache
        if obj.cache.is_some() {
            if let Some(redis_volatile) = redis_volatile {
                info!("Using cache with identify");
                let messages = redis_volatile.subscribe("system.identify".to_owned());
                let obj = obj.clone();
                tokio::spawn(async move {
                    obj.watch_reloads(messages).await
                });
            }
        }

        Ok(obj)
    }

    async fn watch_reloads(self: Arc<Self>, mut messages: mpsc::Receiver<Option<redis_objects::Msg>>) {
        while let Some(msg) = messages.recv().await {
            debug!("Reloading identify.");

            // check if we are the only one still holding this reference
            if Arc::strong_count(&self) <= 1 {
                return
            }

            if let Err(err) = self.handle_reload_event(msg).await {
                error!("Error reloading identify configuration: {err}");
            }
        }
    }

    async fn handle_reload_event(&self, msg: Option<redis_objects::Msg>) -> Result<()> {
        let mut processed = false;
        if let Some(message) = msg {
            if let Ok(change) = message.get_payload::<String>() {
                processed = true;
                // update information
                match change.as_str() {
                    "magic" => self.load_magic_file().await?,
                    "mimes" => self.load_trusted_mimes().await?,
                    "patterns" => self.load_magic_patterns().await?,
                    "yara" => self.load_yara_file().await?,
                    _ => {
                        error!("Invalid system.identify message received: {change}");
                        processed = false;
                    },
                }
            }
        }

        if !processed {
            // handle disconnect event, we may be out of sync
            self.load_magic_file().await?;
            self.load_trusted_mimes().await?;
            self.load_magic_patterns().await?;
            self.load_yara_file().await?;
        }
        Ok(())
    }

    async fn _load_magic_patterns(cache: &Option<CacheStore>) -> Result<Arc<Vec<(String, regex::Regex)>>> {
        let mut magic_patterns: Vec<(Cow<str>, Cow<str>)> = default_magic_patterns.iter().map(|(a, b)|((*a).into(), (*b).into())).collect();

        if let Some(cache) = cache {
            info!("Checking for custom magic patterns...");
            match cache.get("custom_patterns").await {
                Ok(Some(patterns)) => {
                    magic_patterns = serde_yaml::from_slice(&patterns)?;
                    info!("Custom magic patterns loaded!");
                },
                _ => {
                    info!("No custom magic patterns found.");
                }
            }
        }

        let mut compiled_patterns = vec![];
        for (al_type, pattern) in magic_patterns {
            match regex::RegexBuilder::new(&pattern).case_insensitive(true).build() {
                Ok(re) => compiled_patterns.push((al_type.into_owned(), re)),
                Err(err) => error!("Could not process regex for {al_type} [{pattern}] [{err}]"),
            }
        }
        Ok(Arc::new(compiled_patterns))
    }

    async fn load_magic_patterns(&self) -> Result<()> {
        *self.compiled_magic_patterns.lock() = Self::_load_magic_patterns(&self.cache).await?;
        Ok(())
    }
    
    async fn _load_trusted_mimes(cache: &Option<CacheStore>) -> Result<Arc<HashMap<String, String>>> {
        if let Some(cache) = cache {
            info!("Checking for custom trusted mimes...");
            match cache.get("custom_mimes").await {
                Ok(Some(mimes)) => {
                    let trusted_mimes = serde_yaml::from_slice(&mimes)?;
                    info!("Custom trusted mimes loaded!");
                    return Ok(Arc::new(trusted_mimes))
                },
                _ => {
                    info!("No custom magic patterns found.");
                }
            }
        }

        Ok(Arc::new(TRUSTED_MIMES.iter().map(|(a, b)|(a.to_string(), b.to_string())).collect()))
    }
                    
    async fn load_trusted_mimes(&self) -> Result<()> {
        *self.trusted_mimes.lock() = Self::_load_trusted_mimes(&self.cache).await?;
        Ok(())
    }

    async fn _load_magic_file(cache: &Option<CacheStore>) -> Result<(Magic, Magic)> {
        // make sure the default magic file is available
        let magic_default = std::include_bytes!("./default.magic");
        if magic_default.is_empty() {
            return Err(anyhow::anyhow!("Identify magic rules default didn't pack."))
        }

        let magic_file = tempfile::NamedTempFile::new()?;
        tokio::fs::write(magic_file.path(), magic_default).await?;
        let system_default: PathBuf = SYSTEM_DEFAULT_MAGIC.parse()?;

        // setup our magic paths
        let databases: magic::cookie::DatabasePaths = [
            magic_file.path(),
            &system_default,
        ].try_into()?;

        if let Some(cache) = cache {
            info!("Checking for custom magic file...");
            // with get_cachestore("system", config=self.config, datastore=self.datastore) as cache:
            // custom_magic = "/tmp/custom.magic"
            let result = cache.download("custom_magic", magic_file.path()).await;
            // magic_file = ":".join((custom_magic, "/usr/share/file/magic.mgc"))
            match result {
                Ok(_) => info!("Custom magic file loaded!"),
                Err(_) => info!("No custom magic file found.")
            }
        }

        use magic::cookie::Flags;
        let file_type = magic::Cookie::open(Flags::CONTINUE | Flags::RAW)?;
        let file_type = file_type.load(&databases).map_err(|err| anyhow::anyhow!("couldn't load magic config: {err}"))?;
        let mime_type = magic::Cookie::open(Flags::CONTINUE | Flags::RAW | Flags::MIME)?;
        let mime_type = mime_type.load(&databases).map_err(|err| anyhow::anyhow!("couldn't load magic config: {err}"))?;
        Ok((Magic(file_type), Magic(mime_type)))
    }

    async fn load_magic_file(&self) -> Result<()> {
        let (file_type, mime_type) = Self::_load_magic_file(&self.cache).await?;    
        *self.file_type.lock() = file_type;
        *self.mime_type.lock() = mime_type;
        Ok(())
    }

    async fn _load_yara_file(cache: &Option<CacheStore>) -> Result<yara::Rules> {
        // make sure the default yara file is available
        let yara_default = std::include_str!("./default.yara");
        if yara_default.is_empty() {
            return Err(anyhow::anyhow!("Identify yara rules default didn't pack."))
        }

        // try to load from cache
        let mut apply = None;
        if let Some(cache) = cache {
            info!("Checking for custom yara file...");
            let custom_file = tempfile::NamedTempFile::new()?;
            let result = cache.download("custom_yara", custom_file.path()).await;
            match result {
                Ok(_) => {
                    info!("Custom yara file loaded!");
                    apply = Some(custom_file);
                },
                Err(_) => info!("No custom yara file found."),
            }
        }

        // set up the compiler
        let mut compiler = yara::Compiler::new()?;
        for (var, value) in YARA_DEFAULT_EXTERNALS {
            compiler.define_variable(var, value)?;
        }

        // if we didn't load from cache apply the default
        match apply {
            None => {
                compiler = compiler.add_rules_str(yara_default)?;
            },
            Some(custom_file) => {
                compiler = compiler.add_rules_file(custom_file.path())?;
            }
        }

        // yara_rules = yara.compile(filepaths={"default": self.yara_file}, externals=self.yara_default_externals)
        let rules = compiler.compile_rules()?;
        debug!("Loaded {} yara rules", rules.get_rules().len());
        Ok(rules)
    }

    async fn load_yara_file(&self) -> Result<()> {
        let rules = match Self::_load_yara_file(&self.cache).await {
            Ok(rules) => rules,
            Err(error) => return Err(anyhow::anyhow!("Could not load yara rules: {error}")),
        };
        *self.yara_rules.lock() = rules;
        Ok(())
    }

//     def __enter__(self):
//         return self

//     def __exit__(self, *_):
//         self.stop()

//     def stop(self):
//         if self.reload_watcher:
//             self.reload_watcher.stop()

    pub fn ident_blocking(&self, buf: &[u8], path: &Path, digests: Option<Digests>) -> Result<FileIdentity> {
        // data = {"ascii": None, "hex": None, "magic": None, "mime": None, "type": UNKNOWN}
        let mut file_type = UNKNOWN.to_owned();
        let mut magic = String::new();
        let mut mime = None;

        if buf.is_empty() {
            return Ok(FileIdentity { 
                ascii: "".to_string(), entropy: None, hex: "".to_string(), md5: None, magic: "".to_string(), 
                mime: None, sha1: None, sha256: None, size: 0, ssdeep: None, file_type: UNKNOWN.to_string(), tlsh: None, uri_info: None 
            })
        }

        let header = &buf[0..64.min(buf.len())];
        let ascii = dotdump_bytes(header);
        
        // Loop over the labels returned by libmagic, ...
        let mut labels: Vec<String> = match self.file_type.lock().0.file(path) {
            Ok(output) => {
                output.split('\n').map(|row| row.strip_prefix("- ").unwrap_or(row)).map(String::from).collect()
            },
            Err(err) => {
                error!("Magic error: {err}");
                vec![]
            }
        };

        let mut mimes: Vec<String> = match self.mime_type.lock().0.file(path) {
            Ok(output) => output.split('\n').map(|row| row.strip_prefix("- ").unwrap_or(row)).map(String::from).collect(),
            Err(err) => {
                error!("Magic error: {err}");
                vec![]
            }
        };

        // for line in labels.iter_mut() {
        //     if let Some(content) = line.strip_prefix("- ") {
        //         *line = content.to_string();
        //     }
        // }

        // For user feedback set the mime and magic meta data to always be the primary
        // libmagic responses
        if !labels.is_empty() {

            fn find_special_words(word: &str, labels: &[String]) -> Option<usize> {
                for (index, label) in labels.iter().enumerate() {
                    if label.contains(word) {
                        return Some(index)
                    }
                }
                None
            }

            // If an expected label is not the first label returned by Magic, then make it so
            // Manipulating the mime accordingly varies between special word cases
            let special_word_cases = [
                ("OLE 2 Compound Document : Microsoft Word Document", false),
                ("Lotus 1-2-3 WorKsheet", true),
            ];
            for (word, alter_mime) in special_word_cases {
                if let Some(index) = find_special_words(word, &labels) {
                    let moved_item = labels.remove(index);
                    labels.insert(0, moved_item);
                    if labels.len() == mimes.len() && alter_mime {
                        let moved_item = mimes.remove(index);
                        mimes.insert(0, moved_item);
                    }
                }
            }
            if let Some(label) = labels.first() {
                magic = label.clone();
            }
        }

        for possible_mime in &mimes {
            if possible_mime.is_empty() { continue }
            mime = Some(possible_mime.clone());
            break
        }

        // First lets try to find any custom types
        for label in &labels {
            let label = dotdump(label);
            if let Some(item) = label.strip_prefix("custom: ") {
                // Some things, like executable have additional data appended to their identification, like
                // ", dynamically linked, stripped" that we do not want to use as part of the type.
                if let Some((front, _)) = item.split_once(",") {
                    file_type = front.trim().to_owned();
                } else {
                    file_type = item.trim().to_owned();
                }
                break
            }
        }

        // Second priority is mime times marked as trusted
        if file_type == UNKNOWN {
            let trusted_mimes = self.trusted_mimes.lock().clone();

            for mime in &mimes {
                let mime = dotdump(mime);

                if let Some(new_type) = trusted_mimes.get(&mime) {
                    file_type = new_type.to_owned();
                    break
                }

                if let Some((mime, _)) = mime.split_once(";") {
                    if let Some(new_type) = trusted_mimes.get(mime) {
                        file_type = new_type.to_owned();
                        break
                    }    
                }
            }
        }

        // As a third priority try matching the magic_patterns
        if file_type == UNKNOWN {
            let compiled_magic_patterns = self.compiled_magic_patterns.lock().clone();

            'labels: for label in labels {
                let label = dotdump(&label);
                for (new_type, pattern) in compiled_magic_patterns.iter() {
                    if pattern.is_match(&label) {
                        file_type = new_type.to_string();
                        break 'labels
                    }
                }
            }
        }

        // except Exception as e:
        //     self.log.error(f"An error occured during file identification: {e.__class__.__name__}({str(e)})")
        //     pass

        // If mime is text/* and type is unknown, set text/plain to trigger
        // language detection later.
        if let Some(mime) = &mime {
            if file_type == UNKNOWN && mime.starts_with("text/") {
                file_type = TEXT_PLAIN.to_string();
            }
        }

        // Lookup office documents by GUID if we're still not sure what they are
        if file_type == "document/office/unknown" {
            // following byte sequence equivalent to "Root Entry".encode("utf-16-le") in python
            let root_entry_lit: [u8; 20] = [82, 0, 111, 0, 111, 0, 116, 0, 32, 0, 69, 0, 110, 0, 116, 0, 114, 0, 121, 0];
            if let Some(root_entry_property_offset) = find_subsequence(buf, &root_entry_lit) {
                // Get root entry's GUID and try to guess document type
                let clsid_offset = root_entry_property_offset + 0x50;
                if buf.len() >= clsid_offset + 16 {
                    let clsid: [u8; 16] = buf[clsid_offset .. clsid_offset + 16].try_into()?;
                    if clsid != vec![0; clsid.len()].as_slice() { // b"\0" * clsid.len()
                        let clsid = uuid::Uuid::from_bytes_le(clsid);
                        // clsid_str = clsid_str.urn.rsplit(":", 1)[-1].upper();
                        if let Some(value) = ole_clsid_guids().get(&clsid) {
                            file_type = value.to_string();
                        }
                    } else {
                        // byte sequence matching "Details".encode("utf-16-le") in python
                        let details: [u8; 14] = [68, 0, 101, 0, 116, 0, 97, 0, 105, 0, 108, 0, 115, 0];
                        let bup_details_offset = find_subsequence(&buf[.. root_entry_property_offset + 0x100], &details);
                        if bup_details_offset.is_some() {
                            file_type = "quarantine/mcafee".to_string();
                        }
                    }
                }
            }
        }

        let (md5, sha1, sha256, size, ssdeep, tlsh, entropy) = if let Some(digests) = digests {
            let Digests { md5, sha1, sha256, ssdeep, tlsh, size, entropy, .. } = digests;
            let ssdeep = match ssdeep {
                Some(hash) => Some(hash.parse()?),
                None => None
            };
            (Some(md5.parse()?), Some(sha1.parse()?), Some(sha256.parse()?), size, ssdeep, tlsh, entropy)
        } else {
            (None, None, None, 0, None, None, None)
        };

        Ok(FileIdentity {
            magic,
            md5,
            mime,
            sha1,
            sha256,
            size,
            ssdeep,
            tlsh,
            file_type,
            uri_info: None,
            ascii,
            entropy: entropy.map(|value| value as f32),
            hex: hex::encode(header),
        })
    }

    fn yara_ident(&self, path: &Path, info: &FileIdentity) -> Result<Option<String>> {
        // externals = {k: v or "" for k, v in info.items() if k in self.yara_default_externals}
        // set up the parameters for the yara scan
        let yara = self.yara_rules.lock();
        let mut scanner = yara.scanner()?;
        scanner.set_flags(yara::ScanFlags::FAST_MODE);
        scanner.define_variable("mime", info.mime.as_deref().unwrap_or(""))?;
        scanner.define_variable("magic", info.magic.as_str())?;
        scanner.define_variable("type", info.file_type.as_str())?;
        scanner.set_timeout(60);

        // matches = yara_rules.match(path, externals=externals, fast=True)
        let mut scan_matches = match scanner.scan_file(path) {
            Ok(scan_matches) => scan_matches,
            Err(err) => {
                warn!("Yara file identifier failed with error: {err}");
                return Ok(None)
            }
        };

        debug!("yara matches: {:?}", scan_matches.iter().map(|rule| rule.identifier).collect_vec());

        // matches.sort(key=lambda x: x.meta.get("score", 0), reverse=True)
        scan_matches.sort_by_key(|rule| {
            for meta in &rule.metadatas {
                if meta.identifier == "score" {
                    if let yara::MetadataValue::Integer(score) = meta.value {
                        return score
                    }
                }
            }
            return 0
        });

        // in python this was reversed and the list was traversed start-end
        // in this code it isn't reversed and we are going end-start
        while let Some(rule) = scan_matches.pop() {
            for meta in &rule.metadatas {
                if meta.identifier == "type" {
                    if let yara::MetadataValue::String(file_type) = meta.value {
                        return Ok(Some(file_type.to_owned()))
                    }
                }
            }
        }

        return Ok(None)
    }

    pub async fn fileinfo(self: &Arc<Self>, path: PathBuf, generate_hashes: impl IBool, skip_fuzzy_hashes: impl IBool, calculate_entropy: impl IBool) -> Result<FileIdentity> {
        let generate_hashes = generate_hashes.into().unwrap_or(true);
        let skip_fuzzy_hashes = skip_fuzzy_hashes.into().unwrap_or(false);
        let calculate_entropy = calculate_entropy.into().unwrap_or(true);
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            debug!("fileinfo {path:?}; generate_hashes {generate_hashes}; skip_fuzzy_hashes: {skip_fuzzy_hashes}; calculate_entropy: {calculate_entropy}");

            // let path = safe_str(path);
            let mut data: FileIdentity = if generate_hashes {
                let mut digests = get_digests_for_file_blocking(&path, None, calculate_entropy, skip_fuzzy_hashes)?;
                let mut first_block = vec![];
                std::mem::swap(&mut first_block, &mut digests.first_block);
                this.ident_blocking(&first_block, &path, Some(digests))?
            } else {
                let mut file = std::fs::File::open(&path)?;
                let size = file.metadata()?.len();
                let to_read = size.min(DEFAULT_BLOCKSIZE as u64);
                let mut first_block = vec![0u8; to_read as usize];
                file.read_exact(&mut first_block)?;
                // with open(path, "rb") as f:
                //     first_block = f.read(DEFAULT_BLOCKSIZE)
                let mut data = this.ident_blocking(&first_block, &path, None)?;
                data.size = size;
                data
            };

            debug!("mime: {:?}", data.mime);
            debug!("magic: {}", data.magic);

            // Check if file empty
            if data.size == 0 {
                data.file_type = "empty".to_string();

            // Futher identify zip files based of their content
            } else if ["archive/zip", "java/jar", "document/office/unknown"].contains(&data.file_type.as_str()) {
                data.file_type = zip_ident(&path, data.file_type)?;

            // Further check CaRT files, they may have an explicit type set
            } else if data.file_type == "archive/cart" {
                data.file_type = cart_ident(&path)?;

            // Further identify dos executables has this may be a PE that has been misidentified
            } else if data.file_type == "executable/windows/dos" {
                data.file_type = dos_ident(&path)?;

            // If we identified the file as 'uri' from libmagic, we should further identify it, or return it as text/plain
            } else if data.file_type == "uri" {
                data.file_type = uri_ident(&path, &mut data)?;

            // If we've so far failed to identified the file, lets run the yara rules
            } else if data.file_type.contains(UNKNOWN) || data.file_type == TEXT_PLAIN {
                let mime = data.mime.clone().unwrap_or_default();
                // We do not trust magic/mimetype's CSV identification, so we test it first
                if data.magic == "CSV text" || ["text/csv", "application/csv"].contains(&mime.as_str()) {
                    error!("csv testing not implemented");
                    // with open(path, newline='') as csvfile:
                    //     try:
                    //         # Try to read the file as a normal csv without special sniffed dialect
                    //         complete_data = [x for x in islice(csv.reader(csvfile), 100)]
                    //         if len(complete_data) > 2 and len(set([len(x) for x in complete_data])) == 1:
                    //             data["type"] = "text/csv"
                    //             # Final type identified, shortcut further processing
                    //             return data
                    //     except Exception:
                    //         pass
                    //     csvfile.seek(0)
                    //     try:
                    //         # Normal CSV didn't work, try sniffing the csv to see how we could parse it
                    //         dialect = csv.Sniffer().sniff(csvfile.read(1024))
                    //         csvfile.seek(0)
                    //         complete_data = [x for x in islice(csv.reader(csvfile, dialect), 100)]
                    //         if len(complete_data) > 2 and len(set([len(x) for x in complete_data])) == 1:
                    //             data["type"] = "text/csv"
                    //             # Final type identified, shortcut further processing
                    //             return data
                    //     except Exception:
                    //         pass
                }
    
                if data.file_type == TEXT_PLAIN {
                    // Check if the file is a misidentified json first before running the yara rules
                    let body = std::fs::OpenOptions::new().read(true).open(&path)?;
                    if serde_json::from_reader::<_, serde_json::Value>(&body).is_ok() {
                        data.file_type = "text/json".to_string();
                        // Final type identified, shortcut further processing
                        return Ok(data)
                    } 
                }
    
                // Only if the file was not identified as a csv or a json
                if let Some(new_type) = this.yara_ident(&path, &data)? {
                    data.file_type = new_type;
                }
    
                if data.file_type.contains(UNKNOWN) || data.file_type == TEXT_PLAIN {
                    if let Some(new_type) = untrusted_mimes(&mime) {
                        // Rely on untrusted mimes
                        data.file_type = new_type.to_string();
                    }
                }
            }

            // Extra checks for office documents
            //  - Check for encryption
            if [
                "document/office/word",
                "document/office/excel",
                "document/office/powerpoint",
                "document/office/unknown",
            ].contains(&data.file_type.as_str()) {
                let output = pyo3::Python::with_gil(|py| {
                    let locals = [("path", &path)].into_py_dict(py)?;
                    py.run(MSOFFCRYPTO_SRC, None, Some(&locals))?;
                    let value = locals.get_item(intern!(py, "file_type"))?;       
                    value.extract::<Option<String>>()
                });

                match output {
                    Ok(Some(new_type)) => {
                        data.file_type = new_type;
                    },
                    Ok(None) => {},
                    Err(error) => {
                        warn!("Could not process file in msoffcrypto: {error}");
                    },
                }
                // try:
                //     msoffcrypto_obj = msoffcrypto.OfficeFile(open(path, "rb"))
                //     if msoffcrypto_obj and msoffcrypto_obj.is_encrypted():
                //         data.file_type = "document/office/passwordprotected"
                // except Exception:
                //     # If msoffcrypto can't handle the file to confirm that it is/isn't password protected,
                //     # then it's not meant to be. Moving on!
                //     pass
            }

            // Extra checks for PDF documents
            //  - Check for encryption
            //  - Check for PDF collection (portfolio)
            if data.file_type == "document/pdf" {
                // Password protected documents typically contain '/Encrypt'
                let pdf_content = std::fs::read(&path)?;
                if this.pdf_encrypted.find(&pdf_content).is_some() {
                    data.file_type = "document/pdf/passwordprotected".to_string();
                // Portfolios typically contain '/Type/Catalog/Collection
                } else if this.pdf_portfolio.find(&pdf_content).is_some() {
                    data.file_type = "document/pdf/portfolio".to_string();
                }
            }

            return Ok(data)
        }).await?
    }
}


fn zip_ident(path: &Path, fallback: String) -> Result<String> {
    let file_list: Vec<String> = {
        match zip::ZipArchive::new(std::fs::File::open(path)?) {
            Ok(zip_file) => zip_file.file_names().map(str::to_string).collect(),
            Err(_) => return Ok(fallback)
        }
    };

    // try:
    //     with zipfile.ZipFile(path, "r") as zf:
    //         file_list = [zfname for zfname in zf.namelist()]
    // except Exception:
    //     try:
    //         stdout, _ = subprocess.Popen(
    //             ["unzip", "-l", path], stderr=subprocess.PIPE, stdout=subprocess.PIPE
    //         ).communicate()
    //         lines = stdout.splitlines()
    //         index = lines[1].index(b"Name")
    //         for file_name in lines[3:-2]:
    //             file_list.append(safe_str(file_name[index:]))
    //     except Exception:
    //         return fallback

    let tot_files = file_list.len();
    let mut tot_class = 0;
    let mut tot_jar = 0;

    let mut is_ipa = false;
    let mut is_jar = false;
    let mut is_word = false;
    let mut is_excel = false;
    let mut is_ppt = false;
    let mut doc_props = false;
    let mut doc_rels = false;
    let mut doc_types = false;
    let mut android_manifest = false;
    let mut android_dex = false;
    let mut nuspec = false;
    let mut psmdcp = false;

    for file_name in file_list {
        // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_jar.yara#L11
        if file_name.starts_with("META-INF/") {
            is_jar = true;
        } else if file_name == "AndroidManifest.xml" {
            android_manifest = true;
        } else if file_name == "classes.dex" {
            android_dex = true;
        } else if file_name.starts_with("Payload/") && file_name.ends_with(".app/Info.plist") {
            is_ipa = true;
        } else if file_name.ends_with(".nuspec") {
            nuspec = true;
        } else if file_name.starts_with("package/services/metadata/core-properties/") && file_name.ends_with(".psmdcp") {
            psmdcp = true;
        } else if file_name.ends_with(".class") {
            tot_class += 1
        } else if file_name.ends_with(".jar") {
            tot_jar += 1
        } else if file_name.starts_with("word/") {
            is_word = true;
        } else if file_name.starts_with("xl/") {
            is_excel = true;
        } else if file_name.starts_with("ppt/") {
            is_ppt = true;
        } else if file_name.starts_with("docProps/") {
            doc_props = true;
        } else if file_name.starts_with("_rels/") {
            doc_rels = true;
        // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_office_open_xml.yara
        } else if file_name == "[Content_Types].xml" {
            doc_types = true;
        }
    }

    if 0 < tot_files && tot_files < (tot_class + tot_jar) * 2 {
        is_jar = true;
    }

    if is_jar && android_manifest && android_dex {
        return Ok("android/apk".to_string())
    } else if is_ipa {
        return Ok("ios/ipa".to_string())
    } else if is_jar {
        return Ok("java/jar".to_string())
    } else if (doc_props || doc_rels) && doc_types {
        if is_word {
            return Ok("document/office/word".to_string())
        } else if is_excel {
            return Ok("document/office/excel".to_string())
        } else if is_ppt {
            return Ok("document/office/powerpoint".to_string())
        } else if nuspec && psmdcp {
            // It is a nupkg file. Identify as archive/zip for now.
            return Ok("archive/zip".to_string())
        } else {
            return Ok("document/office/unknown".to_string())
        }
    } else {
        return Ok("archive/zip".to_string())
    }
}

fn cart_ident(path: &Path) -> Result<String> {
    match unpack_header(std::fs::File::open(path)?, None) {
        Ok((_, metadata, _)) => {
            if let Some(metadata) = metadata {
                if let Some(section) = metadata.get("al") {
                    if let Some(section) = section.as_object() {
                        if let Some(file_type) = section.get("type") {
                            if let Some(file_type) = file_type.as_str() {
                                return Ok(file_type.to_string())
                            }
                        }
                    }
                }
            } 
            Ok("archive/cart".to_string())
        }
        Err(_) => Ok("corrupted/cart".to_string())
    }
}


fn dos_ident(path: &Path) -> Result<String> {
    match _dos_ident(path) {
        Ok(Some(label)) => return Ok(label),
        Err(err) if err.kind() != std::io::ErrorKind::UnexpectedEof => return Err(err.into()),
        _ => {}
    };
    Ok("executable/windows/dos".to_string())
}


fn _dos_ident(path: &Path) -> Result<Option<String>, std::io::Error> {
    let mut fh = std::io::BufReader::new(std::fs::File::open(path)?);
    
    // file_header = fh.read(0x40)
    let mut file_header = vec![0u8; 0x40];
    fh.read_exact(&mut file_header)?;
    if &file_header[0..2] != b"MZ" {
        return Ok(None)
    }
    
    let header_pos_array: [u8; 4] = file_header[file_header.len() - 4 .. ].try_into().unwrap();
    let header_pos = u32::from_le_bytes(header_pos_array);
    fh.seek(SeekFrom::Start(header_pos as u64))?;
    let mut sign_buffer = vec![0u8; 4];
    fh.read_exact(&mut sign_buffer)?;
    if sign_buffer != b"PE\x00\x00" {
        return Ok(None);
    }

    // (machine_id,) = struct.unpack("<H", fh.read(2))
    let machine_id = fh.read_u16_le()?;
    let width = if machine_id == 0x014C {
        32
    } else if machine_id == 0x8664 {
        64
    } else {
        return Ok(None)
    };

    // (characteristics,) = struct.unpack("<H", fh.read(18)[-2:])
    fh.seek(SeekFrom::Current(16))?;
    let characteristics = fh.read_u16_le()?;
    let pe_type = if (characteristics & 0x2000) != 0 {
        "dll"
    } else if (characteristics & 0x0002) != 0 {
        "pe"
    } else {
        return Ok(None)
    };
    return Ok(Some(format!("executable/windows/{pe_type}{width}")))
}

fn uri_ident(path: &Path, info: &mut FileIdentity) -> Result<String> {
    let file = std::fs::File::open(path)?;

    let data: serde_yaml::Mapping = match serde_yaml::from_reader(file) {
        Ok(data) => data,
        Err(_) => return Ok(TEXT_PLAIN.to_string())
    };

    let uri_data = match data.get("uri") {
        Some(field) => match field.as_str() {
            Some(field) => field,
            None => return Ok(TEXT_PLAIN.to_string())
        },
        None => return Ok(TEXT_PLAIN.to_string())
    };

    let url: url::Url = match uri_data.parse() {
        Ok(url) => url,
        Err(_) => return Ok(TEXT_PLAIN.to_string()),
    };

    let scheme = url.scheme();
    if scheme.is_empty() {
        return Ok(TEXT_PLAIN.to_string())
    }

    info.uri_info = Some(URIInfo{
        uri: uri_data.to_string(),
        scheme: url.scheme().to_string(),
        netloc: url.authority().to_string(),
        path: if url.path().is_empty() { None } else { Some(url.path().to_string()) },
        params: None,
        query: url.query().map(str::to_owned),
        fragment: url.fragment().map(str::to_owned),
        username: if url.username().is_empty() { None } else { Some(url.username().to_string()) },
        password: url.password().map(str::to_owned),
        hostname: url.host_str().unwrap_or_default().to_owned(),
        port: url.port(),
    });
    // info["uri_info"] = dict(
    //     uri=data["uri"],
    //     scheme=u.scheme,
    //     netloc=u.netloc,
    // )
    // if u.path:
    //     info["uri_info"]["path"] = u.path
    // if u.params:
    //     info["uri_info"]["params"] = u.params
    // if u.query:
    //     info["uri_info"]["query"] = u.query
    // if u.fragment:
    //     info["uri_info"]["fragment"] = u.fragment
    // if u.username:
    //     info["uri_info"]["username"] = unquote(u.username)
    // if u.password:
    //     info["uri_info"]["password"] = unquote(u.password)
    // info["uri_info"]["hostname"] = u.hostname
    // if u.port:
    //     info["uri_info"]["port"] = u.port

    Ok(format!("uri/{scheme}"))
}


// if __name__ == "__main__":
//     from pprint import pprint

//     use_cache = True
//     uncart = False
//     args = sys.argv[1:]
//     if "--no-cache" in args:
//         args.remove("--no-cache")
//         use_cache = False
//     if "--uncart" in args:
//         args.remove("--uncart")
//         uncart = True

//     identify = Identify(use_cache=use_cache)

//     if len(args) > 0:
//         fileinfo_data = identify.fileinfo(args[0])

//         if fileinfo_data["type"] == "archive/cart" and uncart:
//             with NamedTemporaryFile("w") as f:
//                 unpack_file(args[0], f.name)
//                 fileinfo_data = identify.fileinfo(f.name)

//         pprint(fileinfo_data)
//     else:
//         name = sys.stdin.readline().strip()
//         while name:
//             a = identify.fileinfo(name, skip_fuzzy_hashes=True)
//             print(
//                 "\t".join(
//                     dotdump(str(a[k]))
//                     for k in (
//                         "type",
//                         "ascii",
//                         "entropy",
//                         "hex",
//                         "magic",
//                         "mime",
//                         "md5",
//                         "sha1",
//                         "sha256",
//                         "ssdeep",
//                         "size",
//                     )
//                 )
//             )
//             name = sys.stdin.readline().strip()

