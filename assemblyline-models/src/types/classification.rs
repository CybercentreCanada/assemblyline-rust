use std::ops::Deref;
use std::sync::{Arc, Mutex};

use serde::{Serialize, Deserialize};
use assemblyline_markings::classification::{ClassificationParser, NormalizeOptions};
use struct_metadata::Described;

use crate::types::JsonMap;
use crate::{ElasticMeta, ModelError};

enum ClassificationMode {
    Uninitialized,
    Disabled,
    Configured(Arc<ClassificationParser>)
}

static GLOBAL_CLASSIFICATION: Mutex<ClassificationMode> = Mutex::new(ClassificationMode::Uninitialized);

// #[cfg(feature = "local_classification")]
// tokio::task_local! {
//     static TASK_CLASSIFICATION: Option<Arc<ClassificationParser>>;
// }

pub fn disable_global_classification() {
    *GLOBAL_CLASSIFICATION.lock().unwrap() = ClassificationMode::Disabled;
}

pub fn set_global_classification(config: Arc<ClassificationParser>) {
    *GLOBAL_CLASSIFICATION.lock().unwrap() = ClassificationMode::Configured(config);
}

pub fn unrestricted_classification() -> String {
    match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
        ClassificationMode::Uninitialized => panic!("classification handling without defining parser"),
        ClassificationMode::Disabled => "".to_string(),
        ClassificationMode::Configured(parser) => parser.unrestricted().to_owned(),
    }
}

pub fn unrestricted_classification_string() -> ClassificationString {
    match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
        ClassificationMode::Uninitialized => panic!("classification handling without defining parser"),
        ClassificationMode::Disabled => ClassificationString(Default::default()),
        ClassificationMode::Configured(parser) => ClassificationString::unrestricted(parser),
    }
}

pub fn unrestricted_expanding_classification() -> ExpandingClassification {
    match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
        ClassificationMode::Uninitialized => panic!("classification handling without defining parser"),
        ClassificationMode::Disabled => ExpandingClassification {
            classification: Default::default(),
            __access_lvl__: Default::default(),
            __access_req__: Default::default(),
            __access_grp1__: Default::default(),
            __access_grp2__: Default::default(),
        },
        ClassificationMode::Configured(parser) => ExpandingClassification::unrestricted(parser),
    }
}


// #[cfg(feature = "local_classification")]
// pub async fn with_local_classification<F: std::future::Future>(config: Arc<ClassificationParser>, f: F)  {
//     TASK_CLASSIFICATION.scope(Some(config), f).await
// }

// MARK: ExpandingClassification

/// Expanding classification type
#[derive(Debug, Clone, Eq)]
pub struct ExpandingClassification<const USER: bool=false> { 
    pub classification: String,
    pub __access_lvl__: i32,
    pub __access_req__: Vec<String>,
    pub __access_grp1__: Vec<String>,
    pub __access_grp2__: Vec<String>,
}

impl<const USER: bool> ExpandingClassification<USER> {
    pub fn new(classification: String, parser: &ClassificationParser) -> Result<Self, ModelError> {
        if parser.original_definition.enforce {

            let parts = parser.get_classification_parts(&classification, false, true, !USER)?;
            let classification = parser.get_normalized_classification_text(parts.clone(), false, false)?;

            Ok(Self {
                classification,
                __access_lvl__: parts.level,
                __access_req__: parts.required,
                __access_grp1__: if parts.groups.is_empty() { vec!["__EMPTY__".to_owned()] } else { parts.groups },
                __access_grp2__: if parts.subgroups.is_empty() { vec!["__EMPTY__".to_owned()] } else { parts.subgroups },
            })
        } else {
            Ok(Self {
                classification,
                __access_lvl__: Default::default(),
                __access_req__: Default::default(),
                __access_grp1__: vec!["__EMPTY__".to_owned()],
                __access_grp2__: vec!["__EMPTY__".to_owned()],
            })
        }
    }

    pub fn try_unrestricted() -> Option<Self> {
        if let ClassificationMode::Configured(parser) = &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            Some(Self::unrestricted(parser))
        } else {
            None
        }   
    }

    pub fn unrestricted(parser: &ClassificationParser) -> Self {
        Self::new(parser.unrestricted().to_string(), parser).unwrap()
    }

    pub fn as_str(&self) -> &str {
        &self.classification
    }

    pub fn insert(parser: &ClassificationParser, output: &mut JsonMap, classification: &str) -> Result<(), ModelError> {
        use serde_json::json;
        if parser.original_definition.enforce {
            let parts = parser.get_classification_parts(classification, true, true, !USER)?;
            let classification = parser.get_normalized_classification_text(parts.clone(), true, false)?;

            output.insert("classification".to_string(), json!(classification));
            output.insert("__access_lvl__".to_string(), json!(parts.level));
            output.insert("__access_req__".to_string(), json!(parts.required));
            output.insert("__access_grp1__".to_string(), json!(if parts.groups.is_empty() { vec!["__EMPTY__".to_string()] } else { parts.groups }));
            output.insert("__access_grp2__".to_string(), json!(if parts.subgroups.is_empty() { vec!["__EMPTY__".to_string()] } else { parts.subgroups }));
            Ok(())
        } else {
            output.insert("classification".to_string(), json!(classification));
            output.insert("__access_lvl__".to_string(), json!(0));
            output.insert("__access_req__".to_string(), serde_json::Value::Array(Default::default()));
            output.insert("__access_grp1__".to_string(), json!(&["__EMPTY__"]));
            output.insert("__access_grp2__".to_string(), json!(&["__EMPTY__"]));
            Ok(())
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RawClassification { 
    pub classification: String,
    #[serde(default)]
    pub __access_lvl__: i32,
    #[serde(default)]
    pub __access_req__: Vec<String>,
    #[serde(default)]
    pub __access_grp1__: Vec<String>,
    #[serde(default)]
    pub __access_grp2__: Vec<String>,
}

impl<const U: bool> From<&ExpandingClassification<U>> for RawClassification {
    fn from(value: &ExpandingClassification<U>) -> Self {
        Self {
            classification: value.classification.clone(),
            __access_lvl__: value.__access_lvl__,
            __access_req__: value.__access_req__.clone(),
            __access_grp1__: value.__access_grp1__.clone(),
            __access_grp2__: value.__access_grp2__.clone(),
        }
    }
}

impl<const U: bool> From<RawClassification> for ExpandingClassification<U> {
    fn from(value: RawClassification) -> Self {
        Self {
            classification: value.classification.clone(),
            __access_lvl__: value.__access_lvl__,
            __access_req__: value.__access_req__.clone(),
            __access_grp1__: value.__access_grp1__.clone(),
            __access_grp2__: value.__access_grp2__.clone(),
        }
    }
}


impl<const U: bool> Serialize for ExpandingClassification<U> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer 
    {
        match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized => return Err(serde::ser::Error::custom("classification engine not initalized")),
            // if it is disabled just serialize as raw
            ClassificationMode::Disabled |
            // assume the content of the struct is already normalized on load or construction
            ClassificationMode::Configured(_) => {},
        }
        RawClassification::from(self).serialize(serializer)
    }
}

impl<'de, const U: bool> Deserialize<'de> for ExpandingClassification<U> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> 
    {
        let parser = match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized => return Err(serde::de::Error::custom("classification engine not initalized")),
            // if it is disabled just handle as raw
            ClassificationMode::Disabled => None,
            // normalize on load
            ClassificationMode::Configured(parser) => Some(parser.clone()),
        };

        if let Some(parser) = parser {
            let raw = RawClassification::deserialize(deserializer)?;
            Self::new(raw.classification, &parser).map_err(serde::de::Error::custom)
        } else {
            Ok(RawClassification::deserialize(deserializer)?.into())
        }        
    }
}

impl<const USER: bool> Described<ElasticMeta> for ExpandingClassification<USER> {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {

        // let group_meta = ElasticMeta {
        //     index: Some(true),
        //     store: None,
        //     ..Default::default()
        // };

        struct_metadata::Descriptor { 
            docs: None, 
            metadata: ElasticMeta{mapping: Some("classification"), ..Default::default()}, 
            kind: struct_metadata::Kind::new_struct("ExpandingClassification", vec![
                struct_metadata::Entry { label: "classification", docs: None, metadata: ElasticMeta{mapping: Some("classification"), ..Default::default()}, type_info: String::metadata(), has_default: false, aliases: &["classification"] },
                // struct_metadata::Entry { label: "__access_lvl__", docs: None, metadata: Default::default(), type_info: i32::metadata(), has_default: false, aliases: &["__access_lvl__"] },
                // struct_metadata::Entry { label: "__access_req__", docs: None, metadata: Default::default(), type_info: Vec::<String>::metadata(), has_default: false, aliases: &["__access_req__"] },
                // struct_metadata::Entry { label: "__access_grp1__", docs: None, metadata: Default::default(), type_info: Vec::<String>::metadata(), has_default: false, aliases: &["__access_grp1__"] },
                // struct_metadata::Entry { label: "__access_grp2__", docs: None, metadata: Default::default(), type_info: Vec::<String>::metadata(), has_default: false, aliases: &["__access_grp2__"] },
            ], &mut [], &mut[]),
            // kind: struct_metadata::Kind::Aliased { 
            //     name: "ExpandingClassification", 
            //     kind: Box::new(String::metadata())
            // },
        }
    }
}

impl<const U: bool> PartialEq<&str> for ExpandingClassification<U> {
    fn eq(&self, other: &&str) -> bool {
        let parser = match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized | ClassificationMode::Disabled => None,
            ClassificationMode::Configured(parser) => Some(parser.clone()),
        };

        if let Some(parser) = parser {
            match (parser.normalize_classification(&self.classification), parser.normalize_classification(other)) {
                (Ok(a), Ok(b)) => a == b,
                _ => false
            }
        } else {
            self.classification.as_str() == *other
        }
    }
}

impl<const U: bool> PartialEq for ExpandingClassification<U> {
    fn eq(&self, other: &Self) -> bool {
        self.eq(&other.classification.as_str())
    }
}


// MARK: ClassificationString

/// A classification value stored as a string
#[derive(Described, Debug, Clone, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="classification_string")]
pub struct ClassificationString(pub (crate) String);

impl From<ClassificationString> for String {
    fn from(value: ClassificationString) -> Self {
        value.0
    }
}

impl Deref for ClassificationString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ClassificationString {
    pub fn new(classification: String, parser: &Arc<ClassificationParser>) -> Result<Self, ModelError> {
        Ok(Self(parser.normalize_classification_options(&classification, NormalizeOptions::short())?))
    }

    pub fn new_unchecked(classification: String) -> Self {
        Self(classification)
    }

    pub fn try_unrestricted() -> Option<Self> {
        if let ClassificationMode::Configured(parser) = &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            Some(Self::unrestricted(parser))
        } else {
            None
        }   
    }
    
    pub fn unrestricted(parser: &ClassificationParser) -> Self {
        Self(parser.unrestricted().to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn default_unrestricted() -> ClassificationString {
        match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized => panic!("classification handling without defining parser"),
            ClassificationMode::Disabled => ClassificationString(Default::default()),
            ClassificationMode::Configured(parser) => ClassificationString::unrestricted(parser),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RawClassificationString(String);


impl From<&ClassificationString> for RawClassificationString {
    fn from(value: &ClassificationString) -> Self {
        Self(value.0.clone())
    }
}

impl From<RawClassificationString> for ClassificationString {
    fn from(value: RawClassificationString) -> Self {
        Self(value.0.clone())
    }
}


impl Serialize for ClassificationString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer 
    {
        match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized => return Err(serde::ser::Error::custom("classification engine not initalized")),
            // if it is disabled just serialize as raw
            ClassificationMode::Disabled |
            // assume the content of the struct is already normalized on load or construction
            ClassificationMode::Configured(_) => {},
        }
        RawClassificationString::from(self).serialize(serializer)
    }
}

// fn response_or_empty_string<'de, D>(d: D) -> Result<Option<Response>, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     #[derive(Deserialize)]
//     #[serde(untagged)]
//     enum MyHelper<'a> {
//         S(&'a str),
//         R(Response),
//     }

//     match MyHelper::deserialize(d) {
//         Ok(MyHelper::R(r)) => Ok(Some(r)),
//         Ok(MyHelper::S(s)) if s.is_empty() => Ok(None),
//         Ok(MyHelper::S(_)) => Err(D::Error::custom("only empty strings may be provided")),
//         Err(err) => Err(err),
//     }
// }

impl<'de> Deserialize<'de> for ClassificationString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> 
    {
        let parser = match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized => return Err(serde::de::Error::custom("classification engine not initalized")),
            // if it is disabled just handle as raw
            ClassificationMode::Disabled => None,
            // normalize on load
            ClassificationMode::Configured(parser) => Some(parser.clone()),
        };

        if let Some(parser) = parser {
            let raw = RawClassificationString::deserialize(deserializer)?;
            Self::new(raw.0, &parser).map_err(serde::de::Error::custom)
        } else {
            Ok(RawClassificationString::deserialize(deserializer)?.into())
        }
    }
}

impl PartialEq for ClassificationString {
    fn eq(&self, other: &Self) -> bool {
        let parser = match &*GLOBAL_CLASSIFICATION.lock().unwrap() {
            ClassificationMode::Uninitialized | ClassificationMode::Disabled => None,
            ClassificationMode::Configured(parser) => Some(parser.clone()),
        };

        if let Some(parser) = parser {
            match (parser.normalize_classification(&self.0), parser.normalize_classification(&other.0)) {
                (Ok(a), Ok(b)) => a == b,
                _ => false
            }
        } else {
            self.0 == other.0
        }
    }
}