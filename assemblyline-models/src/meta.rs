use std::collections::{HashMap, BTreeMap};

use serde::{Serialize, Deserialize};
use serde_json::json;
use struct_metadata::{Described, Descriptor, Kind, MetadataKind};
use crate::serialize::{deserialize_bool, deserialize_string_or_list};


/// Retrieve all subfields recursively flattened into a single listing.    
/// 
/// This method strips direct information about whether a field is optional or part of a sequence and includes it as boolean flags.
/// response type is mapping from path to (type info, bool flag for multivalued, bool flag for optional)
pub fn flatten_fields(target: &Descriptor<ElasticMeta>) -> HashMap<Vec<String>, (&Descriptor<ElasticMeta>, bool, bool)> {    
    // if target.metadata.mapping.is_some() {
    //     return [(vec![], (target, false, false))].into_iter().collect()
    // }
    match &target.kind {
        Kind::Struct { children , ..} => {
            let mut fields = HashMap::<_, _>::new();
            for child in children {
                for (mut path, (subfield, multivalued, optional)) in flatten_fields(&child.type_info) {
                    let mut label = vec![child.label.to_owned()];
                    if !path.is_empty() {
                        label.append(&mut path);
                    }
                    fields.insert(label, (subfield, multivalued, optional));
                }
            }
            fields
        },
        Kind::Sequence(kind) => {
            let mut fields = flatten_fields(kind);
            for row in fields.iter_mut() {
                row.1.1 = true
            }
            fields
        }
        Kind::Option(kind) => {
            let mut fields = flatten_fields(kind);
            for (_, _, optional) in fields.values_mut() {
                *optional = true;
            }
            fields
        },
        Kind::Aliased { kind, .. } => flatten_fields(kind),
        _ => [(vec![], (target, false, false))].into_iter().collect(),
    }
}

/// Metadata fields required for converting the structs to elasticsearch mappings
#[derive(Default, PartialEq, Eq, Debug, Clone)]
pub struct ElasticMeta {
    pub index: Option<bool>,
    pub store: Option<bool>,
    pub copyto: Option<&'static str>,
    pub mapping: Option<&'static str>,
    pub analyzer: Option<&'static str>,
    pub normalizer: Option<&'static str>,
}

impl MetadataKind for ElasticMeta {
    fn forward_propagate_context(&mut self, context: &Self) {
        self.index = self.index.or(context.index);
        self.store = self.store.or(context.store);
        self.copyto = self.copyto.or(context.copyto);
        self.mapping = self.mapping.or(context.mapping);
        self.analyzer = self.analyzer.or(context.analyzer);
        self.normalizer = self.normalizer.or(context.normalizer);        
    }

    fn forward_propagate_child_defaults(&mut self, kind: &ElasticMeta) {
        self.index = self.index.or(kind.index);
        self.store = self.store.or(kind.store);
        self.copyto = self.copyto.or(kind.copyto);
        self.mapping = self.mapping.or(kind.mapping);
        self.analyzer = self.analyzer.or(kind.analyzer);
        self.normalizer = self.normalizer.or(kind.normalizer);
    }

    fn forward_propagate_entry_defaults(&mut self, context: &ElasticMeta, kind: &ElasticMeta) {
        self.index = self.index.or(kind.index).or(context.index);
        self.store = self.store.or(kind.store).or(context.store);
        self.copyto = self.copyto.or(kind.copyto).or(context.copyto);
        self.mapping = self.mapping.or(kind.mapping).or(context.mapping);
        self.analyzer = self.analyzer.or(kind.analyzer).or(context.analyzer);
        self.normalizer = self.normalizer.or(kind.normalizer).or(context.normalizer);
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default)]
#[serde(default)]
pub struct FieldMapping {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(rename="type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_malformed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_values: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_above: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty", deserialize_with="deserialize_string_or_list")]
    pub copy_to: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalizer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, FieldMapping>,
}


#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Debug)]
#[serde(default)]
pub struct DynamicTemplate {
    #[serde(rename="match", skip_serializing_if = "Option::is_none")]
    pub match_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_mapping_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_match: Option<String>,
    pub mapping: FieldMapping,
}


fn dynamic_default() -> bool { true }

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(default)]
pub struct Mappings {
    #[serde(default="dynamic_default", deserialize_with="deserialize_bool")]
    pub dynamic: bool,
    pub properties: BTreeMap<String, FieldMapping>,
    pub dynamic_templates: Vec<HashMap<String, DynamicTemplate>>,
}

impl Default for Mappings {
    fn default() -> Self {
        Self { dynamic: dynamic_default(), properties: Default::default(), dynamic_templates: Default::default() }
    }
}

impl Mappings {
    fn insert(&mut self, name: &str, meta: &ElasticMeta, mut field: FieldMapping) {
        field.index = field.index.or(meta.index);
        field.store = field.store.or(meta.store);

        if field.type_.clone().is_some_and(|x| x != "text") {
            field.doc_values = meta.index;
        }

        if let Some(value) = meta.copyto {
            field.copy_to.push(value.to_owned());
            field.copy_to.sort_unstable();
            field.copy_to.dedup();
            field.copy_to.retain(|s|!s.is_empty());
        }
        // field.copy_to = field.copy_to.or(meta.copyto.map(ToOwned::to_owned));
        // if field.copy_to.as_ref().is_some_and(|copyto| copyto.is_empty()) {
        //     field.copy_to = None;
        // }
        field.analyzer = field.analyzer.or(meta.analyzer.map(ToOwned::to_owned));
        field.normalizer = field.normalizer.or(meta.normalizer.map(ToOwned::to_owned));
            
        self.properties.insert(name.trim_matches('.').to_owned(), field);
    }

    fn build_field(&mut self, label: Option<&str>, kind: &Kind<ElasticMeta>, meta: &ElasticMeta, prefix: &[&str], _allow_refuse_implicit: bool) -> Result<(), MappingError> {
        // resolve the absolute name for this field
        let mut path = Vec::from(prefix);
        if let Some(label) = label {
            path.push(label);
        }
        let full_name = path.join(".");

        // if a mapping is simple or has been explicity set, use it
        let simple_mapping = meta.mapping.or(simple_mapping(kind));

        if let Some(mapping) = simple_mapping {
            if mapping.eq_ignore_ascii_case("classification") {
                self.insert(&full_name, meta, FieldMapping{ type_: "keyword".to_owned().into(), ..Default::default() });
                if !full_name.contains('.') {
                    self.properties.insert("__access_lvl__".to_owned(), FieldMapping{type_: "integer".to_owned().into(), index: true.into(), ..Default::default()});
                    self.properties.insert("__access_req__".to_owned(), FieldMapping{type_: "keyword".to_owned().into(), index: true.into(), ..Default::default()});
                    self.properties.insert("__access_grp1__".to_owned(), FieldMapping{type_: "keyword".to_owned().into(), index: true.into(), ..Default::default()});
                    self.properties.insert("__access_grp2__".to_owned(), FieldMapping{type_: "keyword".to_owned().into(), index: true.into(), ..Default::default()});
                }
            } else if mapping.eq_ignore_ascii_case("classification_string") {
                self.insert(&full_name, meta, FieldMapping{ type_: "keyword".to_owned().into(), ..Default::default() });
            } else if mapping.eq_ignore_ascii_case("flattenedobject") {
                if let Kind::Mapping(key_type, child_type) = kind {
                    if key_type.kind != Kind::String {
                        return Err(MappingError::OnlyStringKeys)
                    }
    
                    let index = meta.index.or(child_type.metadata.index).unwrap_or_default();
                    // todo!("{:?}", child_type.kind);
                    if !index || matches!(child_type.kind, Kind::Any) {
                        self.insert(&full_name, meta, FieldMapping{
                            type_: Some("object".to_owned()),
                            enabled: Some(false),
                            ..Default::default()
                        })
                    } else {
                        self.build_dynamic(&(full_name + ".*"), &child_type.kind, meta, true, index)?;
                    }
                } else {
                    return Err(MappingError::UnsupportedType(full_name, format!("{kind:?}")))
                }
            } else if mapping.eq_ignore_ascii_case("date") {
                self.insert(&full_name, meta, FieldMapping{ 
                    type_: Some(mapping.to_owned()), 
                    // The maximum always safe value in elasticsearch
                    format: Some("date_optional_time||epoch_millis".to_owned()),
                    ..Default::default()
                });                
            } else if mapping.eq_ignore_ascii_case("keyword") {
                self.insert(&full_name, meta, FieldMapping{ 
                    type_: Some(mapping.to_owned()), 
                    // The maximum always safe value in elasticsearch
                    ignore_above: Some(8191),
                    ..Default::default()
                });
            } else if mapping.eq_ignore_ascii_case("wildcard") {
                self.properties.insert(full_name, FieldMapping{ 
                    type_: Some(mapping.to_owned()), 
                    copy_to: meta.copyto.map(|item| vec![item.to_owned()]).unwrap_or_default(),
                    ..Default::default()
                });
            } else {
                self.insert(&full_name, meta, FieldMapping{ 
                    type_: Some(mapping.to_owned()), 
                    ..Default::default()
                });
            }
            return Ok(());
        };
        // handle complex mappings
        match kind {
            Kind::Struct { children, .. } => {
                for child in children {
                    self.build_field(Some(child.label), &child.type_info.kind, &child.metadata,  &path, _allow_refuse_implicit)?;
                }
            },
            Kind::Aliased { kind, .. } => {
                self.build_field(None, &kind.kind, meta, &path, _allow_refuse_implicit)?;
            },
            Kind::Enum { .. } => {
                self.insert(&full_name, meta, FieldMapping{ 
                    type_: Some("keyword".to_owned()), 
                    // The maximum always safe value in elasticsearch
                    ignore_above: Some(8191),
                    ..Default::default()
                });
            },
            Kind::Sequence(kind) => {
                self.build_field(None, &kind.kind, meta, &path, _allow_refuse_implicit)?;
            },
            Kind::Option(kind) => {
                self.build_field(None, &kind.kind, meta, &path, _allow_refuse_implicit)?;
            },
            Kind::Mapping(key, value) => {
                if key.kind != Kind::String {
                    return Err(MappingError::OnlyStringKeys)
                }
                // elif isinstance(field, FlattenedObject):
                //     if not field.index or isinstance(field.child_type, Any):
                //         mappings[name.strip(".")] = {"type": "object", "enabled": False}
                //     else:
                //         dynamic.extend(build_templates(f'{name}.*', field.child_type, nested_template=True, index=field.index))

                // elif isinstance(field, Mapping):
                //     if not field.index or isinstance(field.child_type, Any):
                //         mappings[name.strip(".")] = {"type": "object", "enabled": False}
                //     else:
                //         dynamic.extend(build_templates(f'{name}.*', field.child_type, index=field.index))
                let index = meta.index.unwrap_or(false);
                if !index || value.kind == struct_metadata::Kind::Any {
                    let mut meta = meta.clone();
                    meta.index = None;
                    meta.store = None;
                    self.insert(&full_name, &meta, FieldMapping{
                        type_: "object".to_owned().into(),
                        enabled: false.into(),
                        ..Default::default()
                    });
                } else {
                    self.build_dynamic(&(full_name + ".*"), &value.kind, &value.metadata, false, index)?;
                }
            },
            Kind::Any | Kind::JSON => {
                let index = meta.index.unwrap_or(false);
                if index {
                    return Err(MappingError::NoIndexedAny(full_name));
                }

                self.insert(&full_name, meta, FieldMapping{
                    type_: Some("keyword".to_string()),
                    index: Some(false),
                    ignore_above: Some(8191),
                    doc_values: Some(false),
                    ..Default::default()
                });
            },
            _ => return Err(MappingError::UnsupportedType(full_name, format!("{kind:?}")))
        };
        Ok(())
    }

    // // nested_template = false
    // // index = true
    fn build_dynamic(&mut self, name: &str, kind: &Kind<ElasticMeta>, meta: &ElasticMeta, nested_template: bool, index: bool) -> Result<(), MappingError> {
        // if isinstance(field, (Keyword, Boolean, Integer, Float, Text, Json)):
        match kind {
            Kind::JSON | Kind::String | Kind::Enum { .. } |
            Kind::U64 | Kind::I64 | Kind::U32 | Kind::I32 | Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 |
            Kind::F64 | Kind::F32 |
            Kind::Bool => {
                if nested_template {
                    self.insert_dynamic("nested_".to_owned() + name, DynamicTemplate {
                        match_: Some(name.to_string()),
                        mapping: FieldMapping {
                            type_: "nested".to_string().into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    });
                } else if let Some(mapping) = simple_mapping(kind) {
                    self.insert_dynamic(format!("{name}_tpl"), DynamicTemplate {
                        path_match: Some(name.to_owned()),
                        mapping: FieldMapping{
                            type_: mapping.to_owned().into(),
                            index: meta.index,
                            // copy_to: meta.copyto.map(ToOwned::to_owned),
                            copy_to: meta.copyto.map(|item| vec![item.to_owned()]).unwrap_or_default(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                } else {
                    return Err(MappingError::UnsupportedType(name.to_owned(), format!("{kind:?}")))
                }
                return Ok(())
            },
            
            Kind::Aliased {name: name_, kind } if *name_ == "Wildcard" => {
                self.insert_dynamic(format!("{name}_tpl"), DynamicTemplate {
                    path_match: Some(name.to_owned()),
                    mapping: FieldMapping{
                        type_: Some("wildcard".to_owned()),
                        // copy_to: meta.copyto.map(ToOwned::to_owned),
                        copy_to: meta.copyto.map(|item| vec![item.to_owned()]).unwrap_or_default(),
                        ..Default::default()
                    },
                    ..Default::default()
                });
                return Ok(())
            }

            // elif isinstance(field, (Mapping, List)):
            Kind::Mapping(_, child) | Kind::Sequence(child) => {
                // let temp_name = if field {
                //     format!("{name}.{}", field.name)
                // } else {
                //     name.to_owned()
                // };
                return self.build_dynamic(name, &child.kind, &child.metadata, true, true)
            }


            // elif isinstance(field, Compound):
            Kind::Struct { children, .. } => {
                // let temp_name =  name
                // if field.name:
                //     temp_name = f"{name}.{field.name}"

                // out = []
                for child in children {
                    let sub_name = format!("{name}.{}", child.label);
                    self.build_dynamic(&sub_name, &child.type_info.kind, &child.metadata, false, true)?;
                    // out.extend(build_templates(sub_name, sub_field))
                }
// 
                // return out
                return Ok(())
            }

            // elif isinstance(field, Optional):
            Kind::Option(kind) => { 
                return self.build_dynamic(name, &kind.kind, meta, nested_template, true);
                // return build_templates(name, field.child_type, nested_template=nested_template)
            }

            _ => {}
        }

        // elif isinstance(field, Any) or not index:
        if matches!(kind, Kind::Any) || !index {
            if index {
                return Err(MappingError::NoIndexedAny(name.to_owned()))
            }

            self.insert_dynamic(format!("{name}_tpl"), DynamicTemplate {
                path_match: Some(name.to_owned()),
                mapping: FieldMapping {
                    type_: "keyword".to_owned().into(),
                    index: false.into(),
                    ..Default::default()
                },
                ..Default::default()
            });

            return Ok(())
        }

        todo!("Unknown type for elasticsearch dynamic mapping: {kind:?}");
    }

    pub fn insert_dynamic(&mut self, name: String, template: DynamicTemplate) {
        self.dynamic_templates.push([(name, template)].into_iter().collect());
    }

    pub fn apply_defaults(&mut self) {
        self.dynamic_templates.push([("strings_as_keywords".to_owned(), DynamicTemplate {
            match_: None,
            path_match: None,
            match_mapping_type: Some("string".to_owned()),
            mapping: FieldMapping {
                type_: "keyword".to_owned().into(),
                ignore_above: Some(8191),
                ..Default::default()
            }
        })].into_iter().collect());

        if !self.properties.contains_key("id") {
            self.properties.insert("id".to_owned(), FieldMapping {
                store: true.into(),
                doc_values: Some(true),
                type_: "keyword".to_owned().into(),
                copy_to: vec!["__text__".to_string()],
                ..Default::default()
            });
        }
    
        self.properties.insert("__text__".to_owned(), FieldMapping {
            store: false.into(),
            type_: "text".to_owned().into(),
            ..Default::default()
        });
    
    }

}

pub fn default_settings(index: serde_json::Value) -> serde_json::Value {
     json!({
        "analysis": {
            "filter": {
                "text_ws_dsplit": {
                    "type": "pattern_replace",
                    "pattern": r"(\.)",
                    "replacement": " "
                }
            },
            "analyzer": {
                "string_ci": {
                    "type": "custom",
                    "tokenizer": "keyword",
                    "filter": ["lowercase"]
                },
                "text_fuzzy": {
                    "type": "pattern",
                    "pattern": r"\s*:\s*",
                    "lowercase": false
                },
                "text_whitespace": {
                    "type": "whitespace"
                },
                "text_ws_dsplit": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filters": ["text_ws_dsplit"]
                }
            },
            "normalizer": {
                "lowercase_normalizer": {
                    "type": "custom",
                    "char_filter": [],
                    "filter": ["lowercase"]
                }
            }
        },
        "index": index,
    })
}

pub fn build_mapping<T: Described<ElasticMeta>>() -> Result<Mappings, MappingError> {
    let metadata = T::metadata();
    if let struct_metadata::Kind::Struct { children, .. } = metadata.kind {
        let children: Vec<_> = children.iter().map(|entry|(Some(entry.label), &entry.type_info.kind, &entry.metadata)).collect();
        build_mapping_inner(&children, vec![], true)
    } else {
        Err(MappingError::OnlyStructs)
    }
}

/// The mapping for Elasticsearch based on a model object.
pub fn build_mapping_inner(children: &[(Option<&'static str>, &Kind<ElasticMeta>, &ElasticMeta)], prefix: Vec<&str>, allow_refuse_implicit: bool) -> Result<Mappings, MappingError> {
    let mut mappings = Mappings::default();

    // Fill in the sections
    for (label, kind, meta) in children {
        mappings.build_field(*label, kind, meta, &prefix, allow_refuse_implicit)?;
    }

    // The final template must match everything and disable indexing
    // this effectively disables dynamic indexing EXCEPT for the templates
    // we have defined
    if mappings.dynamic_templates.is_empty() && allow_refuse_implicit {
        // We cannot use the dynamic type matching if others are in play because they conflict with each other
        // TODO: Find a way to make them work together.
        mappings.insert_dynamic("refuse_all_implicit_mappings".to_owned(), DynamicTemplate {
            match_: Some("*".to_owned()),
            mapping: FieldMapping {
                index: false.into(),
                ignore_malformed: true.into(),
                ..Default::default()
            },
            ..Default::default()
        });
    }

    Ok(mappings)
}

// fn build_field_mapping(mapping: &mut Mappings) -> Result<(), MappingError> {
//     todo!()
// }

// Simple types can be resolved by a direct mapping
fn simple_mapping(kind: &Kind<ElasticMeta>) -> Option<&'static str> {
    match kind {
        Kind::Struct { .. } => None,
        Kind::Aliased { kind, .. } => match kind.metadata.mapping {
            Some(mapping) => Some(mapping),
            None => simple_mapping(&kind.kind),
        },
        Kind::Enum { .. } => Some("keyword"),
        Kind::Sequence(kind) => match kind.metadata.mapping {
            Some(mapping) => Some(mapping),
            None => simple_mapping(&kind.kind),
        },
        Kind::Option(kind) => match kind.metadata.mapping {
            Some(mapping) => Some(mapping),
            None => simple_mapping(&kind.kind),
        },
        Kind::Mapping(..) => None,
        Kind::DateTime => Some("date"),
        Kind::String => Some("keyword"),
        Kind::U128 | Kind::I128 => None,
        Kind::U64 => Some("unsigned_long"),
        Kind::I64 | Kind::U32 => Some("long"),
        Kind::I32 | Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 => Some("integer"),
        Kind::F64 => Some("double"),
        Kind::F32 => Some("float"),
        Kind::Bool => Some("boolean"),
        Kind::Any => Some("keyword"),
        Kind::JSON => None,
        _ => todo!("{kind:?}"),
    }
    // Text: 'text',
    // Classification: 'keyword',
    // ClassificationString: 'keyword',
    // UUID: 'keyword',
    // IP: 'ip',
    // Domain: 'keyword',
    // Email: 'keyword',
    // URI: 'keyword',
    // UNCPath: 'keyword',
    // URIPath: 'keyword',
    // MAC: 'keyword',
    // PhoneNumber: 'keyword',
    // SSDeepHash: 'text',
    // SHA1: 'keyword',
    // SHA256: 'keyword',
    // MD5: 'keyword',
    // Platform: 'keyword',
    // Processor: 'keyword',
    // FlattenedObject: 'nested',
    // UpperKeyword: 'keyword',
    // Json: 'keyword',
    // ValidatedKeyword: 'keyword'
}
// // __analyzer_mapping = {
// //     SSDeepHash: 'text_fuzzy',
// // }
// // __normalizer_mapping = {
// //     SHA1: 'lowercase_normalizer',
// //     SHA256: 'lowercase_normalizer',
// //     MD5: 'lowercase_normalizer',
// // }
// // # TODO: We might want to use custom analyzers for Classification and Enum and not create special backmapping cases
// // back_mapping = {v: k for k, v in __type_mapping.items() if k not in [Enum, Classification, UUID, IP, Domain, URI,
// //                                                                      URIPath, MAC, PhoneNumber, SSDeepHash, Email,
// //                                                                      SHA1, SHA256, MD5, Platform, Processor,
// //                                                                      ClassificationString, Any, UpperKeyword, Json,
// //                                                                      ValidatedKeyword, UNCPath]}
// // back_mapping.update({x: Keyword for x in set(__analyzer_mapping.values())})

#[derive(Debug)]
pub enum MappingError {
    OnlyStructs,
    UnsupportedType(String, String),
    NoIndexedAny(String),
    OnlyStringKeys
}

impl std::fmt::Display for MappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingError::OnlyStructs => f.write_str("Mappings can only be created for structs"),
            MappingError::UnsupportedType(name, kind) => f.write_fmt(format_args!("The field {name} is assigned an unsupported type {kind}")),
            MappingError::NoIndexedAny(name) => f.write_fmt(format_args!("The field {name} can't be Any type while being indexed.")),
            MappingError::OnlyStringKeys => f.write_str("Mapping keys must be strings"),
        }
    }
}

impl std::error::Error for MappingError {

}