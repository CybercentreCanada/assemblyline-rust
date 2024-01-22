use std::collections::{HashMap, BTreeMap};

use serde::{Serialize, Deserialize};
use serde_json::json;
use struct_metadata::{Described, Entry, Kind, MetadataKind};

use crate::JsonMap;


/// Metadata fields required for converting the structs to elasticsearch mappings
#[derive(Default, PartialEq, Eq, Debug)]
pub struct ElasticMeta {
    pub index: Option<bool>,
    pub store: Option<bool>,
    pub copyto: Option<&'static str>,
    pub mapping: Option<&'static str>,
    pub analyzer: Option<&'static str>,
    pub normalizer: Option<&'static str>,
}

impl MetadataKind for ElasticMeta {
    fn forward_propagate_entry_defaults(&mut self, context: &ElasticMeta, kind: &ElasticMeta) {
        self.index = self.index.or(kind.index).or(context.index);
        self.store = self.store.or(kind.store).or(context.store);
        self.copyto = self.copyto.or(kind.copyto).or(context.copyto);
        self.mapping = self.mapping.or(kind.mapping).or(context.mapping);
        self.analyzer = self.analyzer.or(kind.analyzer).or(context.analyzer);
        self.normalizer = self.normalizer.or(kind.normalizer).or(context.normalizer);
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(default)]
pub struct FieldMapping {
    pub enabled: bool,
    #[serde(rename="type")]
    pub type_: String,
    pub index: bool,
    pub store: bool,
    pub ignore_malformed: bool,
    pub doc_values: Option<bool>,
    pub ignore_above: Option<u32>,
    pub copy_to: Option<String>,
    pub analyzer: Option<String>,
    pub normalizer: Option<String>,
}

impl Default for FieldMapping {
    fn default() -> Self {
        Self { 
            enabled: true, 
            ignore_malformed: false,
            store: false,
            type_: Default::default(), index: Default::default(), doc_values: Default::default(), ignore_above: Default::default(), copy_to: Default::default(), analyzer: Default::default(), normalizer: Default::default() 
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Debug)]
pub struct DynamicTemplate {
    #[serde(rename="match")]
    pub match_: Option<String>,
    pub match_mapping_type: Option<String>,
    pub path_match: Option<String>,
    pub mapping: FieldMapping,
}


#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Debug)]
pub struct Mappings {
    pub properties: BTreeMap<String, FieldMapping>,
    pub dynamic_templates: Vec<HashMap<String, DynamicTemplate>>,
}

impl Mappings {
    fn insert(&mut self, name: &str, meta: &ElasticMeta, mut field: FieldMapping) {
        field.index |= meta.index.unwrap_or_default();
        // field.store |= meta.store.unwrap_or_default();

        if field.type_ != "text" {
            field.doc_values = meta.index;
        }

        field.copy_to = meta.copyto.map(ToOwned::to_owned);
        field.analyzer = meta.analyzer.map(ToOwned::to_owned);
        field.normalizer = meta.normalizer.map(ToOwned::to_owned);
            
        self.properties.insert(name.trim_matches('.').to_owned(), field);
    }

    fn build_field(&mut self, label: Option<&str>, kind: &Kind<ElasticMeta>, meta: &ElasticMeta, prefix: &Vec<&str>, allow_refuse_implicit: bool) -> Result<(), MappingError> {
        // resolve the absolute name for this field
        let mut path = prefix.clone();
        if let Some(label) = label {
            path.push(label);
        }
        let full_name = path.join(".");

        // if a mapping is simple or has been explicity set, use it
        let simple_mapping = meta.mapping.or(simple_mapping(kind));
        if let Some(mapping) = simple_mapping {
            if mapping.eq_ignore_ascii_case("classification") {
                self.insert(&full_name, meta, FieldMapping{type_: "keyword".to_owned(), ..Default::default()});
                if !full_name.contains(".") {
                    self.properties.insert("__access_lvl__".to_owned(), FieldMapping{type_: "integer".to_owned(), index: true, ..Default::default()});
                    self.properties.insert("__access_req__".to_owned(), FieldMapping{type_: "keyword".to_owned(), index: true, ..Default::default()});
                    self.properties.insert("__access_grp1__".to_owned(), FieldMapping{type_: "keyword".to_owned(), index: true, ..Default::default()});
                    self.properties.insert("__access_grp2__".to_owned(), FieldMapping{type_: "keyword".to_owned(), index: true, ..Default::default()});
                }
            } else if mapping.eq_ignore_ascii_case("keyword") {
                self.insert(&full_name, meta, FieldMapping{ 
                    type_: mapping.to_owned(), 
                    // The maximum always safe value in elasticsearch
                    ignore_above: Some(8191),
                    ..Default::default()
                });
            } else {
                self.insert(&full_name, meta, FieldMapping{ 
                    type_: mapping.to_owned(), 
                    ..Default::default()
                });
            }
            return Ok(());
        };
        // handle complex mappings
        match kind {
            Kind::Struct { children, .. } => {
                for child in children {
                    self.build_field(Some(child.label), &child.type_info.kind, &child.metadata, &path, allow_refuse_implicit)?;
                }
            },
            Kind::Aliased { name, kind } => todo!(),
            Kind::Enum { name, variants } => todo!(),
            Kind::Sequence(_) => todo!(),
            Kind::Option(kind) => {
                self.build_field(None, &kind.kind, &meta, &path, allow_refuse_implicit)?;
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
                    self.insert(&full_name, meta, FieldMapping{
                        type_: "object".to_owned(),
                        enabled: false,
                        ..Default::default()
                    });
                } else {
                    self.build_dynamic(&(full_name + ".*"), &value.kind, &value.metadata, false, index)?;
                }
            },
            Kind::Any => {
                        // elif isinstance(field, Any):
        //     if field.index:
        //         raise ValueError(f"Any may not be indexed: {name}")

        //     mappings[name.strip(".")] = {
        //         "type": "keyword",
        //         "index": False,
        //         "doc_values": False
        //     }
                todo!();
            },
            _ => return Err(MappingError::UnsupportedType(full_name, format!("{kind:?}")))
        };
        return Ok(())
    }

    // // nested_template = false
    // // index = true
    fn build_dynamic(&mut self, name: &str, kind: &Kind<ElasticMeta>, meta: &ElasticMeta, nested_template: bool, index: bool) -> Result<(), MappingError> {
    //     if isinstance(field, (Keyword, Boolean, Integer, Float, Text, Json)):
        match kind {
            Kind::String | Kind::U64 | Kind::I64 | Kind::U32 | Kind::I32 |
            Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 |
            Kind::F64 | Kind::F32 |
            Kind::Bool => {
                if nested_template {
                    self.insert_dynamic("nested_".to_owned() + name, DynamicTemplate {
                        match_: Some(name.to_string()),
                        mapping: FieldMapping {
                            type_: "nested".to_string(),
                            ..Default::default()
                        },
                        ..Default::default()
                    });
                } else if let Some(mapping) = simple_mapping(kind) {
                    self.insert_dynamic(format!("{name}_tpl"), DynamicTemplate {
                        path_match: Some(name.to_owned()),
                        mapping: FieldMapping{
                            type_: mapping.to_owned(),
                            index: index,
                            copy_to: meta.copyto.map(ToOwned::to_owned),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                } else {
                    return Err(MappingError::UnsupportedType(name.to_owned(), format!("{kind:?}")))
                }
                return Ok(())
            },
    //     elif isinstance(field, Any) or not index:
            Kind::Any | _ if !index => {
                if index {
                    return Err(MappingError::NoIndexedAny(name.to_owned()))
                }

                self.insert_dynamic(format!("{name}_tpl"), DynamicTemplate {
                    path_match: Some(name.to_owned()),
                    mapping: FieldMapping {
                        type_: "keyword".to_owned(),
                        index: false,
                        ..Default::default()
                    },
                    ..Default::default()
                });

                return Ok(())
            },
            
        // elif isinstance(field, (Mapping, List)):
            Kind::Mapping(_, child) | Kind::Sequence(child) => {
                // let temp_name = if field {
                //     format!("{name}.{}", field.name)
                // } else {
                //     name.to_owned()
                // };
                return self.build_dynamic(&name, &child.kind, &child.metadata, true, true)
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


        todo!()

    //     else:
    //         raise NotImplementedError(f"Unknown type for elasticsearch dynamic mapping: {field.__class__}")

    }

    pub fn insert_dynamic(&mut self, name: String, template: DynamicTemplate) {
        self.dynamic_templates.push([(name, template)].into_iter().collect());
    }

    pub fn apply_defaults(&mut self) {
        self.dynamic_templates.insert(0, [("strings_as_keywords".to_owned(), DynamicTemplate {
            match_: None,
            path_match: None,
            match_mapping_type: Some("string".to_owned()),
            mapping: FieldMapping {
                type_: "keyword".to_owned(),
                ignore_above: Some(8191),
                ..Default::default()
            }
        })].into_iter().collect());

        if !self.properties.contains_key("id") {
            self.properties.insert("id".to_owned(), FieldMapping {
                store: true,
                doc_values: Some(true),
                type_: "keyword".to_owned(),
                ..Default::default()
            });
        }
    
        self.properties.insert("__text__".to_owned(), FieldMapping {
            store: false,
            type_: "text".to_owned(),
            ..Default::default()
        });
    
    }

}



pub fn build_mapping<T: Described<ElasticMeta>>() -> Result<Mappings, MappingError> {
    let metadata = T::metadata();
    if let struct_metadata::Kind::Struct { name, children } = metadata.kind {
        let children: Vec<_> = children.iter().map(|entry|(Some(entry.label), &entry.type_info.kind, &entry.metadata)).collect();
        build_mapping_inner(&children, vec![], true)
    } else {
        Err(MappingError::OnlyStructs)
    }
}

/// The mapping for Elasticsearch based on a model object.
fn build_mapping_inner(children: &[(Option<&'static str>, &Kind<ElasticMeta>, &ElasticMeta)], prefix: Vec<&str>, allow_refuse_implicit: bool) -> Result<Mappings, MappingError> {
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
                index: false,
                ignore_malformed: true,
                ..Default::default()
            },
            ..Default::default()
        });
    }

    Ok(mappings)
}

fn build_field_mapping(mapping: &mut Mappings) -> Result<(), MappingError> {
    todo!()
}

// Simple types can be resolved by a direct mapping
fn simple_mapping(kind: &Kind<ElasticMeta>) -> Option<&'static str> {
    match kind {
        Kind::Struct { name, children } => None,
        Kind::Aliased { name, kind } => match kind.metadata.mapping {
            Some(mapping) => Some(mapping),
            None => simple_mapping(&kind.kind),
        },
        Kind::Enum { name, variants } => Some("keyword"),
        Kind::Sequence(kind) => simple_mapping(&kind.kind),
        Kind::Option(kind) => simple_mapping(&kind.kind),
        Kind::Mapping(key, value) => None,
        Kind::DateTime => Some("date"),
        Kind::String => Some("keyword"),
        Kind::U128 | Kind::I128 => None,
        Kind::U64 | Kind::I64 | Kind::U32 | Kind::I32 | Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 => Some("integer"),
        Kind::F64 | Kind::F32 => Some("float"),
        Kind::Bool => Some("boolean"),
        Kind::Any => Some("keyword"),
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
