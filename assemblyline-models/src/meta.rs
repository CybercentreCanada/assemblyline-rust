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
            type_: Default::default(), index: Default::default(), doc_values: Default::default(), ignore_above: Default::default(), copy_to: Default::default(), analyzer: Default::default(), normalizer: Default::default() 
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Debug)]
pub struct DynamicTemplate {
    #[serde(rename="match")]
    pub match_: Option<String>,
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
            Kind::Option(_) => todo!(),
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



// macro_rules! json_map {
//     // map-like
//     ($($k:expr => $v:expr),* $(,)?) => {{
//         [$(($k.to_owned(), json!($v)),)*].into_iter().collect()
//     }};
//     // set-like
//     // ($($v:expr),* $(,)?) => {{
//     //     core::convert::From::from([$($v,)*])
//     // }};
// }

// fn set_mapping(meta: &ElasticMeta, mut body: JsonMap) -> serde_json::Value {
//     body.insert("index".to_owned(), json!(meta.index));
//     if let Some(value) = body.get("type").cloned() {
//         if value != "text" {
//             body.insert("doc_values".to_owned(), json!(meta.index));
//         }
//         if value == "keyword" {
//             body.insert("ignore_above".to_owned(), json!(8191));  // The maximum always safe value in elasticsearch
//         }
//     }

//     if let Some(copyto) = meta.copyto {
//         // assert len(temp_field.copyto) == 1
//         body.insert("copy_to".to_owned(), json!(copyto));
//     }

//     if let Some(analyzer) = meta.analyzer {
//         body.insert("analyzer".to_owned(), json!(analyzer));
//     }
//     if let Some(normalizer) = meta.normalizer {
//         body.insert("normalizer".to_owned(), json!(normalizer));
//     }
        
//     return body.into()
// }

// fn field_from_mapping(mappings: &mut JsonMap, full_name: String, meta: &ElasticMeta, mapping: &str) {
//     mappings.insert(full_name.trim_matches('.').to_owned(), set_mapping(meta, json_map!{
//         "type" => mapping,
//     }));
// }

// fn field_from_type(mappings: &mut JsonMap, dynamic: &mut Vec<serde_json::Value>, path: Vec<&str>, full_name: String, kind: &Kind<ElasticMeta>, meta: &ElasticMeta) -> Result<(), MappingError> {
//     match &kind {
//         struct_metadata::Kind::Struct { name, children } => {
//             let children: Vec<_> = children.iter().map(|entry|(Some(entry.label), &entry.type_info.kind, &entry.metadata)).collect();
//             let (mut temp_mappings, temp_dynamic) = build_mapping_inner(&children, path, false)?;
//             mappings.append(&mut temp_mappings);
//             dynamic.extend(temp_dynamic);
//         },
//         struct_metadata::Kind::Aliased { name, kind } => {
//             let (m, d) = build_mapping_inner(&[(None, &kind.kind, &kind.metadata)], path, false)?;
//             mappings.extend(m);
//             dynamic.extend(d);
//         },
//         struct_metadata::Kind::Enum { name, variants } => todo!(),
//         struct_metadata::Kind::Sequence(child_type) => {
//             let (mut temp_mappings, temp_dynamic) = build_mapping_inner(&[(None, &child_type.kind, &child_type.metadata)], path, false)?;
//             mappings.append(&mut temp_mappings);
//             dynamic.extend(temp_dynamic);
//         },
//         struct_metadata::Kind::Option(child_type) => {
//             let (mut temp_mappings, temp_dynamic) = build_mapping_inner(&[(None, &child_type.kind, &child_type.metadata)], path, false)?;
//             mappings.append(&mut temp_mappings);
//             dynamic.extend(temp_dynamic);
//         },
//         struct_metadata::Kind::Mapping(key, value) => {
//             if key.kind != Kind::String {
//                 return Err(MappingError::OnlyStringKeys)
//             }
//             // elif isinstance(field, FlattenedObject):
//             //     if not field.index or isinstance(field.child_type, Any):
//             //         mappings[name.strip(".")] = {"type": "object", "enabled": False}
//             //     else:
//             //         dynamic.extend(build_templates(f'{name}.*', field.child_type, nested_template=True, index=field.index))

//             // elif isinstance(field, Mapping):
//             //     if not field.index or isinstance(field.child_type, Any):
//             //         mappings[name.strip(".")] = {"type": "object", "enabled": False}
//             //     else:
//             //         dynamic.extend(build_templates(f'{name}.*', field.child_type, index=field.index))
//             let index = meta.index.unwrap_or(false);
//             if !index || value.kind == struct_metadata::Kind::Any {
//                 mappings.insert(full_name.trim_matches('.').to_owned(), json!({"type": "object", "enabled": false}));
//             } else {
//                 dynamic.extend(build_templates(&(full_name + ".*"), &value.kind, &value.metadata, false, index)?);
//             }

//         },
//         struct_metadata::Kind::DateTime => {mappings.insert(full_name.trim_matches('.').to_owned(), set_mapping(meta, json_map!{
//             "type" => "date",
//             "format" => "date_optional_time||epoch_millis",
//         }));},
//         struct_metadata::Kind::String => field_from_mapping(mappings, full_name, meta, "keyword"),
//         struct_metadata::Kind::U128 |
//         struct_metadata::Kind::I128 => return Err(MappingError::UnsupportedType(full_name, "128 bit numbers not supported".to_owned())),
//         struct_metadata::Kind::U64 |
//         struct_metadata::Kind::I64 |
//         struct_metadata::Kind::U32 |
//         struct_metadata::Kind::I32 |
//         struct_metadata::Kind::U16 |
//         struct_metadata::Kind::I16 |
//         struct_metadata::Kind::U8 |
//         struct_metadata::Kind::I8 => field_from_mapping(mappings, full_name, meta, "integer"),
//         struct_metadata::Kind::F64 |
//         struct_metadata::Kind::F32 => field_from_mapping(mappings, full_name, meta, "float"),
//         struct_metadata::Kind::Bool => field_from_mapping(mappings, full_name, meta, "boolean"),
//         struct_metadata::Kind::Any => todo!(),
//     }
//     Ok(())
// }


// /// The mapping for Elasticsearch based on a model object.
// fn build_mapping_inner(children: &[(Option<&'static str>, &Kind<ElasticMeta>, &ElasticMeta)], prefix: Vec<&str>, allow_refuse_implicit: bool) -> Result<(JsonMap, Vec<serde_json::Value>), MappingError> {
    
//     let mut mappings: JsonMap = Default::default();
//     let mut dynamic: Vec<serde_json::Value> = vec![];

//     // Fill in the sections
//     for (label, kind, meta) in children {

//         let mut path = prefix.clone();
//         if let Some(label) = label {
//             path.push(label);
//         }
//         let full_name = path.join(".");

//         match meta.mapping {
//             // if a mapping has been explicity set, use it, unless its a special mapping type
//             Some(mapping) => {
//                 if mapping.eq_ignore_ascii_case("classification") {
//                     mappings.insert(full_name.trim_matches('.').to_owned(), set_mapping(meta, json_map!{
//                         "type" => "keyword"
//                     }));
//                     if !full_name.contains(".") {
//                         mappings.insert("__access_lvl__".to_owned(), json_map!{
//                             "type" => "integer",
//                             "index" => true,
//                         });
//                         mappings.insert("__access_req__".to_owned(), json_map!{
//                             "type" => "keyword",
//                             "index" => true,
//                         });
//                         mappings.insert("__access_grp1__".to_owned(), json_map!{
//                             "type" => "keyword",
//                             "index" => true,
//                         });
//                         mappings.insert("__access_grp2__".to_owned(), json_map!{
//                             "type" => "keyword",
//                             "index" => true,
//                         });
//                     }
//                 } else {
//                     field_from_mapping(&mut mappings, full_name, meta, mapping)
//                 }
//             }
//             // No maping type has been set, handle it based on type information instead
//             None => field_from_type(&mut mappings, &mut dynamic, path, full_name, kind, meta)?
//         };




//         // elif isinstance(field, Any):
//         //     if field.index:
//         //         raise ValueError(f"Any may not be indexed: {name}")

//         //     mappings[name.strip(".")] = {
//         //         "type": "keyword",
//         //         "index": False,
//         //         "doc_values": False
//         //     }

//         // else:
//         //     raise NotImplementedError(f"Unknown type for elasticsearch schema: {field.__class__}")
//     }

//     // The final template must match everything and disable indexing
//     // this effectively disables dynamic indexing EXCEPT for the templates
//     // we have defined
//     if dynamic.is_empty() && allow_refuse_implicit {
//         // We cannot use the dynamic type matching if others are in play because they conflict with each other
//         // TODO: Find a way to make them work together.
//         dynamic.push(json!({"refuse_all_implicit_mappings": {
//             "match": "*",
//             "mapping": {
//                 "index": false,
//                 "ignore_malformed": true,
//             }
//         }}))
//     }

//     Ok((mappings, dynamic))
// }


// // nested_template = false
// // index = true
// fn build_templates(name: &str, kind: &Kind<ElasticMeta>, field: &ElasticMeta, nested_template: bool, index: bool) -> Result<Vec<serde_json::Value>, MappingError> {
// //     if isinstance(field, (Keyword, Boolean, Integer, Float, Text, Json)):
// //     match kind {
// //         Kind::String | Kind::U64 | Kind::I64 | Kind::U32 | Kind::I32 |
// //         Kind::U16 | Kind::I16 | Kind::U8 | Kind::I8 |
// //         Kind::F64 | Kind::F32 |
// //         Kind::Bool => {
// //             if nested_template {
// //                 let main_template = json!({
// //                     "match": name,
// //                     "mapping": {
// //                         "type": "nested"
// //                     }
// //                 });

// //                 return Ok(vec![json!({"nested_".to_owned() + name: main_template})])
// //             } else {

// //                 let field_template = json!({
// //                     "path_match": name,
// //                     "mapping": {
// //                         "type": type_mapping(kind),
// //                         "index": index,
// //                         "copy_to": field.copyto
// //                     }
// //                 });

// //                 return Ok(vec![json!({format!("{name}_tpl"): field_template})])
// //             }
// //         },
// // //     elif isinstance(field, Any) or not index:
// //         Kind::Any | _ if !index => {
// //             if index {
// //                 return Err(MappingError::NoIndexedAny(name.to_owned()))
// //             }

// //             let field_template = json!({
// //                 "path_match": name,
// //                 "mapping": {
// //                     "type": "keyword",
// //                     "index": false
// //                 }
// //             });

// //             return Ok(vec![json!({format!("{name}_tpl"): field_template})])
// //         },
        
// //     // elif isinstance(field, (Mapping, List)):
// //         Kind::Mapping(_, child) | Kind::Sequence(child) => {
// //             let temp_name = name;
// //             if field.name:
// //                 temp_name = f"{name}.{field.name}"
// //             return build_templates(temp_name, field.child_type, nested_template=True)
// //         }

// //     elif isinstance(field, Compound):
// //         temp_name = name
// //         if field.name:
// //             temp_name = f"{name}.{field.name}"

// //         out = []
// //         for sub_name, sub_field in field.fields().items():
// //             sub_name = f"{temp_name}.{sub_name}"
// //             out.extend(build_templates(sub_name, sub_field))

// //         return out

// //     elif isinstance(field, Optional):
// //         return build_templates(name, field.child_type, nested_template=nested_template)

//     }


//     todo!()

// //     else:
// //         raise NotImplementedError(f"Unknown type for elasticsearch dynamic mapping: {field.__class__}")

// }