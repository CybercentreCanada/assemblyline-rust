use std::collections::HashMap;

use anyhow::Result;
use assemblyline_models::ElasticMeta;



pub fn flat_fields(data: struct_metadata::Descriptor<ElasticMeta>) -> Result<HashMap<String, struct_metadata::Descriptor<ElasticMeta>>> {
    use struct_metadata::Kind;
    if let Kind::Struct { children, .. } = data.kind {
        Ok(_flat_fields(children))
    } else {
        anyhow::bail!("Only structs have fields")
    }
}

fn _flat_fields(children: Vec<struct_metadata::Entry<ElasticMeta>>) -> HashMap<String, struct_metadata::Descriptor<ElasticMeta>> {
    use struct_metadata::Kind;
    let mut fields = HashMap::new();
    for child in children {
        let child_fields = match child.type_info.kind {
            Kind::Struct { children, .. } => Child::Struct(_flat_fields(children)),
            Kind::Aliased { kind, .. } => _child_flat_fields(*kind),
            Kind::Sequence(descriptor) => _child_flat_fields(*descriptor),
            Kind::Option(descriptor) => _child_flat_fields(*descriptor),
            Kind::Mapping(_, _) => continue,
            _ => {
                Child::Single(child.type_info)
            }
        };

        match child_fields {
            Child::Struct(hash_map) => {
                for (sub_label, descriptor) in hash_map {
                    fields.insert(format!("{}.{sub_label}", child.label), descriptor);
                }
            },
            Child::Single(descriptor) => {
                fields.insert(child.label.to_owned(), descriptor);
            },
        }
    }
    fields
}

enum Child {
    Struct(HashMap<String, struct_metadata::Descriptor<ElasticMeta>>),
    Single(struct_metadata::Descriptor<ElasticMeta>)
}

fn _child_flat_fields(child: struct_metadata::Descriptor<ElasticMeta>) -> Child {
    use struct_metadata::Kind;
    match child.kind {
        Kind::Struct { children, .. } => Child::Struct(_flat_fields(children)),
        Kind::Aliased { kind, .. } => _child_flat_fields(*kind),
        Kind::Sequence(descriptor) => _child_flat_fields(*descriptor),
        Kind::Option(descriptor) => _child_flat_fields(*descriptor),
        Kind::Mapping(_, _) => Child::Struct(Default::default()),
        _ => {
            Child::Single(child)
        }
    }
}


/// convert a json value to a string
/// When a field or parameter is expected to be a string
pub fn value_to_string(value: &serde_json::Value) -> serde_json::Result<String> {
    match value {
        serde_json::Value::Null => Ok("".to_string()),
        serde_json::Value::Bool(value) => Ok(if *value { "true" } else { "false" }.to_string()),
        serde_json::Value::Number(number) => Ok(format!("{number}")),
        serde_json::Value::String(str) => Ok(str.clone()),
        serde_json::Value::Array(vec) => serde_json::to_string(vec),
        serde_json::Value::Object(map) => serde_json::to_string(map),
    }
}
