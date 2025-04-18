
use serde::de;
use serde::Deserialize;

pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: de::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value<'a> {
        Raw(bool),
        Str(&'a str)
    }

    let value: Value = de::Deserialize::deserialize(deserializer)?;

    match value {
        Value::Raw(value) => Ok(value),
        Value::Str(string) => Ok(string.trim().eq_ignore_ascii_case("true")),
    }    
}

pub fn deserialize_string_or_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: de::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Str(String),
        Vec(Vec<String>)
    }

    let value: Value = de::Deserialize::deserialize(deserializer)?;

    match value {
        Value::Vec(value) => Ok(value),
        Value::Str(string) => Ok(vec![string]),
    }    
}


#[cfg(test)]
pub mod test {

    use assemblyline_markings::classification::ClassificationParser;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use struct_metadata::Described;
    use pretty_assertions::assert_eq;

    use crate::{ElasticMeta, ClassificationString, ExpandingClassification};

    pub fn setup_classification() -> std::sync::Arc<ClassificationParser> {
        let parser = std::sync::Arc::new(ClassificationParser::new(serde_json::from_str(r#"{"enforce":true,"dynamic_groups":false,"dynamic_groups_type":"all","levels":[{"aliases":["OPEN"],"css":{"color":"default"},"description":"N/A","lvl":1,"name":"LEVEL 0","short_name":"L0"},{"aliases":[],"css":{"color":"default"},"description":"N/A","lvl":5,"name":"LEVEL 1","short_name":"L1"},{"aliases":[],"css":{"color":"default"},"description":"N/A","lvl":15,"name":"LEVEL 2","short_name":"L2"}],"required":[{"aliases":["LEGAL"],"description":"N/A","name":"LEGAL DEPARTMENT","short_name":"LE","require_lvl":null,"is_required_group":false},{"aliases":["ACC"],"description":"N/A","name":"ACCOUNTING","short_name":"AC","require_lvl":null,"is_required_group":false},{"aliases":[],"description":"N/A","name":"ORIGINATOR CONTROLLED","short_name":"ORCON","require_lvl":null,"is_required_group":true},{"aliases":[],"description":"N/A","name":"NO CONTRACTOR ACCESS","short_name":"NOCON","require_lvl":null,"is_required_group":true}],"groups":[{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP A","short_name":"A","solitary_display_name":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP B","short_name":"B","solitary_display_name":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP X","short_name":"X","solitary_display_name":"XX"}],"subgroups":[{"aliases":["R0"],"auto_select":false,"description":"N/A","name":"RESERVE ONE","short_name":"R1","require_group":null,"limited_to_group":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"RESERVE TWO","short_name":"R2","require_group":"X","limited_to_group":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"RESERVE THREE","short_name":"R3","require_group":null,"limited_to_group":"X"}],"restricted":"L2","unrestricted":"L0"}"#).unwrap()).unwrap());
        crate::types::classification::set_global_classification(parser.clone());
        parser
    }

    #[derive(Described, Serialize, Deserialize)]
    #[metadata_type(ElasticMeta)]
    struct SubObject {
        classification: ClassificationString,
    }

    #[derive(Described, Serialize, Deserialize)]
    #[metadata_type(ElasticMeta)]
    struct TestObject {
        #[serde(flatten)]
        classification: ExpandingClassification,
        other_data: ClassificationString,
        expiry_ts: DateTime<Utc>,
        data: SubObject,
    }

    // Test that the classification components get expanded as expected
    #[test]
    fn classification_serialize() {
        let parser = setup_classification();
        let time = DateTime::parse_from_rfc3339("2001-05-01T01:59:59.001Z").unwrap();
        let sample = TestObject {
            classification: ExpandingClassification::new("L0//LE//REL    A".to_owned(), &parser).unwrap(),
            other_data: ClassificationString::new("L2//AC//REL          B".to_owned(), &parser).unwrap(),
            expiry_ts: time.into(),
            data: SubObject { 
                classification: ClassificationString::new("L1//LE   ".to_owned(), &parser).unwrap(),
            },
        };
        assert_eq!(serde_json::to_value(sample).unwrap(), json!({
            "classification": "L0//LE//REL A",
            "__access_lvl__": 1,
            "__access_req__": ["LE"],
            "__access_grp1__": ["A"],
            "__access_grp2__": ["__EMPTY__"],
            "other_data": "L2//AC//REL B",
            "expiry_ts": "2001-05-01T01:59:59.001Z",
            "data": {
                "classification": "L1//LE"
            }
        }));
    }
}