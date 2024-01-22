
pub mod submission;
pub mod error;
pub mod alert;
pub mod workflow;
pub mod result;
pub mod tagging;
pub mod file;
pub mod service;
pub mod retrohunt;

pub use submission::Submission;
pub use error::Error;
pub use alert::Alert;
pub use workflow::Workflow;
pub use result::Result;
pub use tagging::Tagging;
pub use file::File;
pub use service::Service;
pub use retrohunt::{Retrohunt, RetrohuntHit};


// #[cfg(test)]
// mod python {
//     use serde_json::json;

//     use crate::meta::Mappings;

//     use super::{super::meta::build_mapping, Submission};
//     use pretty_assertions::assert_eq;

//     #[test]
//     fn submission() {
//         let output = std::process::Command::new("python").arg("-c").arg("from assemblyline.datastore.support.build import build_mapping; from assemblyline.odm.models.submission import Submission; import json; print(json.dumps(build_mapping(Submission.fields().values())))").output().unwrap();
//         let stderr = String::from_utf8(output.stderr).unwrap();
//         let stdout = String::from_utf8(output.stdout).unwrap();
//         if !output.status.success() || !stderr.is_empty() {
//             println!("{stderr}");
//             panic!();
//         }

//         let (properties, dynamic_templates): (serde_json::Value, serde_json::Value) = serde_json::from_str(&stdout).unwrap();
//         let py_mapping = json!({
//             "properties": properties,
//             "dynamic_templates": dynamic_templates,
//         });
//         let py_mapping: Mappings = serde_json::from_value(py_mapping).unwrap();


//         println!("{}", serde_json::to_string_pretty(&properties).unwrap());
//         println!("{}", serde_json::to_string_pretty(&dynamic_templates).unwrap());

//         let mapping = build_mapping::<Submission>().unwrap();
//         assert_eq!(mapping, py_mapping);
//         todo!();
//         // let rs_properties: serde_json::Value = rs_properties.into(); 
//         // let rs_dynamic_templates: serde_json::Value = rs_dynamic_templates.into(); 

//         // assert_eq!(properties, rs_properties);
//         // assert_eq!(dynamic_templates, rs_dynamic_templates);
//     }

// }


#[cfg(test)]
mod python {

    use serde_json::json;

    use super::{RetrohuntHit, Retrohunt, File};
    use crate::meta::Mappings;

    use crate::meta::build_mapping;
    use pretty_assertions::assert_eq;

    fn load_mapping(module: &str, name: &str) -> Mappings {
        let output = std::process::Command::new("python").arg("-c").arg("from assemblyline.datastore.support.build import build_mapping; from assemblyline.odm.models. ".to_owned() + module + " import " + name + "; import json; print(json.dumps(build_mapping(" + name + ".fields().values())))").output().unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        if !output.status.success() || !stderr.is_empty() {
            println!("{stderr}");
            panic!();
        }

        let (properties, dynamic_templates): (serde_json::Value, serde_json::Value) = serde_json::from_str(&stdout).unwrap();
        let py_mapping = json!({
            "properties": properties,
            "dynamic_templates": dynamic_templates,
        });
        serde_json::from_value(py_mapping).unwrap()
    }

    #[test]
    fn retrohunt_schema() {
        let py_mappings = load_mapping("retrohunt", "Retrohunt");
        let mapping = build_mapping::<Retrohunt>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn retrohunt_hits_schema() {
        let py_mappings = load_mapping("retrohunt", "RetrohuntHit");
        let mapping = build_mapping::<RetrohuntHit>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn file_schema() {
        let mut py_mappings = load_mapping("file", "File");
        py_mappings.properties.remove("archive_ts");
        let mapping = build_mapping::<File>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    // // Test that the classification components get expanded as expected
    // #[test]
    // fn classification_serialize() {
    //     let time = DateTime::parse_from_rfc3339("2001-05-01T01:59:59Z").unwrap();
    //     let sample = RetrohuntHit {
    //         classification: "",
    //         sha256: Sha256::from_str("00000000000000000000000000000000").unwrap(),
    //         expiry_ts: Some(time.into()),
    //         search: "abc123".to_owned(),
    //     };
    //     assert_eq!(crate::serialize::to_string(&sample).unwrap(), "");
    // }

}