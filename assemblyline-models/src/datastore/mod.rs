
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