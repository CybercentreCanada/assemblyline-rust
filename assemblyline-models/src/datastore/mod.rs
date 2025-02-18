
pub mod submission;
pub mod error;
pub mod alert;
pub mod workflow;
pub mod result;
pub mod tagging;
pub mod file;
pub mod service;
pub mod service_delta;
pub mod retrohunt;
pub mod user;
pub mod filescore;
pub mod emptyresult;
pub mod badlist;
pub mod heuristic;
pub mod safelist;

pub use submission::Submission;
pub use error::Error;
pub use alert::Alert;
pub use workflow::Workflow;
pub use result::Result;
pub use tagging::Tagging;
pub use file::File;
pub use service::Service;
pub use service_delta::ServiceDelta;
pub use retrohunt::{Retrohunt, RetrohuntHit};
pub use emptyresult::EmptyResult;


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

    use super::{RetrohuntHit, File};
    use crate::datastore::badlist::Badlist;
    use crate::datastore::safelist::Safelist;
    use crate::datastore::user::User;
    use crate::datastore::{Retrohunt, Service, ServiceDelta, Tagging, Workflow};
    use crate::datastore::{Alert, EmptyResult, Error, Result, Submission, filescore::FileScore, heuristic::Heuristic};
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
            "dynamic": true,
            "properties": properties,
            "dynamic_templates": dynamic_templates,
        });
        serde_json::from_value(py_mapping).unwrap()
    }

    #[test]
    fn alert_schema() {
        let py_mappings = load_mapping("alert", "Alert");
        let mapping = build_mapping::<Alert>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    
    #[test]
    fn badlist_schema() {
        let py_mappings = load_mapping("badlist", "Badlist");
        // py_mappings.properties.remove("archive_ts");
        let mapping = build_mapping::<Badlist>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn emptyresult_schema() {
        let py_mappings = load_mapping("emptyresult", "EmptyResult");
        let mapping = build_mapping::<EmptyResult>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn error_schema() {
        let py_mappings = load_mapping("error", "Error");
        let mapping = build_mapping::<Error>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn file_schema() {
        let py_mappings = load_mapping("file", "File");
        let mapping = build_mapping::<File>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn filescore_schema() {
        let py_mappings = load_mapping("filescore", "FileScore");
        let mapping = build_mapping::<FileScore>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn heuristic_schema() {
        let py_mappings = load_mapping("heuristic", "Heuristic");
        let mapping = build_mapping::<Heuristic>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn result_schema() {
        let py_mappings = load_mapping("result", "Result");
        let mapping = build_mapping::<Result>().unwrap();
        assert_eq!(mapping, py_mappings);
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
    fn safelist_schema() {
        let py_mappings = load_mapping("safelist", "Safelist");
        let mapping = build_mapping::<Safelist>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn service_delta_schema() {
        let py_mappings = load_mapping("service_delta", "ServiceDelta");
        let mapping = build_mapping::<ServiceDelta>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn service_schema() {
        let py_mappings = load_mapping("service", "Service");
        let mapping = build_mapping::<Service>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn submission_schema() {
        let mut py_mappings = load_mapping("submission", "Submission");
        py_mappings.properties.remove("params.ignore_dynamic_recursion_prevention");
        let mapping = build_mapping::<Submission>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn tagging_schema() {
        let py_mappings = load_mapping("tagging", "Tagging");
        let mapping = build_mapping::<Tagging>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn user_schema() {
        let py_mappings = load_mapping("user", "User");
        // py_mappings.properties.remove("archive_ts");
        let mapping = build_mapping::<User>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn workflow_schema() {
        let py_mappings = load_mapping("workflow", "Workflow");
        // py_mappings.properties.remove("archive_ts");
        let mapping = build_mapping::<Workflow>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

}