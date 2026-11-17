#[cfg(feature = "rand")]
use rand::{Rng, RngExt as _};
use serde::{Deserialize, Serialize};

#[cfg(feature = "rand")]
use crate::datastore::Service;


/// Heuristic model as stored in service manifest
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Heuristic {
    /// Description of the heuristic
    #[serde(default)]
    pub description: String,
    /// What type of files does this heuristic target?
    pub filetype: String,
    /// ID of the Heuristic
    pub heur_id: u32,
    /// Name of the heuristic
    pub name: String,
    /// Default score of the heuristic
    pub score: i32,
}

/// Service Manifest Model
/// Contains information stored in the service_manifest.yml
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServiceManifest {
    /// service body
    #[serde(flatten)]
    pub service: Service,
    /// version of the service tool
    #[serde(default)]
    pub tool_version: Option<String>,
    /// whether or not the file is required for analysis
    #[serde(default = "default_file_required")]
    pub file_required: bool,
    /// list of heuristics
    #[serde(default)]
    pub heuristics: Vec<Heuristic>,
}

fn default_file_required() -> bool {
    true
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<ServiceManifest> for rand::distr::StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ServiceManifest {
        // let num = rng.random::<u8>().as_char();
        let service: Service = rng.random();
        let random_tool_version = format!("v{}", rng.random_range(0..10));

        let heuristics = vec![
            Heuristic {
                description: "test heuristic 1".to_owned(),
                filetype: "txt".to_owned(),
                heur_id: 1,
                name: "heu1".to_owned(),
                score: -100,
            },
            Heuristic {
                description: "test heuristic 2".to_owned(),
                filetype: "txt".to_owned(),
                heur_id: 2,
                name: "heu2".to_owned(),
                score: 1000,
            },
        ];

        ServiceManifest {
            service,
            tool_version: Some(random_tool_version),
            file_required: true,
            heuristics,
        }
    }
}
