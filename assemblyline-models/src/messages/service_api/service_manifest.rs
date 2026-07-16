#[cfg(feature = "rand")]
use std::u32;

#[cfg(feature = "rand")]
use rand::Rng;
use serde::{Deserialize, Serialize};

#[cfg(feature = "rand")]
use crate::datastore::Service;

/// Service Manifest Model
/// Contains information stored in the service_manifest.yml
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Heuristic {
    #[serde(default)]
    description: String,
    filetype: String,
    heur_id: u32,
    name: String,
    score: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServiceManifest {
    #[serde(flatten)]
    pub service: Service,

    #[serde(default)]
    pub tool_version: Option<String>,

    #[serde(default = "default_file_required")]
    pub file_required: bool,

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
                score: 100,
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
