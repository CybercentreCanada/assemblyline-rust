use std::collections::HashMap;

use serde::Deserialize;

const ATTACK_MAP_RAW: &str = include_str!("./attack_map.json");

#[derive(Deserialize)]
pub struct Attack {
    pub name: String,
    pub categories: Vec<String>,
    pub description: String,
    pub platforms: Vec<String>,
    pub attack_id: String,
}

#[derive(Deserialize)]
pub struct Software {
    pub name: String,
    pub description: String,
    pub platforms: Vec<String>,
    pub software_id: String,
    #[serde(rename="type")]
    pub type_: String,
    pub attack_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct Group {
    pub name: String,
    pub description: String,
    pub group_id: String,
}

#[derive(Deserialize)]
pub struct AttackMapFile {
    pub attack_map: HashMap<String, Attack>,
    pub software_map: HashMap<String, Software>,
    pub group_map: HashMap<String, Group>,
    pub revoke_map: HashMap<String, String>,
}

pub fn load_attack_map() -> Result<&'static AttackMapFile, &'static serde_json::Error> {
    static PARSED_ATTACK_MAP: std::sync::OnceLock<Result<AttackMapFile, serde_json::Error>> = std::sync::OnceLock::new();
    PARSED_ATTACK_MAP.get_or_init(|| {
        serde_json::from_str(ATTACK_MAP_RAW)
    }).as_ref()
}