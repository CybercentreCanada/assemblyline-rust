pub mod file;
pub mod search;
pub mod submit;

use std::collections::HashMap;

use serde::Serialize;
use crate::JsonMap;

macro_rules! api_path {
    ($component:expr) => {
        format!("api/v4/{}", $component)
    };
    ($component:expr, $($part:expr),+) => {
        {
            let mut root = format!("api/v4/{}", $component);
            api_path!(add_parts root, $($part),+)
        }
    };
    (add_parts $builder:ident) => (
        $builder
    );
    (add_parts $builder:ident, $part:expr) => {
        {
            $builder.push('/');
            $builder.push_str(&$part);
            $builder
        }
    };
    (add_parts $builder:ident, $part:expr, $($tail:expr),*) => {
        {
            $builder.push('/');
            $builder.push_str(&$part);
            api_path!(add_parts $builder, $($tail),*)
        }
    };
}

pub (crate) use api_path;

/// Submission Parameters
#[derive(Serialize)]
pub struct Params {
    /// classification of the submission
    pub classification: Option<String>,
    /// Should a deep scan be performed?
    pub deep_scan: bool,
    /// Description of the submission
    pub description: Option<String>,
    /// Should this submission generate an alert?
    pub generate_alert: bool,
    /// List of groups related to this scan
    pub groups: Vec<String>,
    /// Ignore the cached service results?
    pub ignore_cache: bool,
    /// Should we ignore dynamic recursion prevention?
    pub ignore_dynamic_recursion_prevention: bool,
    /// Should we ignore filtering services?
    pub ignore_filtering: bool,
    /// Ignore the file size limits?
    pub ignore_size: bool,
    /// Exempt from being dropped by ingester?
    pub never_drop: bool,
    /// Is the file submitted already known to be malicious?
    pub malicious: bool,
    /// Max number of extracted files
    pub max_extracted: u32,
    /// Max number of supplementary files
    pub max_supplementary: u32,
    /// Priority of the scan
    pub priority: u16,
    /// Should the submission do extra profiling?
    pub profile: bool,
    /// Service selection
    pub services: ServiceSelection,
    /// Service-specific parameters
    pub service_spec: HashMap<String, JsonMap>,
    /// Time, in days, to live for this submission
    pub ttl: u32,
    /// Type of submission
    #[serde(rename="type")]
    pub submission_type: String,
    /// Initialization for temporary submission data
    pub initial_data: Option<String>,
    /// Does the submission automatically goes into the archive when completed?
    pub auto_archive: bool,
    /// When the submission is archived, should we delete it from hot storage right away?
    pub delete_after_archive: bool
}

impl Default for Params {
    fn default() -> Self {
        Self {
            classification: None,
            deep_scan: false,
            description: None,
            generate_alert: false,
            groups: vec![],
            ignore_cache: false,
            ignore_dynamic_recursion_prevention: false,
            ignore_filtering: false,
            ignore_size: false,
            never_drop: false,
            malicious: false,
            max_extracted: 100,
            max_supplementary: 100,
            priority: 100,
            profile: false,
            services: Default::default(),
            service_spec: Default::default(),
            ttl: 30,
            submission_type: "USER".to_owned(),
            initial_data: None,
            auto_archive: false,
            delete_after_archive: false,
        }
    }
}


/// Service Selection Scheme
#[derive(Serialize, Default)]
pub struct ServiceSelection {
    /// List of selected services
    pub selected: Option<Vec<String>>,
    /// List of excluded services
    pub excluded: Option<Vec<String>>,
    /// List of services to rescan when moving between systems
    pub rescan: Option<Vec<String>>,
    /// Add to service selection when resubmitting
    pub resubmit: Option<Vec<String>>,
}
