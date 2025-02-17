use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;
use strum::IntoEnumIterator;

use crate::{ElasticMeta, Email, ExpandingClassification, Readable, UpperString};



#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, strum::FromRepr, Described, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "snake_case")]
pub enum UserType {
    /// Perform administartive task and has access to all roles
    Admin = 0,
    /// Normal user of the system
    User = 1,
    /// Super user that also has access to roles for managing signatures in the system
    SignatureManager = 2,
    /// Has access to roles for importing signatures in the system
    SignatureImporter = 3,
    /// User that can only view the data
    Viewer = 4,
    /// User that can only start submissions
    Submitter = 5,
    /// Has custom roles selected
    Custom = 6,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum Scope {R, W, RW, C}


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, strum::FromRepr, strum::EnumIter, Described, Clone, Copy, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "snake_case")]
pub enum UserRole {
    /// Modify labels, priority, status, verdict or owner of alerts
    AlertManage = 0,
    /// View alerts in the system
    AlertView = 1,
    /// Allow access via API keys
    ApikeyAccess = 2,
    /// Create bundle of a submission
    BundleDownload = 3,
    /// View files in the file viewer
    FileDetail = 4,
    /// Download files from the system
    FileDownload = 5,
    /// Purge files from the filestore
    FilePurge = 33,
    /// View heuristics of the system
    HeuristicView = 6,
    /// Allow access via On Behalf Off tokens
    OboAccess = 7,
    /// Allow submission to be replayed on another server
    ReplayTrigger = 8,
    /// View safelist items
    SafelistView = 9,
    /// Manage (add/delete) safelist items
    SafelistManage = 10,
    /// Download signatures from the system
    SignatureDownload = 11,
    /// View signatures
    SignatureView = 12,
    /// Create a submission in the system
    SubmissionCreate = 13,
    /// Delete submission from the system
    SubmissionDelete = 14,
    /// Set user verdict on submissions
    SubmissionManage = 15,
    /// View submission's results
    SubmissionView = 16,
    /// Manage (add/delete) workflows
    WorkflowManage = 17,
    /// View workflows
    WorkflowView = 18,
    /// Perform administrative tasks
    Administration = 19,
    /// Manage status of file/submission/alerts during the replay process
    ReplaySystem = 20,
    /// Import signatures in the system
    SignatureImport = 21,
    /// Manage signatures sources in the system
    SignatureManage = 22,
    /// View archived data in the system
    ArchiveView = 23,
    /// Modify attributes of archived Submissions/Files/Results
    ArchiveManage = 24,
    /// Send Submission, files and results to the archive
    ArchiveTrigger = 25,
    /// Download file from the archive
    ArchiveDownload = 26,
    /// Manage currently logged in user settings
    SelfManage = 27,
    /// View yara searches
    RetrohuntView = 28,
    /// Run yara searches
    RetrohuntRun = 29,
    /// Allow federated searches against external systems
    ExternalQuery = 30,
    /// View badlist items
    BadlistView = 31,
    /// Manage (add/delete) badlist items
    BadlistManage = 32,
    /// Comment on archived files
    ArchiveComment = 35, // is 33 in python, collides with FilePurge
    /// Use the Assemblyline Assistant
    AssistantUse = 34,
}

const USER_ROLES_BASIC: [UserRole; 30] = [
    UserRole::AlertManage,
    UserRole::AlertView,
    UserRole::ArchiveTrigger,
    UserRole::ArchiveView,
    UserRole::ArchiveManage,
    UserRole::ArchiveDownload,
    UserRole::ArchiveComment,
    UserRole::ApikeyAccess,
    UserRole::BundleDownload,
    UserRole::ExternalQuery,
    UserRole::FileDetail,
    UserRole::FileDownload,
    UserRole::HeuristicView,
    UserRole::OboAccess,
    UserRole::ReplayTrigger,
    UserRole::SafelistView,
    UserRole::SafelistManage,
    UserRole::SelfManage,
    UserRole::SignatureDownload,
    UserRole::SignatureView,
    UserRole::SubmissionCreate,
    UserRole::SubmissionDelete,
    UserRole::SubmissionManage,
    UserRole::SubmissionView,
    UserRole::WorkflowManage,
    UserRole::WorkflowView,
    UserRole::RetrohuntView,
    UserRole::RetrohuntRun,
    UserRole::BadlistView,
    UserRole::BadlistManage,
];

// USER_ROLES = USER_ROLES_BASIC.union({
//     ROLES.administration,      # 
//     ROLES.file_purge,          # 
//     ROLES.replay_system,       # 
//     ROLES.signature_import,    # 
//     ROLES.signature_manage,    # 
//     ROLES.assistant_use,       # 
// })

impl UserType {
    #[must_use]
    pub fn roles(&self) -> Vec<UserRole> {
        match self {
            UserType::Admin => UserRole::iter().collect(),
            UserType::SignatureImporter => vec![
                UserRole::BadlistManage,
                UserRole::SafelistManage,
                UserRole::SelfManage,
                UserRole::SignatureDownload,
                UserRole::SignatureImport,
                UserRole::SignatureView
            ],
            UserType::SignatureManager => {
                let mut roles: Vec<_> = UserRole::iter().collect();
                roles.push(UserRole::SignatureManage);
                roles
            },
            UserType::User => USER_ROLES_BASIC.into_iter().collect(),
            UserType::Viewer => vec![
                UserRole::AlertView,
                UserRole::ApikeyAccess,
                UserRole::BadlistView,
                UserRole::FileDetail,
                UserRole::OboAccess,
                UserRole::HeuristicView,
                UserRole::SafelistView,
                UserRole::SelfManage,
                UserRole::SignatureView,
                UserRole::SubmissionView,
                UserRole::WorkflowView,
            ],
            UserType::Submitter => vec![
                UserRole::ApikeyAccess,
                UserRole::OboAccess,
                UserRole::SelfManage,
                UserRole::SubmissionCreate,
                UserRole::ReplayTrigger,
                UserRole::RetrohuntRun,
            ],
            // custom is not a hardcoded list
            UserType::Custom => vec![],
        }
    }
}


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, Clone, Copy)]
#[metadata_type(ElasticMeta)]
pub enum AclCatagory {R, W, E, C}

impl AclCatagory {
    #[must_use]
    pub fn roles(&self) -> &[UserRole] {
        match self {
            AclCatagory::R => &[
                UserRole::AlertView,
                UserRole::ArchiveView,
                UserRole::ArchiveDownload,
                UserRole::BadlistView,
                UserRole::BundleDownload,
                UserRole::ExternalQuery,
                UserRole::FileDetail,
                UserRole::FileDownload,
                UserRole::HeuristicView,
                UserRole::SafelistView,
                UserRole::SignatureDownload,
                UserRole::SignatureView,
                UserRole::SubmissionView,
                UserRole::WorkflowView,
                UserRole::RetrohuntView,
            ],
            AclCatagory::W => &[
                UserRole::AlertManage,
                UserRole::ArchiveTrigger,
                UserRole::ArchiveManage,
                UserRole::BadlistManage,
                UserRole::ReplayTrigger,
                UserRole::SafelistManage,
                UserRole::SubmissionCreate,
                UserRole::SubmissionDelete,
                UserRole::SubmissionManage,
                UserRole::RetrohuntRun,
            ],
            AclCatagory::E => &[
                UserRole::Administration,
                UserRole::ApikeyAccess,
                UserRole::FilePurge,
                UserRole::OboAccess,
                UserRole::ReplaySystem,
                UserRole::SelfManage,
                UserRole::SignatureImport,
                UserRole::SignatureManage,
                UserRole::WorkflowManage
            ],
            AclCatagory::C => &[],
        }
    }
}


// def load_roles_form_acls(acls, curRoles):
//     # Check if we have current roles first
//     if curRoles:
//         return curRoles

//     # Otherwise load the roles from the api_key ACLs
//     roles = set({})
//     for acl in ACL_MAP.keys():
//         if acl in acls:
//             roles = roles.union(ACL_MAP[acl])

//     # Return roles as a list
//     return list(roles)


// def load_roles(types, curRoles):
//     # Check if we have current roles first
//     if curRoles:
//         return curRoles

//     # Otherwise load the roles from the user type
//     roles = set({})
//     for user_type in USER_TYPE_DEP.keys():
//         if user_type in types:
//             roles = roles.union(USER_TYPE_DEP[user_type])

//     # Return roles as a list
//     return list(roles)

/// Model for API keys
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct ApiKey {
    /// Access Control List for the API key
    pub acl: Vec<AclCatagory>,
    /// BCrypt hash of the password for the apikey
    pub password: String,
    /// List of roles tied to the API key
    #[serde(default)]
    pub roles: Vec<UserRole>,
}

/// Model of Apps used of OBO (On Behalf Of)
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct Apps {
    /// Username allowed to impersonate the current user
    pub client_id: String,
    /// DNS hostname for the server
    pub netloc: String,
    /// Scope of access for the App token
    pub scope: Scope,
    /// Name of the server that has access
    pub server: String,
    /// List of roles tied to the App token
    #[serde(default)]
    pub roles: Vec<UserRole>,
}

/// Model of User
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct User {
    /// Date the user agree with terms of service
    #[metadata(index=false, store=false)]
    pub agrees_with_tos: Option<DateTime<Utc>>,
    /// Maximum number of concurrent API requests (0: No Quota)
    #[metadata(store=false, mapping="integer")]
    #[serde(default)]
    pub api_quota: Option<u64>,
    /// Maximum number of API calls a user can do daily (0: No Quota)
    #[metadata(store=false, mapping="integer")]
    #[serde(default)]
    pub api_daily_quota: Option<u64>,
    /// Mapping of API keys
    #[metadata(index=false, store=false)]
    #[serde(default)]
    pub apikeys: HashMap<String, ApiKey>,
    /// Applications with access to the account
    #[metadata(index=false, store=false)]
    #[serde(default)]
    pub apps: HashMap<String, Apps>,
    /// Allowed to query on behalf of others?
    #[metadata(index=false, store=false)]
    #[serde(default)]
    pub can_impersonate: bool,
    /// Maximum classification for the user
    #[metadata(copyto="__text__")]
    #[serde(flatten, default="unrestricted_expanding_classification")]
    pub classification: ExpandingClassification<true>,
    /// User's LDAP DN
    #[metadata(store=false, copyto="__text__")]
    #[serde(default)]
    pub dn: Option<String>,
    /// User's email address
    #[metadata(copyto="__text__")]
    #[serde(default)]
    pub email: Option<Email>,
    /// List of groups the user submits to
    #[metadata(copyto="__text__")]
    #[serde(default)]
    pub groups: Vec<UpperString>,
    /// ID of the matching object in your identity provider (used for logging in as another application)
    #[metadata(copyto="__text__", store=false)]
    #[serde(default)]
    identity_id: Option<String>,
    /// Is the user active?
    #[serde(default="default_user_is_active")]
    pub is_active: bool,
    /// Full name of the user
    #[metadata(copyto="__text__")]
    pub name: String,
    /// Secret key to generate one time passwords
    #[metadata(index=false, store=false)]    
    #[serde(default)]
    pub otp_sk: Option<String>,
    /// BCrypt hash of the user's password
    #[metadata(index=false, store=false)]
    pub password: String,
    /// Maximum number of concurrent submissions (0: No Quota)
    #[metadata(store=false, mapping="integer")]
    #[serde(default)]
    pub submission_quota: Option<u64>,
    /// Maximum number of concurrent async submission (0: No Quota)
    #[metadata(store=false, mapping="integer")]
    #[serde(default)]
    pub submission_async_quota: Option<u64>,
    /// Maximum number of submissions a user can do daily (0: No Quota)
    #[metadata(store=false, mapping="integer")]
    #[serde(default)]
    pub submission_daily_quota: Option<u64>,
    /// Type of user
    #[serde(rename="type", default="default_user_types")]
    pub user_types: Vec<UserType>,
    /// Default roles for user
    #[serde(default)]
    pub roles: Vec<UserRole>,
    /// Map of security tokens
    #[metadata(index=false, store=false)]
    #[serde(default)]
    pub security_tokens: HashMap<String, String>,
    /// Username
    #[metadata(copyto="__text__")]
    pub uname: String,
}

fn default_user_types() -> Vec<UserType> { vec![UserType::User] }
fn default_user_is_active() -> bool { true }

impl Readable for User {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}

impl Default for User {
    fn default() -> Self {
        User {
            agrees_with_tos: None,
            api_quota: None,
            api_daily_quota: None,
            apikeys: Default::default(),
            apps: Default::default(),
            can_impersonate: false,
            classification: ExpandingClassification::try_unrestricted().unwrap(),
            dn: None,
            email: None,
            groups: Default::default(),
            identity_id: None,
            is_active: default_user_is_active(),
            name: "User".to_owned(),
            otp_sk: None,
            password: Default::default(),
            submission_quota: None,
            submission_async_quota: None,
            submission_daily_quota: None,
            user_types: default_user_types(),
            roles: Default::default(),
            security_tokens: Default::default(),
            uname: "user".to_owned(),
        }
    }
}


// #[test]
// fn sample_admin_user() {
//     let data = r#"{
//         "agrees_with_tos": "2025-01-30T19:24:57.559049Z", 
//         "api_quota": null, 
//         "api_daily_quota": null, 
//         "apikeys": {}, 
//         "apps": {}, 
//         "can_impersonate": false, 
//         "classification": "", 
//         "dn": null, 
//         "email": null, 
//         "groups": [], 
//         "identity_id": null, 
//         "is_active": true, 
//         "name": "Administrator", 
//         "otp_sk": null, 
//         "password": "password", 
//         "submission_quota": null, 
//         "submission_async_quota": null, 
//         "submission_daily_quota": null, 
//         "type": ["admin", "user", "signature_importer"], 
//         "roles": [], 
//         "security_tokens": {}, 
//         "uname": "admin"
//     }"#;
//     let _user: User = serde_json::from_str(&data).unwrap();
// }