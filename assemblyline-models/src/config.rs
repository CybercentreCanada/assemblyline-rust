use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};


/// Named Value
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct NamedValue {
    /// Name
    pub name: String,
    /// Value
    pub value: String
}

/// Webhook Configuration
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct Webhook {
    /// Password used to authenticate with source
    #[serde(default)]
    pub password: Option<String>,
    /// CA cert for source
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Ignore SSL errors when reaching out to source?
    #[serde(default)]
    pub ssl_ignore_errors: bool,
    #[serde(default)]
    pub ssl_ignore_hostname: bool,
    /// Proxy server for source
    #[serde(default)]
    pub proxy: Option<String>,
    /// HTTP method used to access webhook
    #[serde(default="default_webhook_method")]
    pub method: String,
    /// URI to source
    pub uri: String,
    /// Username used to authenticate with source
    #[serde(default)]
    pub username: Option<String>,
    /// Headers
    #[serde(default)]
    pub headers: Vec<NamedValue>,
    /// Number of attempts to connect to webhook endpoint
    #[serde(default="default_webhook_retries")]
    pub retries: Option<u32>,
}

fn default_webhook_method() -> String { "POST".to_string() }
fn default_webhook_retries() -> Option<u32> { Some(3) }

/// Resubmission Options
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ResubmitOptions {
    pub additional_services: Vec<String>,
    pub random_below: Option<i32>,
}

/// Postprocessing Action
#[derive(Debug, Serialize, Deserialize)]
pub struct PostprocessAction {
    /// Is this action active
    #[serde(default)]
    pub enabled: bool,
    /// Should this action run on cache hits
    #[serde(default)]
    pub run_on_cache: bool,
    /// Should this action run on newly completed submissions
    #[serde(default)]
    pub run_on_completed: bool,
    /// Query string to select submissions
    pub filter: String,
    /// Webhook action configuration
    #[serde(default)]
    pub webhook: Option<Webhook>,
    /// Raise an alert when this action is triggered
    #[serde(default)]
    pub raise_alert: bool,
    /// Resubmission configuration
    #[serde(default)]
    pub resubmit: Option<ResubmitOptions>,
    /// Archive the submission when this action is triggered
    #[serde(default)]
    pub archive_submission: bool,
}

impl PostprocessAction {
    pub fn new(filter: String) -> Self {
        Self {
            enabled: Default::default(),
            run_on_cache: Default::default(),
            run_on_completed: Default::default(),
            filter,
            webhook: Default::default(),
            raise_alert: Default::default(),
            resubmit: Default::default(),
            archive_submission: Default::default(),
        }
    }

    pub fn enable(mut self) -> Self {
        self.enabled = true; self
    }

    pub fn alert(mut self) -> Self {
        self.raise_alert = true; self
    }

    pub fn on_completed(mut self) -> Self {
        self.run_on_completed = true; self
    }
}

pub fn default_postprocess_actions() -> HashMap<String, PostprocessAction> {
    // Raise alerts for all submissions over 500, both on cache hits and submission complete
    [("default_alerts".to_string(), PostprocessAction{
        enabled: true,
        run_on_cache: true,
        run_on_completed: true,
        filter: "max_score: >=500".to_string(),
        webhook: None,
        raise_alert: true,
        resubmit: None,
        archive_submission: false
    }),
    // Resubmit submissions on completion. All submissions with score >= 0 are elegable, but sampling
    // is applied to scores below 500
    ("default_resubmit".to_string(), PostprocessAction{
        enabled: true,
        run_on_cache: true,
        run_on_completed: true,
        filter: "max_score: >=0".to_string(),
        webhook: None,
        raise_alert: false,
        resubmit: Some(ResubmitOptions{
            additional_services: vec![],
            random_below: Some(500)
        }),
        archive_submission: false
    })].into_iter().collect()
}

// from typing import Dict, List

// from assemblyline import odm
// from assemblyline.odm.models.service import EnvironmentVariable
// from assemblyline.odm.models.service_delta import DockerConfigDelta


// AUTO_PROPERTY_TYPE = ['access', 'classification', 'type', 'role', 'remove_role', 'group']
// DEFAULT_EMAIL_FIELDS = ['email', 'emails', 'extension_selectedEmailAddress', 'otherMails', 'preferred_username', 'upn']


// @odm.model(index=False, store=False, description="Password Requirement")
// class PasswordRequirement(odm.Model):
//     lower: bool = odm.Boolean(description="Password must contain lowercase letters")
//     number: bool = odm.Boolean(description="Password must contain numbers")
//     special: bool = odm.Boolean(description="Password must contain special characters")
//     upper: bool = odm.Boolean(description="Password must contain uppercase letters")
//     min_length: int = odm.Integer(description="Minimum password length")


// DEFAULT_PASSWORD_REQUIREMENTS = {
//     "lower": False,
//     "number": False,
//     "special": False,
//     "upper": False,
//     "min_length": 12
// }


// @odm.model(index=False, store=False,
//            description="Configuration block for [GC Notify](https://notification.canada.ca/) signup and password reset")
// class Notify(odm.Model):
//     base_url: str = odm.Optional(odm.Keyword(), description="Base URL")
//     api_key: str = odm.Optional(odm.Keyword(), description="API key")
//     registration_template: str = odm.Optional(odm.Keyword(), description="Registration template")
//     password_reset_template: str = odm.Optional(odm.Keyword(), description="Password reset template")
//     authorization_template: str = odm.Optional(odm.Keyword(), description="Authorization template")
//     activated_template: str = odm.Optional(odm.Keyword(), description="Activated Template")


// DEFAULT_NOTIFY = {
//     "base_url": None,
//     "api_key": None,
//     "registration_template": None,
//     "password_reset_template": None,
//     "authorization_template": None,
//     "activated_template": None,
// }


// @odm.model(index=False, store=False, description="Configuration block for SMTP signup and password reset")
// class SMTP(odm.Model):
//     from_adr: str = odm.Optional(odm.Keyword(), description="Email address used for sender")
//     host: str = odm.Optional(odm.Keyword(), description="SMTP host")
//     password: str = odm.Optional(odm.Keyword(), description="Password for SMTP server")
//     port: int = odm.Integer(description="Port of SMTP server")
//     tls: bool = odm.Boolean(description="Should we communicate with SMTP server via TLS?")
//     user: str = odm.Optional(odm.Keyword(), description="User to authenticate to the SMTP server")


// DEFAULT_SMTP = {
//     "from_adr": None,
//     "host": None,
//     "password": None,
//     "port": 587,
//     "tls": True,
//     "user": None
// }


// @odm.model(index=False, store=False, description="Signup Configuration")
// class Signup(odm.Model):
//     enabled: bool = odm.Boolean(description="Can a user automatically signup for the system")
//     smtp: SMTP = odm.Compound(SMTP, default=DEFAULT_SMTP, description="Signup via SMTP")
//     notify: Notify = odm.Compound(Notify, default=DEFAULT_NOTIFY, description="Signup via GC Notify")
//     valid_email_patterns: List[str] = odm.List(
//         odm.Keyword(),
//         description="Email patterns that will be allowed to automatically signup for an account")


// DEFAULT_SIGNUP = {
//     "enabled": False,
//     "notify": DEFAULT_NOTIFY,
//     "smtp": DEFAULT_SMTP,
//     "valid_email_patterns": [".*", ".*@localhost"]
// }


// @odm.model(index=False, store=False)
// class AutoProperty(odm.Model):
//     field: str = odm.Keyword(description="Field to apply `pattern` to")
//     pattern: str = odm.Keyword(description="Regex pattern for auto-prop assignment")
//     type: str = odm.Enum(AUTO_PROPERTY_TYPE, description="Type of property assignment on pattern match")
//     value: List[str] = odm.List(odm.Keyword(), auto=True, default=[], description="Assigned property value")


// @odm.model(index=False, store=False, description="LDAP Configuration")
// class LDAP(odm.Model):
//     enabled: bool = odm.Boolean(description="Should LDAP be enabled or not?")
//     admin_dn: str = odm.Optional(odm.Keyword(), description="DN of the group or the user who will get admin privileges")
//     bind_user: str = odm.Optional(odm.Keyword(), description="User use to query the LDAP server")
//     bind_pass: str = odm.Optional(odm.Keyword(), description="Password used to query the LDAP server")
//     auto_create: bool = odm.Boolean(description="Auto-create users if they are missing")
//     auto_sync: bool = odm.Boolean(description="Should we automatically sync with LDAP server on each login?")
//     auto_properties: List[AutoProperty] = odm.List(odm.Compound(AutoProperty), default=[],
//                                                    description="Automatic role and classification assignments")
//     base: str = odm.Keyword(description="Base DN for the users")
//     classification_mappings: Dict[str, str] = odm.Any(description="Classification mapping")
//     email_field: str = odm.Keyword(description="Name of the field containing the email address")
//     group_lookup_query: str = odm.Keyword(description="How the group lookup is queried")
//     image_field: str = odm.Keyword(description="Name of the field containing the user's avatar")
//     image_format: str = odm.Keyword(description="Type of image used to store the avatar")
//     name_field: str = odm.Keyword(description="Name of the field containing the user's name")
//     signature_importer_dn: str = odm.Optional(
//         odm.Keyword(),
//         description="DN of the group or the user who will get signature_importer role")
//     signature_manager_dn: str = odm.Optional(
//         odm.Keyword(),
//         description="DN of the group or the user who will get signature_manager role")
//     uid_field: str = odm.Keyword(description="Field name for the UID")
//     uri: str = odm.Keyword(description="URI to the LDAP server")


// DEFAULT_LDAP = {
//     "enabled": False,
//     "bind_user": None,
//     "bind_pass": None,
//     "auto_create": True,
//     "auto_sync": True,
//     "auto_properties": [],
//     "base": "ou=people,dc=assemblyline,dc=local",
//     "email_field": "mail",
//     "group_lookup_query": "(&(objectClass=Group)(member=%s))",
//     "image_field": "jpegPhoto",
//     "image_format": "jpeg",
//     "name_field": "cn",
//     "uid_field": "uid",
//     "uri": "ldap://localhost:389",

//     # Deprecated
//     "admin_dn": None,
//     "classification_mappings": {},
//     "signature_importer_dn": None,
//     "signature_manager_dn": None,
// }


// @odm.model(index=False, store=False, description="Internal Authentication Configuration")
// class Internal(odm.Model):
//     enabled: bool = odm.Boolean(description="Internal authentication allowed?")
//     failure_ttl: int = odm.Integer(description="How long to wait after `max_failures` before re-attempting login?")
//     max_failures: int = odm.Integer(description="Maximum number of fails allowed before timeout")
//     password_requirements: PasswordRequirement = odm.Compound(PasswordRequirement,
//                                                               default=DEFAULT_PASSWORD_REQUIREMENTS,
//                                                               description="Password requirements")
//     signup: Signup = odm.Compound(Signup, default=DEFAULT_SIGNUP, description="Signup method")


// DEFAULT_INTERNAL = {
//     "enabled": True,
//     "failure_ttl": 60,
//     "max_failures": 5,
//     "password_requirements": DEFAULT_PASSWORD_REQUIREMENTS,
//     "signup": DEFAULT_SIGNUP
// }


// @odm.model(index=False, store=False, description="App provider")
// class AppProvider(odm.Model):
//     access_token_url: str = odm.Keyword(description="URL used to get the access token")
//     user_get: str = odm.Optional(odm.Keyword(), description="Path from the base_url to fetch the user info")
//     group_get: str = odm.Optional(odm.Keyword(), description="Path from the base_url to fetch the group info")
//     scope: str = odm.Keyword()
//     client_id: str = odm.Optional(odm.Keyword(), description="ID of your application to authenticate to the OAuth")
//     client_secret: str = odm.Optional(odm.Keyword(),
//                                       description="Password to your application to authenticate to the OAuth provider")


// @odm.model(index=False, store=False, description="OAuth Provider Configuration")
// class OAuthProvider(odm.Model):
//     auto_create: bool = odm.Boolean(default=True, description="Auto-create users if they are missing")
//     auto_sync: bool = odm.Boolean(default=False, description="Should we automatically sync with OAuth provider?")
//     auto_properties: List[AutoProperty] = odm.List(odm.Compound(AutoProperty), default=[],
//                                                    description="Automatic role and classification assignments")
//     app_provider: AppProvider = odm.Optional(odm.Compound(AppProvider))
//     uid_randomize: bool = odm.Boolean(default=False,
//                                       description="Should we generate a random username for the authenticated user?")
//     uid_randomize_digits: int = odm.Integer(default=0,
//                                             description="How many digits should we add at the end of the username?")
//     uid_randomize_delimiter: str = odm.Keyword(default="-",
//                                                description="What is the delimiter used by the random name generator?")
//     uid_regex: str = odm.Optional(
//         odm.Keyword(),
//         description="Regex used to parse an email address and capture parts to create a user ID out of it")
//     uid_format: str = odm.Optional(odm.Keyword(),
//                                    description="Format of the user ID based on the captured parts from the regex")
//     client_id: str = odm.Optional(odm.Keyword(),
//                                   description="ID of your application to authenticate to the OAuth provider")
//     client_secret: str = odm.Optional(odm.Keyword(),
//                                       description="Password to your application to authenticate to the OAuth provider")
//     request_token_url: str = odm.Optional(odm.Keyword(), description="URL to request token")
//     request_token_params: str = odm.Optional(odm.Keyword(), description="Parameters to request token")
//     access_token_url: str = odm.Optional(odm.Keyword(), description="URL to get access token")
//     access_token_params: str = odm.Optional(odm.Keyword(), description="Parameters to get access token")
//     authorize_url: str = odm.Optional(odm.Keyword(), description="URL used to authorize access to a resource")
//     authorize_params: str = odm.Optional(odm.Keyword(), description="Parameters used to authorize access to a resource")
//     api_base_url: str = odm.Optional(odm.Keyword(), description="Base URL for downloading the user's and groups info")
//     client_kwargs: Dict[str, str] = odm.Optional(odm.Mapping(odm.Keyword()),
//                                                  description="Keyword arguments passed to the different URLs")
//     jwks_uri: str = odm.Optional(odm.Keyword(), description="URL used to verify if a returned JWKS token is valid")
//     uid_field: str = odm.Optional(odm.Keyword(), description="Name of the field that will contain the user ID")
//     user_get: str = odm.Optional(odm.Keyword(), description="Path from the base_url to fetch the user info")
//     user_groups: str = odm.Optional(odm.Keyword(), description="Path from the base_url to fetch the group info")
//     user_groups_data_field: str = odm.Optional(
//         odm.Keyword(),
//         description="Field return by the group info API call that contains the list of groups")
//     user_groups_name_field: str = odm.Optional(
//         odm.Keyword(),
//         description="Name of the field in the list of groups that contains the name of the group")
//     use_new_callback_format: bool = odm.Boolean(default=False, description="Should we use the new callback method?")
//     allow_external_tokens: bool = odm.Boolean(
//         default=False, description="Should token provided to the login API directly be use for authentication?")
//     external_token_alternate_audiences: List[str] = odm.List(
//         odm.Keyword(), default=[], description="List of valid alternate audiences for the external token.")
//     email_fields: List[str] = odm.List(odm.Keyword(), default=DEFAULT_EMAIL_FIELDS,
//                                        description="List of fields in the claim to get the email from")
//     username_field: str = odm.Keyword(default='uname', description="Name of the field that will contain the username")


// DEFAULT_OAUTH_PROVIDER_AZURE = {
//     "access_token_url": 'https://login.microsoftonline.com/common/oauth2/token',
//     "api_base_url": 'https://login.microsoft.com/common/',
//     "authorize_url": 'https://login.microsoftonline.com/common/oauth2/authorize',
//     "client_id": None,
//     "client_secret": None,
//     "client_kwargs": {"scope": "openid email profile"},
//     "jwks_uri": "https://login.microsoftonline.com/common/discovery/v2.0/keys",
//     "user_get": "openid/userinfo"
// }

// DEFAULT_OAUTH_PROVIDER_GOOGLE = {
//     "access_token_url": 'https://oauth2.googleapis.com/token',
//     "api_base_url": 'https://openidconnect.googleapis.com/',
//     "authorize_url": 'https://accounts.google.com/o/oauth2/v2/auth',
//     "client_id": None,
//     "client_secret": None,
//     "client_kwargs": {"scope": "openid email profile"},
//     "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
//     "user_get": "v1/userinfo"
// }

// DEFAULT_OAUTH_PROVIDER_AUTH_ZERO = {
//     "access_token_url": 'https://{TENANT}.auth0.com/oauth/token',
//     "api_base_url": 'https://{TENANT}.auth0.com/',
//     "authorize_url": 'https://{TENANT}.auth0.com/authorize',
//     "client_id": None,
//     "client_secret": None,
//     "client_kwargs": {"scope": "openid email profile"},
//     "jwks_uri": "https://{TENANT}.auth0.com/.well-known/jwks.json",
//     "user_get": "userinfo"
// }

// DEFAULT_OAUTH_PROVIDERS = {
//     'auth0': DEFAULT_OAUTH_PROVIDER_AUTH_ZERO,
//     'azure_ad': DEFAULT_OAUTH_PROVIDER_AZURE,
//     'google': DEFAULT_OAUTH_PROVIDER_GOOGLE,
// }


// @odm.model(index=False, store=False, description="OAuth Configuration")
// class OAuth(odm.Model):
//     enabled: bool = odm.Boolean(description="Enable use of OAuth?")
//     gravatar_enabled: bool = odm.Boolean(description="Enable gravatar?")
//     providers: Dict[str, OAuthProvider] = odm.Mapping(odm.Compound(OAuthProvider), default=DEFAULT_OAUTH_PROVIDERS,
//                                                       description="OAuth provider configuration")


// DEFAULT_OAUTH = {
//     "enabled": False,
//     "gravatar_enabled": True,
//     "providers": DEFAULT_OAUTH_PROVIDERS
// }


// @odm.model(index=False, store=False, description="Authentication Methods")
// class Auth(odm.Model):
//     allow_2fa: bool = odm.Boolean(description="Allow 2FA?")
//     allow_apikeys: bool = odm.Boolean(description="Allow API keys?")
//     allow_extended_apikeys: bool = odm.Boolean(description="Allow extended API keys?")
//     allow_security_tokens: bool = odm.Boolean(description="Allow security tokens?")
//     internal: Internal = odm.Compound(Internal, default=DEFAULT_INTERNAL,
//                                       description="Internal authentication settings")
//     ldap: LDAP = odm.Compound(LDAP, default=DEFAULT_LDAP, description="LDAP settings")
//     oauth: OAuth = odm.Compound(OAuth, default=DEFAULT_OAUTH, description="OAuth settings")


// DEFAULT_AUTH = {
//     "allow_2fa": True,
//     "allow_apikeys": True,
//     "allow_extended_apikeys": True,
//     "allow_security_tokens": True,
//     "internal": DEFAULT_INTERNAL,
//     "ldap": DEFAULT_LDAP,
//     "oauth": DEFAULT_OAUTH
// }


// @odm.model(index=False, store=False, description="Alerter Configuration")
// class Alerter(odm.Model):
//     alert_ttl: int = odm.Integer(description="Time to live (days) for an alert in the system")
//     constant_alert_fields: List[str] = odm.List(
//         odm.Keyword(), description="List of fields that should not change during an alert update")
//     default_group_field: str = odm.Keyword(description="Default field used for alert grouping view")
//     delay: int = odm.Integer(
//         description="Time in seconds that we give extended scans and workflow to complete their work "
//                     "before we start showing alerts in the alert viewer.")
//     filtering_group_fields: List[str] = odm.List(
//         odm.Keyword(),
//         description="List of group fields that when selected will ignore certain alerts where this field is missing.")
//     non_filtering_group_fields: List[str] = odm.List(
//         odm.Keyword(), description="List of group fields that are sure to be present in all alerts.")
//     process_alert_message: str = odm.Keyword(
//         description="Python path to the function that will process an alert message.")
//     threshold: int = odm.Integer(description="Minimum score to reach for a submission to be considered an alert.")


// DEFAULT_ALERTER = {
//     "alert_ttl": 90,
//     "constant_alert_fields": ["alert_id", "file", "ts"],
//     "default_group_field": "file.sha256",
//     "delay": 300,
//     "filtering_group_fields": [
//         "file.name",
//         "status",
//         "priority"
//     ],
//     "non_filtering_group_fields": [
//         "file.md5",
//         "file.sha1",
//         "file.sha256"
//     ],
//     "process_alert_message": "assemblyline_core.alerter.processing.process_alert_message",
//     "threshold": 500
// }

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Classification {
    pub path: Option<PathBuf>,
    pub config: Option<String>,
}

impl Default for Classification {
    fn default() -> Self {
        Self { 
            path: Some("/etc/assemblyline/classification.yml".into()), 
            config: None,
        }
    }
}


/// Dispatcher Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Dispatcher {
    /// Time between re-dispatching attempts, as long as some action (submission or any task completion) happens before this timeout ends, the timeout resets.
    pub timeout: f64,
    /// Maximum submissions allowed to be in-flight
    pub max_inflight: u64,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self { 
            timeout: 15.0 * 60.0, 
            max_inflight: 1000 
        }
    }
}


// Configuration options regarding data expiry
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Expiry {
    /// Perform expiry in batches?<br>Delete queries are rounded by day therefore all delete operation happen at the same time at midnight
    pub batch_delete: bool,
    /// Delay, in hours, that will be applied to the expiry query so we can keep data longer then previously set or we can offset deletion during non busy hours
    pub delay: u32,
    /// Should we also cleanup the file storage?
    pub delete_storage: bool,
    /// Time, in seconds, to sleep in between each expiry run
    pub sleep_time: u32,
    /// Number of concurrent workers
    pub workers: u32,
    /// Worker processes for file storage deletes.
    pub delete_workers: u32,
    /// How many query chunks get run per iteration.
    pub iteration_max_tasks: u32,
    /// How large a batch get deleted per iteration.
    pub delete_batch_size: u32,
    /// The default period, in days, before tags expire from Badlist
    pub badlisted_tag_dtl: u32,
}

impl Default for Expiry {
    fn default() -> Self {
        Self { 
            batch_delete: false, 
            delay: 0, 
            delete_storage: true, 
            sleep_time: 15, 
            workers: 20, 
            delete_workers: 2, 
            iteration_max_tasks: 20, 
            delete_batch_size: 200, 
            badlisted_tag_dtl: 0 
        }
    }
}


#[derive(strum::EnumIter, strum::Display, strum::EnumString, SerializeDisplay, DeserializeFromStr, PartialEq, Eq, Hash)]
#[strum(ascii_case_insensitive, serialize_all = "kebab-case")]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
    UserLow,
    UserMedium,
    UserHigh,
}

impl Priority {
    pub fn range(&self) -> (u16, u16) {
        match self {
            Priority::Low => (0, 100),
            Priority::Medium => (101, 200),
            Priority::High => (201, 300),
            Priority::Critical => (301, 400),
            Priority::UserLow => (401, 500),
            Priority::UserMedium => (501, 1000),
            Priority::UserHigh => (1001, 1500),
        }
    }
}


/// Ingester Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Ingester {
    // /// Default user for bulk ingestion and unattended submissions
    // pub default_user: str = odm.Keyword()
    // /// Default service selection
    // pub default_services: List[str] = odm.List(odm.Keyword(), )
    // /// Default service selection for resubmits
    // pub default_resubmit_services: List[str] = odm.List(odm.Keyword(), )
    // /// A prefix for descriptions. When a description is automatically generated, it will be the hash prefixed by this string
    // pub description_prefix: str = odm.Keyword()
    // /// Path to a callback function filtering ingestion tasks that should have their priority forcefully reset to low
    // pub is_low_priority: str = odm.Keyword()
    // get_whitelist_verdict: str = odm.Keyword()
    // whitelist: str = odm.Keyword()
    // /// How many extracted files may be added to a Submission. Overrideable via submission parameters.
    // pub default_max_extracted: int = odm.Integer()
    // /// How many supplementary files may be added to a Submission. Overrideable via submission parameters
    // pub default_max_supplementary: int = odm.Integer()
    /// Period, in seconds, in which a task should be expired
    pub expire_after: f32,
    /// Drop a task altogether after this many seconds
    pub stale_after_seconds: f32,
    /// How long should scores be kept before expiry
    pub incomplete_expire_after_seconds: f32,
    /// How long should scores be cached in the ingester
    pub incomplete_stale_after_seconds: f32,
    /// Thresholds at certain buckets before sampling
    pub sampling_at: HashMap<Priority, i64>,
    /// How many files to send to dispatcher concurrently
    pub max_inflight: u64,
    /// How long are files results cached
    pub cache_dtl: u32,
    /// Always create submissions even on cache hit?
    pub always_create_submission: bool,
}

impl Default for Ingester {
    fn default() -> Self {
        Self {
            cache_dtl: 2,
//     'default_user': 'internal',
//     'default_services': [],
//     'default_resubmit_services': [],
//     'description_prefix': 'Bulk',
//     'is_low_priority': 'assemblyline.common.null.always_false',
//     'get_whitelist_verdict': 'assemblyline.common.signaturing.drop',
//     'whitelist': 'assemblyline.common.null.whitelist',
//     'default_max_extracted': 100,
//     'default_max_supplementary': 100,
            expire_after: 15.0 * 24.0 * 60.0 * 60.0,
            stale_after_seconds: 1.0 * 24.0 * 60.0 * 60.0,
            incomplete_expire_after_seconds: 3600.0,
            incomplete_stale_after_seconds: 1800.0,
            sampling_at: [
                (Priority::Low, 10000000),
                (Priority::Medium, 2000000),
                (Priority::High, 1000000),
                (Priority::Critical, 500000),
            ].into_iter().collect(),
            max_inflight: 5000,
            always_create_submission: false,
        }
    }
}


/// Redis Service configuration
#[derive(Serialize, Deserialize)]
pub struct RedisServer {
    /// Hostname of Redis instance
    pub host: String,
    /// Port of Redis instance
    pub port: u16,
    /// Which db to connect to
    #[serde(default)]
    pub db: i64,
}

fn default_redis_nonpersistant() -> RedisServer {
    RedisServer {
        host: "127.0.0.1".to_owned(),
        port: 6379,
        db: 0,
    }
}

fn default_redis_persistant() -> RedisServer {
    RedisServer {
        host: "127.0.0.1".to_owned(),
        port: 6380,
        db: 0,
    }
}


// @odm.model(index=False, store=False)
// class ESMetrics(odm.Model):
//     hosts: List[str] = odm.Optional(odm.List(odm.Keyword()), description="Elasticsearch hosts")
//     host_certificates: str = odm.Optional(odm.Keyword(), description="Host certificates")
//     warm = odm.Integer(description="How long, per unit of time, should a document remain in the 'warm' tier?")
//     cold = odm.Integer(description="How long, per unit of time, should a document remain in the 'cold' tier?")
//     delete = odm.Integer(description="How long, per unit of time, should a document remain before being deleted?")
//     unit = odm.Enum(['d', 'h', 'm'], description="Unit of time used by `warm`, `cold`, `delete` phases")


// DEFAULT_ES_METRICS = {
//     'hosts': None,
//     'host_certificates': None,
//     'warm': 2,
//     'cold': 30,
//     'delete': 90,
//     'unit': 'd'
// }


// @odm.model(index=False, store=False)
// class APMServer(odm.Model):
//     server_url: str = odm.Optional(odm.Keyword(), description="URL to API server")
//     token: str = odm.Optional(odm.Keyword(), description="Authentication token for server")


// DEFAULT_APM_SERVER = {
//     'server_url': None,
//     'token': None
// }


/// Metrics Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Metrics {
//     apm_server: APMServer = odm.Compound(APMServer, default=DEFAULT_APM_SERVER, description="APM server configuration")
//     elasticsearch: ESMetrics = odm.Compound(ESMetrics, default=DEFAULT_ES_METRICS, description="Where to export metrics?")
    /// How often should we be exporting metrics in seconds?
    pub export_interval: u32,
    /// Redis for Dashboard metrics
    pub redis: RedisServer,
}

impl Default for Metrics {
    fn default() -> Self {
        Self { 
            export_interval: 5, 
            redis: default_redis_nonpersistant()
        }
    }
// DEFAULT_METRICS = {
//     'apm_server': DEFAULT_APM_SERVER,
//     'elasticsearch': DEFAULT_ES_METRICS,
//     'export_interval': 5,
//     'redis': DEFAULT_REDIS_NP,
// }
}


#[derive(Serialize, Deserialize, Default)]
/// Malware Archive Configuration
#[serde(default)]
pub struct Archiver {
    /// List of minimum required service before archiving takes place
    pub minimum_required_services: Vec<String>,
}

/// Redis Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Redis {
    /// A volatile Redis instance
    pub nonpersistent: RedisServer,
    /// A persistent Redis instance
    pub persistent: RedisServer,
}

impl Default for Redis {
    fn default() -> Self {
        Self { 
            nonpersistent: default_redis_nonpersistant(), 
            persistent: default_redis_persistant()
        }
    }
}


// @odm.model(index=False, store=False, description="A configuration for mounting existing volumes to a container")
// class Mount(odm.Model):
//     name: str = odm.Keyword(description="Name of volume mount")
//     path: str = odm.Text(description="Target mount path")
//     read_only: bool = odm.Boolean(default=True, description="Should this be mounted as read-only?")
//     privileged_only: bool = odm.Boolean(default=False,
//                                         description="Should this mount only be available for privileged services?")

//     # Kubernetes-specific
//     resource_type: str = odm.Enum(default='volume', values=['secret', 'configmap', 'volume'],
//                                   description="Type of mountable Kubernetes resource")
//     resource_name: str = odm.Optional(odm.Keyword(), description="Name of resource (Kubernetes only)")
//     resource_key: str = odm.Optional(odm.Keyword(), description="Key of ConfigMap/Secret (Kubernetes only)")

//     # TODO: Deprecate in next major change in favour of general configuration above for mounting Kubernetes resources
//     config_map: str = odm.Optional(odm.Keyword(), description="Name of ConfigMap (Kubernetes only, deprecated)")
//     key: str = odm.Optional(odm.Keyword(), description="Key of ConfigMap (Kubernetes only, deprecated)")


// @odm.model(index=False, store=False,
//            description="A set of default values to be used running a service when no other value is set")
// class ScalerServiceDefaults(odm.Model):
//     growth: int = odm.Integer(description="Period, in seconds, to wait before scaling up a service deployment")
//     shrink: int = odm.Integer(description="Period, in seconds, to wait before scaling down a service deployment")
//     backlog: int = odm.Integer(description="Backlog threshold that dictates scaling adjustments")
//     min_instances: int = odm.Integer(description="The minimum number of service instances to be running")
//     environment: List[EnvironmentVariable] = odm.List(odm.Compound(EnvironmentVariable), default=[],
//                                                       description="Environment variables to pass onto services")
//     mounts: List[Mount] = odm.List(odm.Compound(Mount), default=[],
//                                    description="A list of volume mounts for every service")


// # The operations we support for label and field selectors are based on the common subset of
// # what kubernetes supports on the list_node API endpoint and the nodeAffinity field
// # on pod specifications. The selector needs to work in both cases because we use these
// # selectors both for probing what nodes are available (list_node) and making sure
// # the pods only run on the pods that are returned there (using nodeAffinity)

// @odm.model(index=False, store=False, description="Limit a set of kubernetes objects based on a field query.")
// class FieldSelector(odm.Model):
//     key = odm.keyword(description="Name of a field to select on.")
//     equal = odm.boolean(default=True, description="When true key must equal value, when false it must not")
//     value = odm.keyword(description="Value to compare field to.")


// # Excluded from this list is Gt and Lt for above reason
// KUBERNETES_LABEL_OPS = ['In', 'NotIn', 'Exists', 'DoesNotExist']


// @odm.model(index=False, store=False, description="Limit a set of kubernetes objects based on a label query.")
// class LabelSelector(odm.Model):
//     key = odm.keyword(description="Name of label to select on.")
//     operator = odm.Enum(KUBERNETES_LABEL_OPS, description="Operation to select label with.")
//     values = odm.sequence(odm.keyword(), description="Value list to compare label to.")


// @odm.model(index=False, store=False)
// class Selector(odm.Model):
//     field = odm.sequence(odm.compound(FieldSelector), default=[],
//                          description="Field selector for resource under kubernetes")
//     label = odm.sequence(odm.compound(LabelSelector), default=[],
//                          description="Label selector for resource under kubernetes")


// @odm.model(index=False, store=False)
// class Scaler(odm.Model):
//     service_defaults: ScalerServiceDefaults = odm.Compound(ScalerServiceDefaults,
//                                                            description="Defaults Scaler will assign to a service.")
//     cpu_overallocation: float = odm.Float(description="Percentage of CPU overallocation")
//     memory_overallocation: float = odm.Float(description="Percentage of RAM overallocation")
//     overallocation_node_limit = odm.Optional(odm.Integer(description="If the system has this many nodes or "
//                                                                      "more overallocation is ignored"))
//     additional_labels: List[str] = odm.Optional(
//         odm.List(odm.Text()), description="Additional labels to be applied to services('=' delimited)")
//     linux_node_selector = odm.compound(Selector, description="Selector for linux nodes under kubernetes")
//     # windows_node_selector = odm.compound(Selector, description="Selector for windows nodes under kubernetes")


// DEFAULT_SCALER = {
//     'additional_labels': None,
//     'cpu_overallocation': 1,
//     'memory_overallocation': 1,
//     'overallocation_node_limit': None,
//     'service_defaults': {
//         'growth': 60,
//         'shrink': 30,
//         'backlog': 100,
//         'min_instances': 0,
//         'environment': [
//             {'name': 'SERVICE_API_HOST', 'value': 'http://service-server:5003'},
//             {'name': 'AL_SERVICE_TASK_LIMIT', 'value': 'inf'},
//         ],
//     },
//     'linux_node_selector': {
//         'field': [],
//         'label': [],
//     },
//     # 'windows_node_selector': {
//     #     'field': [],
//     #     'label': [],
//     # },
// }


// @odm.model(index=False, store=False)
// class RegistryConfiguration(odm.Model):
//     name: str = odm.Text(description="Name of container registry")
//     proxies: Dict = odm.Optional(odm.Mapping(odm.Text()),
//                                  description="Proxy configuration that is passed to Python Requests")


// @odm.model(index=False, store=False)
// class Updater(odm.Model):
//     job_dockerconfig: DockerConfigDelta = odm.Compound(
//         DockerConfigDelta, description="Container configuration used for service registration/updates")
//     registry_configs: List = odm.List(odm.Compound(RegistryConfiguration),
//                                       description="Configurations to be used with container registries")


// DEFAULT_UPDATER = {
//     'job_dockerconfig': {
//         'cpu_cores': 1,
//         'ram_mb': 1024,
//         'ram_mb_min': 256,
//     },
//     'registry_configs': [{
//         'name': 'registry.hub.docker.com',
//         'proxies': {}
//     }]
// }


// @odm.model(index=False, store=False)
// class VacuumSafelistItem(odm.Model):
//     name = odm.Keyword()
//     conditions = odm.Mapping(odm.Keyword())


// @odm.model(index=False, store=False)
// class Vacuum(odm.Model):
//     list_cache_directory: str = odm.Keyword()
//     worker_cache_directory: str = odm.Keyword()
//     data_directories: List[str] = odm.List(odm.Keyword())
//     file_directories: List[str] = odm.List(odm.Keyword())
//     assemblyline_user: str = odm.Keyword()
//     department_map_url = odm.Optional(odm.Keyword())
//     department_map_init = odm.Optional(odm.Keyword())
//     stream_map_url = odm.Optional(odm.Keyword())
//     stream_map_init = odm.Optional(odm.Keyword())
//     safelist = odm.List(odm.Compound(VacuumSafelistItem))
//     worker_threads: int = odm.Integer()
//     worker_rollover: int = odm.Integer()
//     minimum_classification: str = odm.Keyword()
//     ingest_type = odm.keyword()


// DEFAULT_VACUUM = dict(
//     list_cache_directory="/cache/",
//     worker_cache_directory="/memory/",
//     data_directories=[],
//     file_directories=[],
//     assemblyline_user="vacuum-service-account",
//     department_map_url=None,
//     department_map_init=None,
//     stream_map_url=None,
//     stream_map_init=None,
//     safelist=[],
//     worker_threads=50,
//     worker_rollover=1000,
//     minimum_classification='U',
//     ingest_type='VACUUM',
// )


/// Core Component Configuration
#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
// @odm.model(index=False, store=False, description="")
pub struct Core {
    // /// Configuration for Alerter
    // #[serde(default)]
    // pub alerter: Alerter,
    /// Configuration for the permanent submission archive
    pub archiver: Archiver,
    /// Configuration for Dispatcher
    pub dispatcher: Dispatcher,
    /// Configuration for Expiry
    pub expiry: Expiry,
    /// Configuration for Ingester
    pub ingester: Ingester,
    /// Configuration for Metrics Collection
    pub metrics: Metrics,
    /// Configuration for system cleanup
    pub plumber: Plumber,
    /// Configuration for Redis instances
    pub redis: Redis,
    // /// Configuration for Scaler
    // #[serde(default)]
    // pub scaler: Scaler,
    // /// Configuration for Updater
    // #[serde(default)]
    // pub updater: Updater,
    // /// Configuration for Vacuum
    // #[serde(default)]
    // pub vacuum: Vacuum,
}

// DEFAULT_CORE = {
//     "alerter": DEFAULT_ALERTER,
//     "archiver": DEFAULT_ARCHIVER,
//     "dispatcher": DEFAULT_DISPATCHER,
//     "expiry": DEFAULT_EXPIRY,
//     "ingester": DEFAULT_INGESTER,
//     "metrics": DEFAULT_METRICS,
//     "redis": DEFAULT_REDIS,
//     "scaler": DEFAULT_SCALER,
//     "updater": DEFAULT_UPDATER,
// }

/// Plumber Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Plumber {
    /// Interval in seconds at which the notification queue cleanup should run
    pub notification_queue_interval: u64,
    /// Max age in seconds notification queue messages can be
    pub notification_queue_max_age: u64,
}

impl Default for Plumber {
    fn default() -> Self {
        Self { 
            notification_queue_interval: 30 * 60,
            notification_queue_max_age: 24 * 60 * 60
        }
    }
}




/// Datastore Archive feature configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Archive {
    /// Are we enabling Achiving features across indices?
    pub enabled: bool,
    /// List of indices the ILM Applies to
    pub indices: Vec<String>,
}

impl Default for Archive {
    fn default() -> Self {
        Self { 
            enabled: false, 
            indices: vec!["file".to_owned(), "submission".to_owned(), "result".to_owned()], 
        }
    }
}


#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all="lowercase")]
pub enum DatastoreType {
    Elasticsearch
}

#[test]
fn test_datastore_type_serialization() {
    assert_eq!(serde_json::to_string(&DatastoreType::Elasticsearch).unwrap(), "\"elasticsearch\"");
    assert_eq!(serde_json::from_str::<DatastoreType>("\"elasticsearch\"").unwrap(), DatastoreType::Elasticsearch);
    assert_eq!(serde_json::to_value(DatastoreType::Elasticsearch).unwrap(), serde_json::json!("elasticsearch"));
    // assert_eq!(serde_json::from_str::<DatastoreType>("\"Elasticsearch\"").unwrap(), DatastoreType::Elasticsearch);

    #[derive(Debug, Serialize, Deserialize)]
    struct Test {
        ds: DatastoreType
    }
    let sample = Test {ds: DatastoreType::Elasticsearch};
    assert_eq!(serde_json::to_string(&sample).unwrap(), "{\"ds\":\"elasticsearch\"}");
}


/// Datastore Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Datastore {
    /// List of hosts used for the datastore
    pub hosts: Vec<String>,
    /// Datastore Archive feature configuration
    pub archive: Archive,
    /// Default cache lenght for computed indices (submission_tree, submission_summary...
    pub cache_dtl: u32,
    /// Type of application used for the datastore
    #[serde(rename="type")]
    pub dtype: DatastoreType,
}

impl Default for Datastore {
    fn default() -> Self {
        Self {
            hosts: vec!["http://elastic:devpass@localhost:9200".to_owned()],
            archive: Default::default(),
            cache_dtl: 5,
            dtype: DatastoreType::Elasticsearch,
        }
    }
}


// @odm.model(index=False, store=False, description="Datasource Configuration")
// class Datasource(odm.Model):
//     classpath: str = odm.Keyword()
//     config: Dict[str, str] = odm.Mapping(odm.Keyword())


// DEFAULT_DATASOURCES = {
//     "al": {
//         "classpath": 'assemblyline.datasource.al.AL',
//         "config": {}
//     },
//     "alert": {
//         "classpath": 'assemblyline.datasource.alert.Alert',
//         "config": {}
//     }
// }


/// Filestore Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Filestore {
    /// List of filestores used for malware archive
    pub archive: Vec<String>,
    /// List of filestores used for caching
    pub cache: Vec<String>,
    /// List of filestores used for storage
    pub storage: Vec<String>,
}

impl Default for Filestore {
    fn default() -> Self {
        Self { 
            archive: vec!["s3://al_storage_key:Ch@ngeTh!sPa33w0rd@localhost:9000?s3_bucket=al-archive&use_ssl=False".to_string()], 
            cache: vec!["s3://al_storage_key:Ch@ngeTh!sPa33w0rd@localhost:9000?s3_bucket=al-cache&use_ssl=False".to_string()], 
            storage: vec!["s3://al_storage_key:Ch@ngeTh!sPa33w0rd@localhost:9000?s3_bucket=al-storage&use_ssl=False".to_string()] 
        }
    }
}

#[derive(Debug, strum::Display, SerializeDisplay, strum::EnumString, DeserializeFromStr)]
#[strum(serialize_all="UPPERCASE", ascii_case_insensitive)]
pub enum LogLevel {
    Debug, 
    Info,
    Warning,
    Error,
    Critical,
    Disabled,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SyslogTransport {
    Udp,
    Tcp
}

/// Model Definition for the Logging Configuration
#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Logging {
    /// What level of logging should we have?
    pub log_level: LogLevel,
    /// Should we log to console?
    pub log_to_console: bool,
    /// Should we log to files on the server?
    pub log_to_file: bool,
    /// If `log_to_file: true`, what is the directory to store logs?
    pub log_directory: PathBuf,
    /// Should logs be sent to a syslog server?
    pub log_to_syslog: bool,
    /// If `log_to_syslog: true`, provide hostname/IP of the syslog server?
    pub syslog_host: String,
    /// If `log_to_syslog: true`, provide port of the syslog server?
    pub syslog_port: u16,
    /// If `log_to_syslog: true`, provide transport for syslog server?
    pub syslog_transport: SyslogTransport,
    // /// How often, in seconds, should counters log their values?
    // pub export_interval: int = odm.Integer(")
    /// Log in JSON format?
    pub log_as_json: bool,
    // /// Add a health check to core components.<br>If `true`, core components will touch this path regularly to tell the container environment it is healthy
    // pub heartbeat_file: str = odm.Optional(odm.Keyword(),")
}

impl Default for Logging {
    fn default() -> Self {
        Self { 
            log_directory: "/var/log/assemblyline/".into(),
            log_as_json: true,
            log_level: LogLevel::Info,
            log_to_console: true,
            log_to_file: false,
            log_to_syslog: false,
            syslog_host: "localhost".to_owned(),
            syslog_port: 514,
            syslog_transport: SyslogTransport::Tcp,
            // export_interval: 5,
            // heartbeat_file: "/tmp/heartbeat"
        }
    }
}

// SERVICE_CATEGORIES = [
//     'Antivirus',
//     'Dynamic Analysis',
//     'External',
//     'Extraction',
//     'Filtering',
//     'Internet Connected',
//     'Networking',
//     'Static Analysis',
// ]

fn default_service_stages() -> Vec<String> {
    vec![
        "FILTER".to_string(),
        "EXTRACT".to_string(),
        "CORE".to_string(),
        "SECONDARY".to_string(),
        "POST".to_string(),
        "REVIEW".to_string(),
    ]
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Debug, Clone, Copy, PartialEq, Eq)]
// #[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum SafelistHashTypes {
    Sha1, Sha256, Md5
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Debug, Clone, Copy)]
#[strum(serialize_all = "lowercase")]
pub enum RegistryTypes {
    Docker, 
    Harbor
}

/// Service's Safelisting Configuration
// @odm.model(index=False, store=False, description="")
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ServiceSafelist {
    /// Should services be allowed to check extracted files against safelist?
    pub enabled: bool,
    /// Types of file hashes used for safelist checks
    pub hash_types: Vec<SafelistHashTypes>,
    /// Should the Safelist service always run on extracted files?
    pub enforce_safelist_service: bool,
}

impl Default for ServiceSafelist {
    fn default() -> Self {
        Self { 
            enabled: true, 
            hash_types: vec![SafelistHashTypes::Sha1, SafelistHashTypes::Sha256], 
            enforce_safelist_service: false,
        }
    }
}

// @odm.model(index=False, store=False, description="Pre-Configured Registry Details for Services")
// class ServiceRegistry(odm.Model):
//     name: str = odm.Keyword(description="Name of container registry")
//     type: str = odm.Enum(values=REGISTRY_TYPES, default='docker', description="Type of container registry")
//     username: str = odm.Keyword(description="Username for container registry")
//     password: str = odm.Keyword(description="Password for container registry")


/// Services Configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Services {
//     categories: List[str] = odm.List(odm.Keyword(), description="List of categories a service can be assigned to")
//     default_timeout: int = odm.Integer(description="Default service timeout time in seconds")
    /// List of execution stages a service can be assigned to
    pub stages: Vec<String>, 
    /// Substitution variables for image paths (for custom registry support)
    // pub image_variables: Dict[str, str] = odm.Mapping(odm.Keyword(default=''), ),
    /// Similar to `image_variables` but only applied to the updater. Intended for use with local registries.
    // pub update_image_variables: Dict[str, str] = odm.Mapping(odm.Keyword(default=''), ),
    /// Default update channel to be used for new services
    pub preferred_update_channel: String,
    /// Allow fetching container images from insecure registries
    pub allow_insecure_registry: bool,
    /// Global registry type to be used for fetching updates for a service (overridable by a service)
    pub preferred_registry_type: RegistryTypes,
    /// Global preference that controls if services should be privileged to communicate with core infrastucture
    pub prefer_service_privileged: bool,
    /// How much CPU do we want to reserve relative to the service's request?<br> At `1`, a service's full CPU request will be reserved for them.<br> At `0` (only for very small appliances/dev boxes), the service's CPU will be limited ""but no CPU will be reserved allowing for more flexible scheduling of containers.
    pub cpu_reservation: f64,
    pub safelist: ServiceSafelist,
//     registries = odm.Optional(odm.List(odm.Compound(ServiceRegistry)), description="Global set of registries for services")
//     service_account = odm.optional(odm.keyword(description="Service account to use for pods in kubernetes where the service does not have one configured."))
}

impl Default for Services {
    fn default() -> Self {
        Self { 
    //     "categories": SERVICE_CATEGORIES,
    //     "default_timeout": 60,
            stages: default_service_stages(),
    //     "image_variables": {},
    //     "update_image_variables": {},
            preferred_update_channel: "stable".to_string(),
            preferred_registry_type: RegistryTypes::Docker,
            prefer_service_privileged: false,
            allow_insecure_registry: false,
            cpu_reservation: 0.25,
            safelist: Default::default(),
    //     "registries": []
        }
    }
}

// @odm.model(index=False, store=False, description="System Configuration")
// class System(odm.Model):
//     constants: str = odm.Keyword(description="Module path to the assemblyline constants")
//     organisation: str = odm.Text(description="Organisation acronym used for signatures")
//     type: str = odm.Enum(values=['production', 'staging', 'development'], description="Type of system")


// DEFAULT_SYSTEM = {
//     "constants": "assemblyline.common.constants",
//     "organisation": "ACME",
//     "type": 'production',
// }


// @odm.model(index=False, store=False, description="Statistics")
// class Statistics(odm.Model):
//     alert: List[str] = odm.List(odm.Keyword(),
//                                 description="Fields used to generate statistics in the Alerts page")
//     submission: List[str] = odm.List(odm.Keyword(),
//                                      description="Fields used to generate statistics in the Submissions page")


// DEFAULT_STATISTICS = {
//     "alert": [
//         'al.attrib',
//         'al.av',
//         'al.behavior',
//         'al.domain',
//         'al.ip',
//         'al.yara',
//         'file.name',
//         'file.md5',
//         'owner'
//     ],
//     "submission": [
//         'params.submitter'
//     ]
// }


// @odm.model(index=False, store=False, description="Alerting Metadata")
// class AlertingMeta(odm.Model):
//     important: List[str] = odm.List(odm.Keyword(), description="Metadata keys that are considered important")
//     subject: List[str] = odm.List(odm.Keyword(), description="Metadata keys that refer to an email's subject")
//     url: List[str] = odm.List(odm.Keyword(), description="Metadata keys that refer to a URL")


// DEFAULT_ALERTING_META = {
//     'important': [
//         'original_source',
//         'protocol',
//         'subject',
//         'submitted_url',
//         'source_url',
//         'url',
//         'web_url',
//         'from',
//         'to',
//         'cc',
//         'bcc',
//         'ip_src',
//         'ip_dst',
//         'source'
//     ],
//     'subject': [
//         'subject'
//     ],
//     'url': [
//         'submitted_url',
//         'source_url',
//         'url',
//         'web_url'
//     ]

// }


// @odm.model(index=False, store=False, description="Target definition of an external link")
// class ExternalLinksTargets(odm.Model):
//     type: str = odm.Enum(values=['metadata', 'tag', 'hash'], description="Type of external link target")
//     key: str = odm.Keyword(description="Key that it can be used against")


// @odm.model(index=False, store=False, description="External links that specific metadata and tags can pivot to")
// class ExternalLinks(odm.Model):
//     allow_bypass: bool = odm.boolean(
//         default=False,
//         description="If the classification of the item is higher than the max_classificaiton, can we let the user "
//                     "bypass the check and still query the external link?")
//     name: str = odm.Keyword(description="Name of the link")
//     double_encode: bool = odm.boolean(default=False, description="Should the replaced value be double encoded?")
//     classification = odm.Optional(
//         odm.ClassificationString(description="Minimum classification the user must have to see this link"))
//     max_classification = odm.Optional(
//         odm.ClassificationString(description="Maximum classification of data that may be handled by the link"))
//     replace_pattern: str = odm.Keyword(
//         description="Pattern that will be replaced in the URL with the metadata or tag value")
//     targets: List[ExternalLinksTargets] = odm.List(
//         odm.Compound(ExternalLinksTargets),
//         default=[],
//         description="List of external sources to query")
//     url: str = odm.Keyword(description="URL to redirect to")


// EXAMPLE_EXTERNAL_LINK_VT = {
//     # This is an example on how this would work with VirusTotal
//     "name": "VirusTotal",
//     "replace_pattern": "{REPLACE}",
//     "targets": [
//         {"type": "tag", "key": "network.static.uri"},
//         {"type": "tag", "key": "network.dynamic.uri"},
//         {"type": "metadata", "key": "submitted_url"},
//         {"type": "hash", "key": "md5"},
//         {"type": "hash", "key": "sha1"},
//         {"type": "hash", "key": "sha256"},
//     ],
//     "url": "https://www.virustotal.com/gui/search/{REPLACE}",
//     "double_encode": True,
//     # "classification": "TLP:CLEAR",
//     # "max_classification": "TLP:CLEAR",
// }

// EXAMPLE_EXTERNAL_LINK_MB_SHA256 = {
//     # This is an example on how this would work with Malware Bazaar
//     "name": "MalwareBazaar",
//     "replace_pattern": "{REPLACE}",
//     "targets": [
//         {"type": "hash", "key": "sha256"},
//     ],
//     "url": "https://bazaar.abuse.ch/sample/{REPLACE}/",
//     # "classification": "TLP:CLEAR",
//     # "max_classification": "TLP:CLEAR",
// }


// @odm.model(index=False, store=False, description="Connection details for external systems/data sources.")
// class ExternalSource(odm.Model):
//     name: str = odm.Keyword(description="Name of the source.")
//     classification = odm.Optional(
//         odm.ClassificationString(
//             description="Minimum classification applied to information from the source"
//                         " and required to know the existance of the source."))
//     max_classification = odm.Optional(
//         odm.ClassificationString(description="Maximum classification of data that may be handled by the source"))
//     url: str = odm.Keyword(description="URL of the upstream source's lookup service.")


// EXAMPLE_EXTERNAL_SOURCE_VT = {
//     # This is an example on how this would work with VirusTotal
//     "name": "VirusTotal",
//     "url": "vt-lookup.namespace.svc.cluster.local",
//     "classification": "TLP:CLEAR",
//     "max_classification": "TLP:CLEAR",
// }

// EXAMPLE_EXTERNAL_SOURCE_MB = {
//     # This is an example on how this would work with Malware Bazaar
//     "name": "Malware Bazaar",
//     "url": "mb-lookup.namespace.scv.cluster.local",
//     "classification": "TLP:CLEAR",
//     "max_classification": "TLP:CLEAR",
// }


/// UI Configuration
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct UI {
//     alerting_meta: AlertingMeta = odm.Compound(AlertingMeta, default=DEFAULT_ALERTING_META,description="Alerting metadata fields")
    /// Allow user to tell in advance the system that a file is malicious?
    pub allow_malicious_hinting: bool,
//     allow_raw_downloads: bool = odm.Boolean(description="Allow user to download raw files?")
//     allow_zip_downloads: bool = odm.Boolean(description="Allow user to download files as password protected ZIPs?")
//     allow_replay: bool = odm.Boolean(description="Allow users to request replay on another server?")
//     allow_url_submissions: bool = odm.Boolean(description="Allow file submissions via url?")
//     audit: bool = odm.Boolean(description="Should API calls be audited and saved to a separate log file?")
//     banner: Dict[str, str] = odm.Optional(odm.Mapping(odm.Keyword()), description="Banner message display on the main page (format: {<language_code>: message})")
//     banner_level: str = odm.Enum(values=["info", "warning", "success", "error"],description="Banner message level")
//     debug: bool = odm.Boolean(description="Enable debugging?")
//     discover_url: str = odm.Optional(odm.Keyword(), description="Discover URL")
//     download_encoding = odm.Enum(values=["raw", "cart"], description="Which encoding will be used for downloads?")
//     email: str = odm.Optional(odm.Email(), description="Assemblyline admins email address")
    /// Enforce the user's quotas?
    pub enforce_quota: bool,
//     external_links: List[ExternalLinks] = odm.List(odm.Compound(ExternalLinks),description="List of external pivot links")
//     external_sources: List[ExternalSource] = odm.List(odm.Compound(ExternalSource), description="List of external sources to query")
//     fqdn: str = odm.Text(description="Fully qualified domain name to use for the 2-factor authentication validation")
//     ingest_max_priority: int = odm.Integer(description="Maximum priority for ingest API")
//     read_only: bool = odm.Boolean(description="Turn on read only mode in the UI")
//     read_only_offset: str = odm.Keyword(default="", description="Offset of the read only mode for all paging and searches")
//     rss_feeds: List[str] = odm.List(odm.Keyword(), default=[], description="List of RSS feeds to display on the UI")
//     services_feed: str = odm.Keyword(description="Feed of all the services available on AL")
//     secret_key: str = odm.Keyword(description="Flask secret key to store cookies, etc.")
//     session_duration: int = odm.Integer(description="Duration of the user session before the user has to login again")
//     statistics: Statistics = odm.Compound(Statistics, default=DEFAULT_STATISTICS, description="Statistics configuration")
//     tos: str = odm.Optional(odm.Text(), description="Terms of service")
//     tos_lockout: bool = odm.Boolean(description="Lock out user after accepting the terms of service?")
//     tos_lockout_notify: List[str] = odm.Optional(odm.List(odm.Keyword()), description="List of admins to notify when a user gets locked out")
//     url_submission_headers: Dict[str, str] = odm.Optional(odm.Mapping(odm.Keyword()), description="Headers used by the url_download method")
//     url_submission_proxies: Dict[str, str] = odm.Optional(odm.Mapping(odm.Keyword()), description="Proxy used by the url_download method")
//     url_submission_timeout: int = odm.Integer(default=15, description="Request timeout for fetching URLs")
//     validate_session_ip: bool = odm.Boolean(description="Validate if the session IP matches the IP the session was created from")
//     validate_session_useragent: bool = odm.Boolean(description="Validate if the session useragent matches the useragent the session was created with")
}

impl Default for UI {
    fn default() -> Self {
        Self { 
// DEFAULT_UI = {
//     "alerting_meta": DEFAULT_ALERTING_META,
            allow_malicious_hinting: false,
//     "allow_raw_downloads": True,
//     "allow_zip_downloads": True,
//     "allow_replay": False,
//     "allow_url_submissions": True,
//     "audit": True,
//     "banner": None,
//     "banner_level": 'info',
//     "debug": False,
//     "discover_url": None,
//     "download_encoding": "cart",
//     "email": None,
            enforce_quota: true,
//     "external_links": [],
//     "external_sources": [],
//     "fqdn": "localhost",
//     "ingest_max_priority": 250,
//     "read_only": False,
//     "read_only_offset": "",
//     "rss_feeds": [
//         "https://alpytest.blob.core.windows.net/pytest/stable.json",
//         "https://alpytest.blob.core.windows.net/pytest/services.json",
//         "https://alpytest.blob.core.windows.net/pytest/blog.json"
//     ],
//     "services_feed": "https://alpytest.blob.core.windows.net/pytest/services.json",
//     "secret_key": "This is the default flask secret key... you should change this!",
//     "session_duration": 3600,
//     "statistics": DEFAULT_STATISTICS,
//     "tos": None,
//     "tos_lockout": False,
//     "tos_lockout_notify": None,
//     "url_submission_headers": {
//         "User-Agent": "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko)"
//                       " Chrome/110.0.0.0 Safari/537.36"
//     },
//     "url_submission_proxies": {},
//     "validate_session_ip": True,
//     "validate_session_useragent": True,
// }
        }
    }
}

// # Options regarding all submissions, regardless of their input method
// @odm.model(index=False, store=False)
// class TagTypes(odm.Model):
//     attribution: List[str] = odm.List(odm.Keyword(), description="Attibution tags")
//     behavior: List[str] = odm.List(odm.Keyword(), description="Behaviour tags")
//     ioc: List[str] = odm.List(odm.Keyword(), description="IOC tags")


// DEFAULT_TAG_TYPES = {
//     'attribution': [
//         'attribution.actor',
//         'attribution.campaign',
//         'attribution.exploit',
//         'attribution.implant',
//         'attribution.family',
//         'attribution.network',
//         'av.virus_name',
//         'file.config',
//         'technique.obfuscation',
//     ],
//     'behavior': [
//         'file.behavior'
//     ],
//     'ioc': [
//         'network.email.address',
//         'network.static.ip',
//         'network.static.domain',
//         'network.static.uri',
//         'network.dynamic.ip',
//         'network.dynamic.domain',
//         'network.dynamic.uri',

//     ]
// }


// @odm.model(index=False, store=False, description="A source entry for the sha256 downloader")
// class Sha256Source(odm.Model):
//     name: str = odm.Keyword(description="Name of the sha256 source")
//     classification = odm.Optional(
//         odm.ClassificationString(
//             description="Minimum classification applied to the downloaded "
//                         "files and required to know the existance of the source."))
//     data: str = odm.Optional(odm.Keyword(description="Data block sent during the URL call (Uses replace pattern)"))
//     failure_pattern: str = odm.Optional(odm.Keyword(
//         description="Pattern to find as a failure case when API return 200 OK on failures..."))
//     method: str = odm.Enum(values=['GET', 'POST'], default="GET", description="Method used to call the URL")
//     url: str = odm.Keyword(description="Url to fetch the file via SHA256 from (Uses replace pattern)")
//     replace_pattern: str = odm.Keyword(description="Pattern to replace in the URL with the SHA256")
//     headers: Dict[str, str] = odm.Mapping(odm.Keyword(), default={},
//                                           description="Headers used to connect to the URL")
//     proxies: Dict[str, str] = odm.Mapping(odm.Keyword(), default={},
//                                           description="Proxy used to connect to the URL")
//     verify: bool = odm.Boolean(default=True, description="Should the download function Verify SSL connections?")


// EXAMPLE_SHA256_SOURCE_VT = {
//     # This is an example on how this would work with VirusTotal
//     "name": "VirusTotal",
//     "url": r"https://www.virustotal.com/api/v3/files/{SHA256}/download",
//     "replace_pattern": r"{SHA256}",
//     "headers": {"x-apikey": "YOUR_KEY"},
// }

// EXAMPLE_SHA256_SOURCE_MB = {
//     # This is an example on how this would work with Malware Bazaar
//     "name": "Malware Bazaar",
//     "url": r"https://mb-api.abuse.ch/api/v1/",
//     "headers": {"Content-Type": "application/x-www-form-urlencoded"},
//     "data": r"query=get_file&sha256_hash={SHA256}",
//     "method": "POST",
//     "replace_pattern": r"{SHA256}",
//     "failure_pattern": '"query_status": "file_not_found"'
// }


// @odm.model(index=False, store=False,
//            description="Minimum score value to get the specified verdict, otherwise the file is considered safe.")
// class Verdicts(odm.Model):
//     info: int = odm.Integer(description="Minimum score for the verdict to be Informational.")
//     suspicious: int = odm.Integer(description="Minimum score for the verdict to be Suspicious.")
//     highly_suspicious: int = odm.Integer(description="Minimum score for the verdict to be Highly Suspicious.")
//     malicious: int = odm.Integer(description="Minimum score for the verdict to be Malicious.")


// DEFAULT_VERDICTS = {
//     'info': 0,
//     'suspicious': 300,
//     'highly_suspicious': 700,
//     'malicious': 1000
// }

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Debug, Clone, Copy)]
// #[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum TemporaryKeyType {
    Union,
    Overwrite,
}

impl Default for TemporaryKeyType {
    fn default() -> Self {
        Self::Overwrite
    }
}


/// Default values for parameters for submissions that may be overridden on a per submission basis
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Submission {
    // /// How many extracted files may be added to a submission?
    // pub default_max_extracted: u32,
    // /// How many supplementary files may be added to a submission?
    // pub default_max_supplementary: u32,
    // /// Number of days submissions will remain in the system by default
    // pub dtl: u32,
    /// Number of days emptyresult will remain in the system
    pub emptyresult_dtl: u32,
    /// Maximum number of days submissions will remain in the system
    pub max_dtl: u32,
    /// Maximum files extraction depth
    pub max_extraction_depth: u32,
    /// Maximum size for files submitted in the system
    pub max_file_size: u64,
    /// Maximum length for each metadata values
    pub max_metadata_length: u32,
    /// Maximum length for each temporary data values
    pub max_temp_data_length: u32,
    // /// List of external source to fetch file via their SHA256 hashes
    // pub sha256_sources: Vec<Sha256Source>,
    // /// Tag types that show up in the submission summary
    // pub tag_types: TagTypes,
    // /// Minimum score value to get the specified verdict.
    // pub verdicts: Verdicts,

    /// Set the operation that will be used to update values using this key in the temporary submission data.
    pub default_temporary_keys: HashMap<String, TemporaryKeyType>,
    pub temporary_keys: HashMap<String, TemporaryKeyType>,
}

impl Default for Submission {
    fn default() -> Self {
        Self {
            // default_max_extracted: 500,
            // default_max_supplementary: 500,
            // dtl: 30,
            emptyresult_dtl: 5,
            max_dtl: 0,
            max_extraction_depth: 6,
            max_file_size: 104857600,
            max_metadata_length: 4096,
            max_temp_data_length: 4096,
            // sha256_sources: Default::default(),
            // tag_types: Default::default(),
            // verdicts: Default::default()
            default_temporary_keys: [
                ("passwords".to_owned(), TemporaryKeyType::Union),
                ("email_body".to_owned(), TemporaryKeyType::Union),
            ].into_iter().collect(),
            temporary_keys: Default::default()
        }
    }
}


// @odm.model(index=False, store=False, description="Configuration for connecting to a retrohunt service.")
// class Retrohunt(odm.Model):
//     enabled = odm.Boolean(default=False, description="Is the Retrohunt functionnality enabled on the frontend")
//     dtl: int = odm.Integer(default=30, description="Number of days retrohunt jobs will remain in the system by default")
//     max_dtl: int = odm.Integer(
//         default=0, description="Maximum number of days retrohunt jobs will remain in the system")
//     url = odm.Keyword(description="Base URL for service API")
//     api_key = odm.Keyword(description="Service API Key")
//     tls_verify = odm.Boolean(description="Should tls certificates be verified", default=True)


/// Assemblyline Deployment Configuration
#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// Classification information
    pub classification: Classification,
    // /// Authentication module configuration
    // pub auth: Auth,
    /// Core component configuration
    pub core: Core,
    /// Datastore configuration
    pub datastore: Datastore,
    // /// Datasources configuration
    // #[serde(default = "default_datasources")]
    // pub datasources: HashMap<String, Datasource>,
    /// Filestore configuration
    pub filestore: Filestore,
    /// Logging configuration
    pub logging: Logging,
    /// Service configuration
    pub services: Services,
    // /// System configuration
    // pub system: System,
    /// UI configuration parameters
    pub ui: UI,
    /// Options for how submissions will be processed
    pub submission: Submission,
    // /// Retrohunt configuration for the frontend and server
    // pub retrohunt: Option<Retrohunt>,
}


