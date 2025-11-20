use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::{ElasticMeta, Readable};
use crate::datastore::user::{UserRole, AclCatagory};

const APIKEY_ID_DELIMETER: &str = "+";
pub const FORBIDDEN_APIKEY_CHARACTERS: &str = r"[+@!#$%^&*()<>?/\|}{~:]";


/// Model of Apikey
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Apikey {
    /// Access Control List for the API key
    pub acl: Vec<AclCatagory>,
    /// BCrypt hash of the password for the apikey
    pub password: String,
    /// List of roles tied to the API key
    #[serde(default)]
    pub roles: Vec<UserRole>,
    /// Username
    #[metadata(copyto="__text__")]
    pub uname: String,
    /// Name of the key
    #[metadata(copyto="__text__")]
    pub key_name: String,
    /// The date this API key is created.
    #[serde(default="chrono::Utc::now")]
    pub creation_date: DateTime<Utc>,
    /// Expiry timestamp.
    #[serde(default)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// The last time this API key was used.
    #[serde(default)]
    pub last_used: Option<DateTime<Utc>>,
}

impl Readable for Apikey {
    fn set_from_archive(&mut self, _from_archive: bool) { }
}

pub fn get_apikey_id(keyname: &str, uname: &str) -> String {
    format!("{keyname}{APIKEY_ID_DELIMETER}{uname}")
}

pub fn split_apikey_id(key_id: &str) -> Option<(&str, &str)> {
    key_id.split_once(APIKEY_ID_DELIMETER)
}
