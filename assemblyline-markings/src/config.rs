//! Objects for parsing configuration data from assemblyline.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::errors::Errors;

/// Define sources for dynamic groups
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
#[serde(rename_all="lowercase")]
pub enum DynamicGroupType {
    /// Draw classification groups from user email domains
    #[default]
    Email,
    /// Draw classification groups from user ldap groups
    Group,
    /// Draw classification groups from user email domains and ldap groups
    All,
}


/// A description of the configuration block used by assemblyline for classification schemes
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClassificationConfig {
    /// Turn on/off classification enforcement. When this flag is off, this
    /// completely disables the classification engine, any documents added while
    /// the classification engine is off gets the default unrestricted value
    pub enforce: bool,

    /// Turn on/off dynamic group creation. This feature allow you to dynamically create classification groups based on
    ///  features from the user.
    pub dynamic_groups: bool,

    /// Set the type of dynamic groups to be used
    #[serde(default)]
    pub dynamic_groups_type: DynamicGroupType,

    /// List of Classification level.
    /// Graded list were a smaller number is less restricted then an higher number.
    pub levels: Vec<ClassificationLevel>,

    /// List of required tokens:
    /// A user requesting access to an item must have all the
    /// required tokens the item has to gain access to it
    pub required: Vec<ClassificationMarking>,

    /// List of groups:
    /// A user requesting access to an item must be part of a least
    /// of one the group the item is part of to gain access
    pub groups: Vec<ClassificationGroup>,

    /// List of subgroups:
    /// A user requesting access to an item must be part of a least
    /// of one the subgroup the item is part of to gain access
    pub subgroups: Vec<ClassificationSubGroup>,

    /// Default restricted classification
    pub restricted: String,

    /// Default unrestricted classification.
    /// When no classification are provided or that the classification engine is
    /// disabled, this is the classification value each items will get
    pub unrestricted: String,
}

/// A category of data delineating access
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClassificationLevel {
    /// List of alternate names for the current marking
    #[serde(default)]
    pub aliases: Vec<NameString>,

    /// Stylesheet applied in the UI for the different levels
    #[serde(default="default_css")]
    pub css: HashMap<String, String>,

    /// Description of the classification level
    #[serde(default="default_description")]
    pub description: String,

    /// Interger value of the Classification level (higher is more classified)
    pub lvl: i32,

    /// Long name of the classification item
    pub name: NameString,

    /// Short name of the classification item
    pub short_name: NameString,

    // #[serde(flatten)]
    // currently planning to static define other fields as optional, making other_fields unneeded
    // pub other_fields: HashMap<String, serde_value::Value>,
}

impl ClassificationLevel {
    #[cfg(test)]
    pub fn new(lvl: i32, short_name: &str, name: &str, aliases: Vec<&str>) -> Self {
        ClassificationLevel {
            aliases: aliases.into_iter().map(|x|x.parse().unwrap()).collect(),
            css: default_css(),
            description: default_description(),
            lvl,
            name: name.parse().unwrap(),
            short_name: short_name.parse().unwrap()
        }
    }

    /// Get all of the unique names used by this item
    pub fn unique_names(&self) -> Vec<NameString> {
        let mut names = vec![self.name.clone(), self.short_name.clone()];
        names.extend(self.aliases.iter().cloned());
        names.sort_unstable();
        names.dedup();
        return names
    }
}

/// Get the CSS value to be used for classification levels when none is configured
fn default_css() -> HashMap<String, String> { [("color".to_owned(), "default".to_owned()), ].into_iter().collect() }

/// Get the description to be used on any description field that is not defined
fn default_description() -> String {"N/A".to_owned()}

/// A control or dissemination marking
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClassificationMarking {
    /// List of alternate names for the current marking
    #[serde(default)]
    pub aliases: Vec<NameString>,

    /// Long form description of marking
    #[serde(default="default_description")]
    pub description: String,

    /// Long form canonical name of marking
    // #[serde(deserialize_with="deserialize_normalized_name")]
    pub name: NameString,

    /// Short form canonical name of marking
    // #[serde(deserialize_with="deserialize_normalized_name")]
    pub short_name: NameString,

    /// The minimum classification level an item must have for this token to be valid. (optional)
    #[serde(default)]
    pub require_lvl: Option<i32>,

    /// This is a token that is required but will display in the groups part
    /// of the classification string. (optional)
    #[serde(default)]
    pub is_required_group: bool
}

impl ClassificationMarking {
    #[cfg(test)]
    pub fn new(short_name: &str, name: &str, aliases: Vec<&str>) -> Self {
        Self {
            aliases: aliases.into_iter().map(|x|x.parse().unwrap()).collect(),
            description: default_description(),
            name: name.parse().unwrap(),
            short_name: short_name.parse().unwrap(),
            require_lvl: None,
            is_required_group: false,
        }
    }

    #[cfg(test)]
    pub fn new_required(short_name: &str, name: &str) -> Self {
        let mut new = Self::new(short_name, name, vec![]);
        new.is_required_group = true;
        new
    }

    /// Get all of the unique names used by this item
    pub fn unique_names(&self) -> Vec<NameString> {
        let mut names = vec![self.name.clone(), self.short_name.clone()];
        names.extend(self.aliases.iter().cloned());
        names.sort_unstable();
        names.dedup();
        return names
    }
}

/// A group granted access to an object
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClassificationGroup {
    /// List of alternate names for this group
    #[serde(default)]
    pub aliases: Vec<NameString>,

    /// This is a special flag that when set to true, if any groups are selected
    ///   in a classification. This group will automatically be selected too. (optional)
    #[serde(default)]
    pub auto_select: bool,

    /// Long form description of marking
    #[serde(default="default_description")]
    pub description: String,

    /// Long form canonical name of marking
    pub name: NameString,

    /// Short form canonical name of marking
    pub short_name: NameString,

    /// Assuming that this groups is the only group selected, this is the display name
    /// that will be used in the classification (that values has to be in the aliases
    /// of this group and only this group) (optional)
    #[serde(default)]
    pub solitary_display_name: Option<NameString>,
}

impl ClassificationGroup {
    #[cfg(test)]
    pub fn new(short_name: &str, name: &str) -> Self {
        Self {
            name: name.parse().unwrap(),
            short_name: short_name.parse().unwrap(),
            aliases: vec![],
            auto_select: false,
            description: default_description(),
            solitary_display_name: None
        }
    }

    #[cfg(test)]
    pub fn new_solitary(short_name: &str, name: &str, solitary_display: &str) -> Self {
        let mut new = Self::new(short_name, name);
        new.solitary_display_name = Some(solitary_display.parse().unwrap());
        return new
    }
}

/// A subgroup granted access to an object
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClassificationSubGroup {
    /// List of alternate names for the current marking
    #[serde(default)]
    pub aliases: Vec<NameString>,

    /// This is a special flag that when set to true, if any groups are selected
    ///   in a classification. This group will automatically be selected too. (optional)
    #[serde(default)]
    pub auto_select: bool,

    /// Long form description of marking
    #[serde(default="default_description")]
    pub description: String,

    /// Long form canonical name of marking
    pub name: NameString,

    /// Short form canonical name of marking
    pub short_name: NameString,

    /// Assuming that this groups is the only group selected, this is the display name
    /// that will be used in the classification (that values has to be in the aliases
    /// of this group and only this group) (optional)
    ///
    /// Loaded in the python version, but not actually used
    // #[serde(default)]
    // pub solitary_display_name: Option<String>,

    /// This is a special flag that auto-select the corresponding group when
    /// this subgroup is selected (optional)
    #[serde(default)]
    pub require_group: Option<NameString>,

    /// This is a special flag that makes sure that none other then the
    /// corresponding group is selected when this subgroup is selected (optional)
    #[serde(default)]
    pub limited_to_group: Option<NameString>,
}


impl ClassificationSubGroup {
    #[cfg(test)]
    pub fn new_aliased(short_name: &str, name: &str, aliases: Vec<&str>) -> Self {
        Self {
            short_name: short_name.parse().unwrap(),
            name: name.parse().unwrap(),
            aliases: aliases.iter().map(|item|item.parse().unwrap()).collect(),
            auto_select: false,
            description: default_description(),
            require_group: None,
            limited_to_group: None,
        }
    }

    #[cfg(test)]
    pub fn new_with_required(short_name: &str, name: &str, required: &str) -> Self {
        let mut new = Self::new_aliased(short_name, name, vec![]);
        new.require_group = Some(required.parse().unwrap());
        return new
    }

    #[cfg(test)]
    pub fn new_with_limited(short_name: &str, name: &str, limited: &str) -> Self {
        let mut new = Self::new_aliased(short_name, name, vec![]);
        new.limited_to_group = Some(limited.parse().unwrap());
        return new
    }
}


/// A string restricted to the conditions required for the name of a classification element.
/// Non-zero length and uppercase.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NameString(String);

impl std::str::FromStr for NameString {
    type Err = Errors;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let data = s.trim().to_uppercase();
        if data.is_empty() {
            return Err(Errors::ClassificationNameEmpty)
        }
        Ok(Self(data))
    }
}

impl core::fmt::Display for NameString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl NameString {
    /// Access the raw string data behind this object
    pub (crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for NameString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let data = <&str>::deserialize(deserializer)?;
        data.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for NameString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        self.0.serialize(serializer)
    }
}