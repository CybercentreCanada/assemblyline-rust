use std::collections::HashMap;

use serde::{Deserialize, Serialize};



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
    pub aliases: Vec<String>,

    /// Stylesheet applied in the UI for the different levels
    #[serde(default="default_css")]
    pub css: HashMap<String, String>,

    /// Description of the classification level
    #[serde(default="default_description")]
    pub description: String,

    /// Interger value of the Classification level (higher is more classified)
    pub lvl: i32,

    /// Long name of the classification item
    pub name: String,

    /// Short name of the classification item
    pub short_name: String,

    // #[serde(flatten)]
    // currently planning to static define other fields as optional, making other_fields unneeded
    // pub other_fields: HashMap<String, serde_value::Value>,
}

impl ClassificationLevel {
    #[cfg(test)]
    pub fn new(lvl: i32, short_name: String, name: String, aliases: Vec<&str>) -> Self {
        ClassificationLevel {
            aliases: aliases.into_iter().map(|x|x.to_owned()).collect(),
            css: default_css(),
            description: default_description(),
            lvl,
            name,
            short_name
        }
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
    pub aliases: Vec<String>,

    /// Long form description of marking
    #[serde(default="default_description")]
    pub description: String,

    /// Long form canonical name of marking
    pub name: String,

    /// Short form canonical name of marking
    pub short_name: String,

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
    pub fn new(short_name: String, name: String, aliases: Vec<&str>) -> Self {
        Self {
            aliases: aliases.into_iter().map(|x|x.to_owned()).collect(),
            description: default_description(),
            name,
            short_name,
            require_lvl: None,
            is_required_group: false,
        }
    }

    #[cfg(test)]
    pub fn new_required(short_name: String, name: String) -> Self {
        Self {
            aliases: vec![],
            description: default_description(),
            name,
            short_name,
            require_lvl: None,
            is_required_group: true,
        }
    }
}

/// A group granted access to an object
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClassificationGroup {
    /// List of alternate names for this group
    #[serde(default)]
    pub aliases: Vec<String>,

    /// This is a special flag that when set to true, if any groups are selected
    ///   in a classification. This group will automatically be selected too. (optional)
    #[serde(default)]
    pub auto_select: bool,

    /// Long form description of marking
    #[serde(default="default_description")]
    pub description: String,

    /// Long form canonical name of marking
    pub name: String,

    /// Short form canonical name of marking
    pub short_name: String,

    /// Assuming that this groups is the only group selected, this is the display name
    /// that will be used in the classification (that values has to be in the aliases
    /// of this group and only this group) (optional)
    #[serde(default)]
    pub solitary_display_name: Option<String>,
}

impl Default for ClassificationGroup {
    fn default() -> Self {
        Self {
            aliases: vec![],
            auto_select: false,
            description: default_description(),
            name: Default::default(),
            short_name: Default::default(),
            solitary_display_name: None
        }
    }
}

impl ClassificationGroup {
    #[cfg(test)]
    pub fn new(short_name: String, name: String) -> Self {
        Self {
            name,
            short_name,
            ..Default::default()
        }
    }
}

/// A subgroup granted access to an object
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClassificationSubGroup {
    /// List of alternate names for the current marking
    #[serde(default)]
    pub aliases: Vec<String>,

    /// This is a special flag that when set to true, if any groups are selected
    ///   in a classification. This group will automatically be selected too. (optional)
    #[serde(default)]
    pub auto_select: bool,

    /// Long form description of marking
    #[serde(default="default_description")]
    pub description: String,

    /// Long form canonical name of marking
    pub name: String,

    /// Short form canonical name of marking
    pub short_name: String,

    /// Assuming that this groups is the only group selected, this is the display name
    /// that will be used in the classification (that values has to be in the aliases
    /// of this group and only this group) (optional)
    #[serde(default)]
    pub solitary_display_name: Option<String>,

    /// This is a special flag that auto-select the corresponding group when
    /// this subgroup is selected (optional)
    #[serde(default)]
    pub require_group: Option<String>,

    /// This is a special flag that makes sure that none other then the
    /// corresponding group is selected when this subgroup is selected (optional)
    #[serde(default)]
    pub limited_to_group: Option<String>,
}

impl Default for ClassificationSubGroup {
    fn default() -> Self {
        Self {
            aliases: vec![],
            auto_select: false,
            description: default_description(),
            name: Default::default(),
            short_name: Default::default(),
            solitary_display_name: None,
            require_group: None,
            limited_to_group: None
        }
    }
}

// #[cfg(test)]
// mod test {
//     use serde_json::json;

//     use super::ClassificationLevel;

    // #[test]
    // fn test_level_param() {
    //     let level: ClassificationLevel = serde_json::from_value(json!({
    //         "aliases": [],
    //         "css": {},
    //         "description": "",
    //         "lvl": 0,
    //         "name": "",
    //         "short_name": "",
    //     })).unwrap();
    //     assert!(level.other_fields.is_empty());

    //     let level: ClassificationLevel = serde_json::from_value(json!({
    //         "aliases": [],
    //         "css": {},
    //         "description": "",
    //         "lvl": 0,
    //         "name": "",
    //         "short_name": "",
    //         "other_key": "other_value"
    //     })).unwrap();
    //     assert_eq!(level.other_fields, [("other_key".to_owned(), serde_value::to_value("other_value").unwrap())].into_iter().collect());

    //     let level: ClassificationLevel = serde_json::from_value(json!({
    //         "aliases": [],
    //         "css": {},
    //         "description": "",
    //         "lvl": 0,
    //         "name": "",
    //         "short_name": "",
    //         "other_key": 100
    //     })).unwrap();
    //     // Using u64 on this constant because of interplay between serde_json and serde_value's
    //     // choices around number types. If this ends up changing after an update just make the
    //     // type match as long as the numeric value is still the same.
    //     assert_eq!(level.other_fields, [("other_key".to_owned(), serde_value::to_value(100u64).unwrap())].into_iter().collect());
    // }

// }