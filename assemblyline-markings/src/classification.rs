//! Classification processing and manipulating tools
use std::collections::{HashSet, HashMap};

use itertools::Itertools;

use crate::config::{ClassificationConfig, DynamicGroupType, ClassificationLevel, ClassificationMarking, ClassificationSubGroup, ClassificationGroup};
use crate::errors::Errors;

/// A result that always uses the local error type
type Result<T> = std::result::Result<T, Errors>;

/// The smallest permitted classification level value
const MIN_LVL: i32 = 1;
/// The largest permitted classification level value
const MAX_LVL: i32 = 10000;
/// The classification level value used for null values
const NULL_LVL: i32 = 0;
/// The classification level value used for invalid values
const INVALID_LVL: i32 = 10001;
/// Short and long name used for null classification level
const NULL_CLASSIFICATION: &str = "NULL";
/// Short name used with invalid classification level
const INVALID_SHORT_CLASSIFICATION: &str = "INV";
/// Long name used with invalid classification level
const INVALID_CLASSIFICATION: &str = "INVALID";


/// A parser to process classification banners
#[derive(Default, Debug, PartialEq)]
pub struct ClassificationParser {
    /// The config object used to build this parser
    pub original_definition: ClassificationConfig,

    /// Should this parser enforce access control
    enforce: bool,

    dynamic_groups: bool,
    dynamic_groups_type: DynamicGroupType,

    levels: HashMap<i32, ClassificationLevel>,
    levels_map: HashMap<i32, String>,
    levels_scores_map: HashMap<String, i32>,
    levels_map_stl: HashMap<String, String>,
    levels_map_lts: HashMap<String, String>,
    levels_styles_map: HashMap<String, HashMap<String, String>>,
    levels_aliases: HashMap<String, String>,

    access_req: HashMap<String, ClassificationMarking>,
    access_req_map_lts: HashMap<String, String>,
    access_req_map_stl: HashMap<String, String>,
    access_req_aliases: HashMap<String, HashSet<String>>,

    groups: HashMap<String, ClassificationGroup>,
    groups_map_lts: HashMap<String, String>,
    groups_map_stl: HashMap<String, String>,
    groups_aliases: HashMap<String, HashSet<String>>,
    groups_auto_select: Vec<String>,
    groups_auto_select_short: Vec<String>,

    subgroups: HashMap<String, ClassificationSubGroup>,
    subgroups_map_lts: HashMap<String, String>,
    subgroups_map_stl: HashMap<String, String>,
    subgroups_aliases: HashMap<String, HashSet<String>>,
    subgroups_auto_select: Vec<String>,
    subgroups_auto_select_short: Vec<String>,

    // params_map: HashMap<String, String>,
    description: HashMap<String, String>,
    invalid_mode: bool,
    _classification_cache: HashSet<String>,
    _classification_cache_short: HashSet<String>,

    unrestricted: String,
    restricted: String,
}

/// A convenience trait that lets you pass true, false, or None for boolean arguments
pub trait IBool: Into<Option<bool>> + Copy {}
impl<T: Into<Option<bool>> + Copy> IBool for T {}

impl ClassificationParser {

    /// Load a classification parser from a configuration file
    pub fn load(path: &std::path::Path) -> Result<Self> {
        // Open the file
        let file = std::fs::File::open(path)?;
        Self::new(serde_yaml::from_reader(file)?)
    }

    /// Convert a config into a usable parser
    pub fn new(definition: ClassificationConfig) -> Result<Self> {
        let mut new = Self {
            original_definition: definition.clone(),
            enforce: definition.enforce,
            dynamic_groups: definition.dynamic_groups,
            dynamic_groups_type: definition.dynamic_groups_type,
            ..Default::default()
        };

        // Add Invalid classification
        new.levels_scores_map.insert(INVALID_SHORT_CLASSIFICATION.to_owned(), INVALID_LVL);
        new.levels_map.insert(INVALID_LVL, INVALID_SHORT_CLASSIFICATION.to_owned());
        new.levels_map_stl.insert(INVALID_SHORT_CLASSIFICATION.to_owned(), INVALID_CLASSIFICATION.to_owned());
        new.levels_map_lts.insert(INVALID_CLASSIFICATION.to_owned(), INVALID_SHORT_CLASSIFICATION.to_owned());

        // Add null classification
        new.levels_scores_map.insert(NULL_CLASSIFICATION.to_owned(), NULL_LVL);
        new.levels_map.insert(NULL_LVL, NULL_CLASSIFICATION.to_owned());
        new.levels_map_stl.insert(NULL_CLASSIFICATION.to_owned(), NULL_CLASSIFICATION.to_owned());
        new.levels_map_lts.insert(NULL_CLASSIFICATION.to_owned(), NULL_CLASSIFICATION.to_owned());

        // Convert the levels
        for x in definition.levels {
            let short_name = x.short_name.trim().to_uppercase();
            let name = x.name.trim().to_uppercase();

            if [INVALID_CLASSIFICATION, INVALID_SHORT_CLASSIFICATION, NULL_CLASSIFICATION].contains(&&short_name[..]) {
                return Err(Errors::InvalidDefinition("You cannot use reserved words NULL, INVALID or INV in your classification definition.".to_owned()));
            }
            if [INVALID_CLASSIFICATION, INVALID_SHORT_CLASSIFICATION, NULL_CLASSIFICATION].contains(&&name[..]) {
                return Err(Errors::InvalidDefinition("You cannot use reserved words NULL, INVALID or INV in your classification definition.".to_owned()));
            }

            if x.lvl > MAX_LVL {
                return Err(Errors::InvalidDefinition(format!("Level over maximum classification level of {MAX_LVL}.")))
            }
            if x.lvl < MIN_LVL {
                return Err(Errors::InvalidDefinition(format!("Level under minimum classification level of {MIN_LVL}.")))
            }

            if new.levels_scores_map.insert(short_name.to_owned(), x.lvl).is_some() {
                return Err(Errors::InvalidDefinition(format!("Duplicate classification name: {}", short_name)))
            }
            new.levels_map.insert(x.lvl, short_name.to_owned());
            new.levels_map_stl.insert(short_name.to_owned(), name.to_owned());
            new.levels_map_lts.insert(name.to_owned(), short_name.to_owned());
            for a in &x.aliases {
                new.levels_aliases.insert(a.trim().to_uppercase(), short_name.to_owned());
            }
            // new.params_map.insert(short_name.clone(), x.other_fields); // {k: v for k, v in x.items() if k not in banned_params_keys}
            // new.params_map[name] = new.params_map[short_name]
            new.levels_styles_map.insert(short_name.to_owned(), x.css.clone());
            new.levels_styles_map.insert(name.to_owned(),  x.css.clone());
            new.description.insert(short_name.to_owned(), x.description.clone());
            new.description.insert(name.to_owned(), x.description.clone());

            if let Some(old) = new.levels.insert(x.lvl, x) {
                return Err(Errors::InvalidDefinition(format!("Duplicate classification level: {}", old.lvl)))
            }
        }

        for mut x in definition.required {
            x.short_name = x.short_name.trim().to_uppercase();
            x.name = x.name.trim().to_uppercase();
            new.access_req_map_lts.insert(x.name.clone(), x.short_name.clone());
            new.access_req_map_stl.insert(x.short_name.clone(), x.name.clone());
            for a in &x.aliases {
                new.access_req_aliases.entry(a.trim().to_uppercase()).or_default().insert(x.short_name.clone());
            }
            // new.params_map[short_name] = {k: v for k, v in x.items() if k not in banned_params_keys}
            // new.params_map[name] = new.params_map[short_name]
            new.description.insert(x.short_name.clone(), x.description.clone());
            new.description.insert(x.name.clone(), x.description.clone());

            if x.name != x.short_name {
                if let Some(old) = new.access_req.insert(x.name.clone(), x.clone()) {
                    return Err(Errors::InvalidDefinition(format!("Duplicate required name: {}", old.name)))
                }
            }
            if let Some(old) = new.access_req.insert(x.short_name.clone(), x) {
                return Err(Errors::InvalidDefinition(format!("Duplicate required name: {}", old.short_name)))
            }
        }

        for mut x in definition.groups {
            x.short_name = x.short_name.trim().to_uppercase();
            x.name = x.name.trim().to_uppercase();
            new.groups_map_lts.insert(x.name.clone(), x.short_name.clone());
            new.groups_map_stl.insert(x.short_name.clone(), x.name.clone());
            for a in &x.aliases {
                new.groups_aliases.entry(a.trim().to_uppercase()).or_default().insert(x.short_name.clone());
            }
            if let Some(a) = &x.solitary_display_name {
                new.groups_aliases.entry(a.trim().to_uppercase()).or_default().insert(x.short_name.clone());
            }
            if x.auto_select {
                new.groups_auto_select.push(x.name.clone());
                new.groups_auto_select_short.push(x.short_name.clone());
            }
            // new.params_map[x.short_name] = {k: v for k, v in x.items() if k not in banned_params_keys}
            // new.params_map[name] = new.params_map[x.short_name]
            new.description.insert(x.short_name.clone(), x.description.clone());
            new.description.insert(x.name.clone(), x.description.clone());

            if x.name != x.short_name {
                if let Some(old) = new.groups.insert(x.name.clone(), x.clone()) {
                    return Err(Errors::InvalidDefinition(format!("Duplicate group name: {}", old.name)))
                }
            }
            if let Some(old) = new.groups.insert(x.short_name.clone(), x) {
                return Err(Errors::InvalidDefinition(format!("Duplicate group name: {}", old.short_name)))
            }
        }

        for mut x in definition.subgroups {
            x.short_name = x.short_name.trim().to_uppercase();
            x.name = x.name.trim().to_uppercase();
            new.subgroups_map_lts.insert(x.name.clone(), x.short_name.clone());
            new.subgroups_map_stl.insert(x.short_name.clone(), x.name.clone());
            for a in &x.aliases {
                new.subgroups_aliases.entry(a.trim().to_uppercase()).or_default().insert(x.short_name.clone());
            }
            // if let Some(a) = &x.solitary_display_name {
            //     new.subgroups_aliases.entry(a.trim().to_uppercase()).or_default().insert(x.short_name.clone());
            // }
            if x.auto_select {
                new.subgroups_auto_select.push(x.name.clone());
                new.subgroups_auto_select_short.push(x.short_name.clone());
            }
            // new.params_map[x.short_name] = {k: v for k, v in x.items() if k not in banned_params_keys}
            // new.params_map[name] = new.params_map[x.short_name]
            new.description.insert(x.short_name.clone(), x.description.clone());
            new.description.insert(x.name.clone(), x.description.clone());
            if x.name != x.short_name {
                if let Some(old) = new.subgroups.insert(x.name.clone(), x.clone()) {
                    return Err(Errors::InvalidDefinition(format!("Duplicate subgroup name: {}", old.name)))
                }
            }
            if let Some(old) = new.subgroups.insert(x.short_name.clone(), x) {
                return Err(Errors::InvalidDefinition(format!("Duplicate subgroup name: {}", old.short_name)))
            }
        }

        if !new.is_valid(&definition.unrestricted) {
            return Err(Errors::InvalidDefinition("Classification definition's unrestricted classification is invalid.".to_owned()));
        }

        if !new.is_valid(&definition.restricted) {
            return Err(Errors::InvalidDefinition("Classification definition's restricted classification is invalid.".to_owned()));
        }

        new.unrestricted = new.normalize_classification(&definition.unrestricted)?;
        new.restricted = new.normalize_classification(&definition.restricted)?;

        // except Exception as e:
        //     self.UNRESTRICTED = self.NULL_CLASSIFICATION
        //     self.RESTRICTED = self.INVALID_CLASSIFICATION

        //     self.invalid_mode = True

        //     log.warning(str(e))

        Ok(new)
    }


//     ############################
//     # Private functions
//     ############################
    // fn _build_combinations(items: &HashSet<String>) -> HashSet<String> {
    //     Self::_build_combinations_options(items, "/", &Default::default())
    // }

    // /// build the combination string
    // fn _build_combinations_options(items: &HashSet<String>, separator: &str, solitary_display: &HashMap<String, String>) -> HashSet<String> {
    //     let mut out = HashSet::<String>::from(["".to_owned()]);
    //     for i in items {
    //         let others = items.iter().filter(|x| *x != i).collect_vec();
    //         for x in 0..=others.len() {
    //             for c in others.iter().combinations(x) {
    //                 let mut value = vec![i];
    //                 value.extend(c);
    //                 value.sort_unstable();
    //                 let value = value.into_iter().join(separator);
    //                 out.insert(solitary_display.get(&value).unwrap_or(&value).clone());
    //             }
    //         }
    //     }
    //     return out
    // }

//     @staticmethod
//     def _list_items_and_aliases(data: List, long_format: bool = True) -> Set:
//         items = set()
//         for item in data:
//             if long_format:
//                 items.add(item['name'])
//             else:
//                 items.add(item['short_name'])

//         return items

    /// From the classification string get the level number
    fn _get_c12n_level_index(&self, c12n: &str) -> Result<i32> {
        // Parse classifications in uppercase mode only
        let c12n = c12n.trim().to_uppercase();

        let (lvl, _) = c12n.split_once("//").unwrap_or((&c12n, ""));
        if let Some(value) = self.levels_scores_map.get(lvl) {
            return Ok(*value)
        } else if let Some(lvl) = self.levels_map_lts.get(lvl) {
            return Ok(*self.levels_scores_map.get(lvl).unwrap())
        } else if let Some(lvl) = self.levels_aliases.get(lvl) {
            return Ok(*self.levels_scores_map.get(lvl).unwrap())
        }
        Err(Errors::InvalidClassification(format!("Classification level '{lvl}' was not found in your classification definition.")))
    }

    /// convert a level number to a text form
    fn _get_c12n_level_text(&self, lvl_idx: i32, long_format: bool) -> Result<String> {
        if let Some(short_name) = self.levels_map.get(&lvl_idx) {
            if long_format {
                match self.levels_map_stl.get(short_name) {
                    Some(long) => return Ok(long.clone()),
                    None => return Err(Errors::InvalidClassification(format!("Error finding long name of: {short_name}")))
                }
            }
            return Ok(short_name.clone())
        }

        Err(Errors::InvalidClassification(format!("Classification level number '{lvl_idx}' was not found in your classification definition.")))
    }

    /// Get required section items
    fn _get_c12n_required(&self, c12n: &str, long_format: impl IBool) -> Vec<String> {
        let long_format = long_format.into().unwrap_or(true);

        // Parse classifications in uppercase mode only
        let c12n = c12n.trim().to_uppercase();

        let mut return_set: Vec<&str> = vec![];

        for p in c12n.split('/') {
            if let Some(part) = self.access_req_map_lts.get(p) {
                return_set.push(part)
            } else if self.access_req_map_stl.contains_key(p) {
                return_set.push(p)
            }  else if let Some(aliases) = self.access_req_aliases.get(p) {
                for a in aliases {
                    return_set.push(a)
                }
            }
        }

        let mut return_set = if long_format {
            return_set
            .into_iter()
            .filter_map(|i| self.access_req_map_stl.get(i))
            .cloned().collect_vec()
        } else {
            return_set
            .into_iter()
            .map(str::to_owned)
            .collect_vec()
        };

        return_set.sort_unstable();
        return_set.dedup();
        return return_set
    }

    /// Get the groups and subgroups for a classification
    fn _get_c12n_groups(&self, c12n: &str,
        long_format: impl IBool,
        get_dynamic_groups: impl IBool
    ) -> Result<(Vec<String>, Vec<String>)> {
        let long_format = long_format.into().unwrap_or(true);
        let get_dynamic_groups = get_dynamic_groups.into().unwrap_or(true);

        // Parse classifications in uppercase mode only
        let c12n = c12n.trim().to_uppercase();

        let mut g1_set: Vec<&str> = vec![];
        let mut g2_set: Vec<&str> = vec![];
        let mut others = vec![];


        let mut groups = vec![];
        for gp in c12n.split("//") {
            let gp = gp.replace("REL TO ", "");
            let gp = gp.replace("REL ", "");
            // TODO check if these splits are the right way round
            for t in gp.split(',') {
                groups.extend(t.trim().split('/').map(|x|x.trim().to_owned()));
            }
        }

        for g in &groups {
            if let Some(g) = self.groups_map_lts.get(g) {
                g1_set.push(g);
            } else if self.groups_map_stl.contains_key(g) {
                g1_set.push(g);
            } else if let Some(aliases) = self.groups_aliases.get(g) {
                for a in aliases {
                    g1_set.push(a)
                }
            } else if let Some(g) = self.subgroups_map_lts.get(g) {
                g2_set.push(g);
            } else if self.subgroups_map_stl.contains_key(g) {
                g2_set.push(g);
            } else if let Some(aliases) = self.subgroups_aliases.get(g) {
                for a in aliases {
                    g2_set.push(a)
                }
            } else {
                others.push(g)
            }
        }

        if self.dynamic_groups && get_dynamic_groups {
            for o in others {
                if !self.access_req_map_lts.contains_key(o)
                && !self.access_req_map_stl.contains_key(o)
                && !self.access_req_aliases.contains_key(o)
                && !self.levels_scores_map.contains_key(o)
                && !self.levels_map_lts.contains_key(o)
                && !self.levels_aliases.contains_key(o)
                {
                    g1_set.push(o)
                }
            }
        }

        let (mut g1_set, mut g2_set) = if long_format {
            let g1: Result<Vec<&String>> = g1_set.into_iter()
                .map(|r| self.groups_map_stl.get(r).ok_or(Errors::InvalidClassification("".to_owned())))
                .collect();
            let g2: Result<Vec<&String>> = g2_set.into_iter()
                .map(|r| self.subgroups_map_stl.get(r).ok_or(Errors::InvalidClassification("".to_owned())))
                .collect();

            (g1?.into_iter().cloned().collect_vec(), g2?.into_iter().cloned().collect_vec())
        } else {
            (g1_set.into_iter().map(|r|r.to_owned()).collect_vec(), g2_set.into_iter().map(|r| r.to_owned()).collect_vec())
        };

        g1_set.sort_unstable();
        g1_set.dedup();
        g2_set.sort_unstable();
        g2_set.dedup();
        return Ok((g1_set, g2_set))
    }

    /// check if the user's access controls match the requirements
    fn _can_see_required(user_req: &Vec<String>, req: &Vec<String>) -> bool {
        let req: HashSet<&String> = HashSet::from_iter(req);
        let user_req = HashSet::from_iter(user_req);
        return req.is_subset(&user_req)
    }

    /// check if the user is in a group permitted dissemination
    fn _can_see_groups(user_groups: &Vec<String>, required_groups: &Vec<String>) -> bool {
        if required_groups.is_empty() {
            return true
        }

        for g in user_groups {
            if required_groups.contains(g) {
                return true
            }
        }

        return false
    }

    /// Put the given components back togeather into a classification string
    /// default long_format = true
    /// default skip_auto_select = false
    fn _get_normalized_classification_text(&self, parts: ParsedClassification, long_format: bool, skip_auto_select: bool) -> Result<String> {
        let ParsedClassification{level: lvl_idx, required: req, mut groups, mut subgroups} = parts;

        let group_delim = if long_format {"REL TO "} else {"REL "};

        // 1. Check for all required items if they need a specific classification lvl
        let mut required_lvl_idx = 0;
        for r in &req {
            if let Some(params) = self.access_req.get(r) {
                required_lvl_idx = required_lvl_idx.max(params.require_lvl.unwrap_or_default())
            }
        }
        let mut out = self._get_c12n_level_text(lvl_idx.max(required_lvl_idx), long_format)?;

        // 2. Check for all required items if they should be shown inside the groups display part
        let mut req_grp = vec![];
        for r in &req {
            if let Some(params) = self.access_req.get(r) {
                if params.is_required_group {
                    req_grp.push(r.clone());
                }
            }
        }
        // req = list(set(req).difference(set(req_grp)))
        let req = req.into_iter().filter(|item|!req_grp.contains(item)).collect_vec();

        if !req.is_empty() {
            out += &("//".to_owned() + &req.join("/"));
        }
        if !req_grp.is_empty() {
            req_grp.sort_unstable();
            out += &("//".to_owned() + &req_grp.join("/"));
        }

        // 3. Add auto-selected subgroups
        if long_format {
            if !subgroups.is_empty() && !self.subgroups_auto_select.is_empty() && !skip_auto_select {
                // subgroups = sorted(list(set(subgroups).union(set(self.subgroups_auto_select))))
                subgroups.extend(self.subgroups_auto_select.iter().cloned());
            }
        } else {
            if !subgroups.is_empty() && !self.subgroups_auto_select_short.is_empty() && !skip_auto_select {
                subgroups.extend(self.subgroups_auto_select_short.iter().cloned())
                // subgroups = sorted(list(set(subgroups).union(set(self.subgroups_auto_select_short))))
            }
        }
        subgroups.sort_unstable();
        subgroups.dedup();

        // 4. For every subgroup, check if the subgroup requires or is limited to a specific group
        let mut temp_groups = vec![];
        for sg in &subgroups {
            if let Some(subgroup) = self.subgroups.get(sg) {
                if let Some(require_group) = &subgroup.require_group {
                    temp_groups.push(require_group.clone())
                }

                if let Some(limited_to_group) = &subgroup.limited_to_group {
                    if temp_groups.contains(limited_to_group) {
                        temp_groups = vec![limited_to_group.clone()]
                    } else {
                        temp_groups.clear()
                    }
                }
            }
        }

        for g in &temp_groups {
            if long_format {
                groups.push(self.groups_map_stl.get(g).unwrap_or(g).clone())
            } else {
                groups.push(self.groups_map_lts.get(g).unwrap_or(g).clone())
            }
        }

        // 5. Add auto-selected groups
        if long_format {
            if !groups.is_empty() && !self.groups_auto_select.is_empty() && !skip_auto_select {
                groups.extend(self.groups_auto_select.iter().cloned());
            }
        } else {
            if !groups.is_empty() && !self.groups_auto_select_short.is_empty() && !skip_auto_select {
                groups.extend(self.groups_auto_select_short.iter().cloned());
            }
        }
        groups.sort_unstable();
        groups.dedup();

        if !groups.is_empty() {
            out += if req_grp.is_empty() {"//"} else {"/"};
            if groups.len() == 1 {
                // 6. If only one group, check if it has a solitary display name.
                let grp = &groups[0];
                if let Some(group_data) = self.groups.get(grp) {
                    if let Some(display_name) = &group_data.solitary_display_name {
                        out += display_name;
                    } else {
                        out += group_delim;
                        out += grp;
                    }
                }
            } else {
                if !long_format {
                    // 7. In short format mode, check if there is an alias that can replace multiple groups
                    let group_set: HashSet<String> = groups.iter().cloned().collect();
                    for (alias, values) in self.groups_aliases.iter() {
                        if values.len() > 1 && *values == group_set {
                            groups = vec![alias.clone()]
                        }
                    }
                }
                out += group_delim;
                out += &groups.join(", ");
            }
        }

        if !subgroups.is_empty() {
            if groups.is_empty() && req_grp.is_empty() {
                out += "//"
            } else {
                out += "/"
            }
            subgroups.sort_unstable();
            out += &subgroups.join("/");
        }

        return Ok(out)
    }

    /// Break a classification into its parts
    pub fn get_classification_parts(&self, c12n: &str, long_format: impl IBool, get_dynamic_groups: impl IBool) -> Result<ParsedClassification> {
        let level = self._get_c12n_level_index(c12n)?;
        let required = self._get_c12n_required(c12n, long_format);
        let (groups, subgroups) = self._get_c12n_groups(c12n, long_format, get_dynamic_groups)?;

        Ok(ParsedClassification { level, required, groups, subgroups })
    }

    /// Listing all classifcation permutations can take a really long time the more the classification
    /// definition is complexe. Normalizing each entry makes it even worst. Use only this function if
    /// absolutely necessary.
    // pub fn list_all_classification_combinations(self, long_format: bool = True, normalized: bool = False) -> Set {

    //     combinations = set()

    //     levels = self._list_items_and_aliases(self.original_definition['levels'], long_format=long_format)
    //     reqs = self._list_items_and_aliases(self.original_definition['required'], long_format=long_format)
    //     grps = self._list_items_and_aliases(self.original_definition['groups'], long_format=long_format)
    //     sgrps = self._list_items_and_aliases(self.original_definition['subgroups'], long_format=long_format)

    //     req_cbs = self._build_combinations(reqs)
    //     if long_format:
    //         grp_solitary_display = {
    //             x['name']: x['solitary_display_name'] for x in self.original_definition['groups']
    //             if 'solitary_display_name' in x
    //         }
    //     else:
    //         grp_solitary_display = {
    //             x['short_name']: x['solitary_display_name'] for x in self.original_definition['groups']
    //             if 'solitary_display_name' in x
    //         }
    //     solitary_names = [x['solitary_display_name'] for x in self.original_definition['groups']
    //                       if 'solitary_display_name' in x]

    //     grp_cbs = self._build_combinations(grps, separator=", ", solitary_display=grp_solitary_display)
    //     sgrp_cbs = self._build_combinations(sgrps)

    //     for p in itertools.product(levels, req_cbs):
    //         cl = "//".join(p)
    //         if cl.endswith("//"):
    //             combinations.add(cl[:-2])
    //         else:
    //             combinations.add(cl)

    //     temp_combinations = copy(combinations)
    //     for p in itertools.product(temp_combinations, grp_cbs):
    //         cl = "//REL TO ".join(p)
    //         if cl.endswith("//REL TO "):
    //             combinations.add(cl[:-9])
    //         else:
    //             combinations.add(cl)

    //     for sol_name in solitary_names:
    //         to_edit = []
    //         to_find = "REL TO {sol_name}".format(sol_name=sol_name)
    //         for c in combinations:
    //             if to_find in c:
    //                 to_edit.append(c)

    //         for e in to_edit:
    //             combinations.add(e.replace(to_find, sol_name))
    //             combinations.remove(e)

    //     temp_combinations = copy(combinations)
    //     for p in itertools.product(temp_combinations, sgrp_cbs):
    //         if "//REL TO " in p[0]:
    //             cl = "/".join(p)

    //             if cl.endswith("/"):
    //                 combinations.add(cl[:-1])
    //             else:
    //                 combinations.add(cl)
    //         else:
    //             cl = "//REL TO ".join(p)

    //             if cl.endswith("//REL TO "):
    //                 combinations.add(cl[:-9])
    //             else:
    //                 combinations.add(cl)

    //     if normalized:
    //         return {self.normalize_classification(x, long_format=long_format) for x in combinations}
    //     return combinations
    // }

//     # noinspection PyUnusedLocal
//     def default_user_classification(self, user: Optional[str] = None, long_format: bool = True) -> str:
//         """
//         You can overload this function to specify a way to get the default classification of a user.
//         By default, this function returns the UNRESTRICTED value of your classification definition.

//         Args:
//             user: Which user to get the classification for
//             long_format: Request a long classification format or not

//         Returns:
//             The classification in the specified format
//         """
//         return self.UNRESTRICTED

//     def get_parsed_classification_definition(self) -> Dict:
//         """
//         Returns all dictionary of all the variables inside the classification object that will be used
//         to enforce classification throughout the system.
//         """
//         from copy import deepcopy
//         out = deepcopy(self.__dict__)
//         out['levels_map'].pop("INV", None)
//         out['levels_map'].pop(str(self.INVALID_LVL), None)
//         out['levels_map_stl'].pop("INV", None)
//         out['levels_map_lts'].pop("INVALID", None)
//         out['levels_map'].pop("NULL", None)
//         out['levels_map'].pop(str(self.NULL_LVL), None)
//         out['levels_map_stl'].pop("NULL", None)
//         out['levels_map_lts'].pop("NULL", None)
//         out.pop('_classification_cache', None)
//         out.pop('_classification_cache_short', None)
//         return out

    /// Returns a dictionary containing the different access parameters Lucene needs to build it's queries
    ///
    /// Args:
    ///     c12n: The classification to get the parts from
    ///     user_classification: Is a user classification, (old default = false)
    pub fn get_access_control_parts(&self, c12n: &str, user_classification: bool) -> Result<serde_json::Value> {
        let c12n = if !self.enforce || self.invalid_mode {
            self.unrestricted.clone()
        } else {
            c12n.to_owned()
        };

        let result: Result<serde_json::Value> = (||{
            // Normalize the classification before gathering the parts
            let c12n = self.normalize_classification_options(&c12n, NormalizeOptions { skip_auto_select: user_classification, ..Default::default()})?;

            let access_lvl = self._get_c12n_level_index(&c12n)?;
            let access_req = self._get_c12n_required(&c12n, false);
            let (access_grp1, access_grp2) = self._get_c12n_groups(&c12n, false, true)?;

            return Ok(serde_json::json!({
                "__access_lvl__": access_lvl,
                "__access_req__": access_req,
                "__access_grp1__": if access_grp1.is_empty() { vec!["__EMPTY__".to_owned()] } else { access_grp1 },
                "__access_grp2__": if access_grp2.is_empty() { vec!["__EMPTY__".to_owned()] } else { access_grp2 }
            }))
        })();

        if let Err(Errors::InvalidClassification(_)) = &result {
            if !self.enforce || self.invalid_mode {
                return Ok(serde_json::json!({
                    "__access_lvl__": NULL_LVL,
                    "__access_req__": [],
                    "__access_grp1__": ["__EMPTY__"],
                    "__access_grp2__": ["__EMPTY__"]
                }))
            }
        }
        return result
    }

//     def get_access_control_req(self) -> Union[KeysView, List]:
//         """
//         Returns a list of the different possible REQUIRED parts
//         """
//         if not self.enforce or self.invalid_mode:
//             return []

//         return self.access_req_map_stl.keys()

//     def get_access_control_groups(self) -> Union[KeysView, List]:
//         """
//         Returns a list of the different possible GROUPS
//         """
//         if not self.enforce or self.invalid_mode:
//             return []

//         return self.groups_map_stl.keys()

//     def get_access_control_subgroups(self) -> Union[KeysView, List]:
//         """
//         Returns a list of the different possible SUBGROUPS
//         """
//         if not self.enforce or self.invalid_mode:
//             return []

//         return self.subgroups_map_stl.keys()

    /// This function intersects two user classification to return the maximum classification
    /// that both user could see.
    ///
    /// Args:
    ///     user_c12n_1: First user classification
    ///     user_c12n_2: Second user classification
    ///     long_format: True/False in long format
    ///
    /// Returns:
    ///     Intersected classification in the desired format
    pub fn intersect_user_classification(&self, user_c12n_1: &str, user_c12n_2: &str, long_format: impl IBool) -> Result<String> {
        let long_format = long_format.into().unwrap_or(true);
        if !self.enforce || self.invalid_mode {
            return Ok(self.unrestricted.clone())
        }

        // Normalize classifications before comparing them
        let user_c12n_1 = self.normalize_classification_options(user_c12n_1, NormalizeOptions { skip_auto_select: true, ..Default::default() })?;
        let user_c12n_2 = self.normalize_classification_options(user_c12n_2, NormalizeOptions { skip_auto_select: true, ..Default::default() })?;

        let parts1 = self.get_classification_parts(&user_c12n_1, long_format, None)?;
        let parts2 = self.get_classification_parts(&user_c12n_2, long_format, None)?;

        let parts = ParsedClassification {
            level: parts1.level.min(parts2.level),
            required: intersection(&parts1.required, &parts2.required),
            groups: intersection(&parts1.groups, &parts2.groups),
            subgroups: intersection(&parts1.subgroups, &parts2.subgroups),
        };

        return self._get_normalized_classification_text(parts, long_format, true)
    }

    /// Given a user classification, check if a user is allow to see a certain classification
    ///
    /// Args:
    ///     user_c12n: Maximum classification for the user
    ///     c12n: Classification the user which to see
    /// , ignore_invalid: bool = False
    /// Returns:
    ///     True is the user can see the classification
    pub fn is_accessible(&self, user_c12n: &str, c12n: &str) -> Result<bool> {
        if self.invalid_mode {
            return Ok(false)
        }

        if !self.enforce {
            return Ok(true)
        }

        // if c12n is None:
        //     return True

        // Normalize classifications before comparing them
        let user_c12n = self.normalize_classification_options(user_c12n, NormalizeOptions{skip_auto_select: true, ..Default::default()})?;
        let c12n = self.normalize_classification_options(c12n, NormalizeOptions{skip_auto_select: true, ..Default::default()})?;

        let user_req = self._get_c12n_required(&user_c12n, None);
        let (user_groups, user_subgroups) = self._get_c12n_groups(&user_c12n, None, None)?;
        let req = self._get_c12n_required(&c12n, None);
        let (groups, subgroups) = self._get_c12n_groups(&c12n, None, None)?;

        if self._get_c12n_level_index(&user_c12n)? >= self._get_c12n_level_index(&c12n)? {
            if !Self::_can_see_required(&user_req, &req) {
                return Ok(false)
            }
            if !Self::_can_see_groups(&user_groups, &groups) {
                return Ok(false)
            }
            if !Self::_can_see_groups(&user_subgroups, &subgroups) {
                return Ok(false)
            }
            return Ok(true)
        }
        return Ok(false)
    }

    /// Check if the given classification banner can be interpreted
    pub fn is_valid(&self, c12n: &str) -> bool {
        self.is_valid_skip_auto(c12n, false)
    }

    /// Performs a series of checks against a classification to make sure it is valid in it's current form
    ///
    /// Args:
    ///     c12n: The classification we want to validate
    ///     skip_auto_select: skip the auto selection phase
    ///
    /// Returns:
    ///     True if the classification is valid
    pub fn is_valid_skip_auto(&self, c12n: &str, skip_auto_select: bool) -> bool {
        if !self.enforce {
            return true;
        }


        // Classification normalization test
        let n_c12n = match self.normalize_classification_options(c12n, NormalizeOptions{skip_auto_select, ..Default::default()}) {
            Ok(n_c12n) => n_c12n,
            Err(_) => return false,
        };

        // parse the classification + normal form into parts
        let ParsedClassification{level: lvl_idx, required: mut req, mut groups, mut subgroups} = match self.get_classification_parts(c12n, None, None) {
            Ok(row) => row,
            Err(_) => return false,
        };
        let ParsedClassification{level: n_lvl_idx, required: mut n_req, groups: mut n_groups, subgroups: mut n_subgroups} = match self.get_classification_parts(&n_c12n, None, None) {
            Ok(row) => row,
            Err(_) => return false,
        };

        if lvl_idx != n_lvl_idx { return false }

        req.sort_unstable();
        n_req.sort_unstable();
        if req != n_req { return false }

        groups.sort_unstable();
        n_groups.sort_unstable();
        if groups != n_groups { return false }

        subgroups.sort_unstable();
        n_subgroups.sort_unstable();
        if subgroups != n_subgroups { return false; }

        let c12n = c12n.replace("REL TO ", "");
        let c12n = c12n.replace("REL ", "");
        let parts = c12n.split("//").collect_vec();

        // There is a maximum of 3 parts
        if parts.len() > 3 {
            return false
        }


        // First parts as to be a classification level part
        let mut parts = parts.iter();
        let first = *match parts.next() {
            Some(part) => part,
            None => return false,
        };
        if !self.levels_aliases.contains_key(first) & !self.levels_map_lts.contains_key(first) & !self.levels_map_stl.contains_key(first) {
            return false;
        }

        let mut check_groups = false;
        for cur_part in parts {
            // Can't be two groups sections.
            if check_groups { return false }

            let mut items = cur_part.split('/').collect_vec();
            let mut comma_idx = None;
            for (idx, i) in items.iter().enumerate() {
                if i.contains(',') {
                    if comma_idx.is_some() {
                        return false;
                    } else {
                        comma_idx = Some(idx)
                    }
                }
            }

            if let Some(comma_idx) = comma_idx {
                let value = items.remove(comma_idx);
                items.extend(value.split(',').map(str::trim))
            }

            for i in items {
                if !check_groups {
                    // If current item not found in access req, we might already be dealing with groups
                    if !self.access_req_aliases.contains_key(i) &&
                       !self.access_req_map_stl.contains_key(i) &&
                       !self.access_req_map_lts.contains_key(i)
                    {
                        check_groups = true
                    }
                }

                if check_groups && !self.dynamic_groups {
                    // If not groups. That stuff does not exists...
                    if !self.groups_aliases.contains_key(i) &&
                       !self.groups_map_stl.contains_key(i) &&
                       !self.groups_map_lts.contains_key(i) &&
                       !self.subgroups_aliases.contains_key(i) &&
                       !self.subgroups_map_stl.contains_key(i) &&
                       !self.subgroups_map_lts.contains_key(i)
                    {
                        return false
                    }
                }
            }
        }
        return true
    }

    /// Mixes to classification and returns to most restrictive form for them
    ///
    /// Args:
    ///     c12n_1: First classification
    ///     c12n_2: Second classification
    ///     long_format: True/False in long format, defaulted to true
    ///
    /// Returns:
    ///     The most restrictive classification that we could create out of the two
    pub fn max_classification(&self, c12n_1: &str, c12n_2: &str, long_format: impl IBool) -> Result<String> {
        let long_format = long_format.into().unwrap_or(true);

        if !self.enforce || self.invalid_mode {
            return Ok(self.unrestricted.clone())
        }

        // Normalize classifications before comparing them
        let c12n_1 = self.normalize_classification(c12n_1)?;
        let c12n_2 = self.normalize_classification(c12n_2)?;

        let parts1 = self.get_classification_parts(&c12n_1, long_format, None)?;
        let parts2 = self.get_classification_parts(&c12n_2, long_format, None)?;

        let parts = parts1.max(&parts2)?;

        return self._get_normalized_classification_text(parts, long_format, false)
    }

    /// Mixes to classification and returns to least restrictive form for them
    ///
    /// Args:
    ///     c12n_1: First classification
    ///     c12n_2: Second classification
    ///     long_format: True/False in long format
    ///
    /// Returns:
    ///     The least restrictive classification that we could create out of the two
    pub fn min_classification(&self, c12n_1: &str, c12n_2: &str, long_format: impl IBool) -> Result<String> {
        let long_format = long_format.into().unwrap_or(true);

        if !self.enforce || self.invalid_mode {
            return Ok(self.unrestricted.clone())
        }

        // Normalize classifications before comparing them
        let c12n_1 = self.normalize_classification(c12n_1)?;
        let c12n_2  = self.normalize_classification(c12n_2)?;

        // if c12n_1 is None:
        //     return c12n_2
        // if c12n_2 is None:
        //     return c12n_1

        let parts1 = self.get_classification_parts(&c12n_1, long_format, None)?;
        let parts2 = self.get_classification_parts(&c12n_2, long_format, None)?;

        let parts = parts1.min(&parts2);

        return self._get_normalized_classification_text(parts, long_format, false)
    }

    /// call normalize_classification_options with default arguments
    pub fn normalize_classification(&self, c12n: &str) -> Result<String> {
        self.normalize_classification_options(c12n, Default::default())
    }

    /// Normalize a given classification by applying the rules defined in the classification definition.
    /// This function will remove any invalid parts and add missing parts to the classification.
    /// It will also ensure that the display of the classification is always done the same way
    ///
    /// Args:
    ///     c12n: Classification to normalize
    ///     long_format: True/False in long format
    ///     skip_auto_select: True/False skip group auto adding, use True when dealing with user's classifications
    ///
    /// Returns:
    ///     A normalized version of the original classification
    pub fn normalize_classification_options(&self, c12n: &str, options: NormalizeOptions) -> Result<String> {
        let NormalizeOptions{long_format, skip_auto_select, get_dynamic_groups} = options;

        if !self.enforce || self.invalid_mode {
            return Ok(self.unrestricted.clone())
        }

        // Has the classification has already been normalized before?
        // if long_format and c12n in self._classification_cache and get_dynamic_groups:
        //     return c12n
        // if not long_format and c12n in self._classification_cache_short and get_dynamic_groups:
        //     return c12n

        let parts = self.get_classification_parts(c12n, long_format, get_dynamic_groups)?;
        let new_c12n = self._get_normalized_classification_text(parts, long_format, skip_auto_select)?;
        // if long_format {
        //     self._classification_cache.add(new_c12n)
        // } else {
        //     self._classification_cache_short.add(new_c12n)
        // }

        return Ok(new_c12n)
    }

//     def build_user_classification(self, c12n_1: str, c12n_2: str, long_format: bool = True) -> str:
//         """
//         Mixes to classification and return the classification marking that would give access to the most data

//         Args:
//             c12n_1: First classification
//             c12n_2: Second classification
//             long_format: True/False in long format

//         Returns:
//             The classification that would give access to the most data
//         """
//         if not self.enforce or self.invalid_mode:
//             return self.UNRESTRICTED

//         # Normalize classifications before comparing them
//         if c12n_1 is not None:
//             c12n_1 = self.normalize_classification(c12n_1, skip_auto_select=True)
//         if c12n_2 is not None:
//             c12n_2 = self.normalize_classification(c12n_2, skip_auto_select=True)

//         if c12n_1 is None:
//             return c12n_2
//         if c12n_2 is None:
//             return c12n_1

//         lvl_idx_1, req_1, groups_1, subgroups_1 = self._get_classification_parts(c12n_1, long_format=long_format)
//         lvl_idx_2, req_2, groups_2, subgroups_2 = self._get_classification_parts(c12n_2, long_format=long_format)

//         req = list(set(req_1) | set(req_2))
//         groups = list(set(groups_1) | set(groups_2))
//         subgroups = list(set(subgroups_1) | set(subgroups_2))

//         return self._get_normalized_classification_text(max(lvl_idx_1, lvl_idx_2), req, groups, subgroups,
//                                                         long_format=long_format, skip_auto_select=True)



}

#[derive(Debug, PartialEq, Default)]
pub struct ParsedClassification {
    pub level: i32,
    pub required: Vec<String>,
    pub groups: Vec<String>,
    pub subgroups: Vec<String>,
}

/// Gather the intersection of two string vectors
fn intersection(a: &Vec<String>, b: &Vec<String>) -> Vec<String> {
    HashSet::<&String>::from_iter(a).intersection(&HashSet::from_iter(b)).map(|&r|r.clone()).collect()
}

/// Gather the union of two string vectors
fn union(a: &Vec<String>, b: &Vec<String>) -> Vec<String> {
    let mut out = a.clone();
    out.extend(b.iter().cloned());
    out.sort_unstable();
    out.dedup();
    out
}


impl ParsedClassification {
    /// Calculate the minimum access requirements across two classifications
    fn min(&self, other: &Self) -> Self {
        let required = intersection(&self.required, &other.required);

        let groups = if self.groups.is_empty() || other.groups.is_empty() {
            vec![]
        } else {
            union(&self.groups, &other.groups)
        };

        let subgroups = if self.subgroups.is_empty() || other.subgroups.is_empty() {
            vec![]
        } else {
            union(&self.subgroups, &other.subgroups)
        };

        Self {
            level: self.level.min(other.level),
            required,
            groups,
            subgroups,
        }
    }

    fn _max_groups(groups_1: &Vec<String>, groups_2: &Vec<String>) -> Result<Vec<String>> {
        let groups = if groups_1.len() > 0 && groups_2.len() > 0 {
            intersection(groups_1, groups_2)
            // set(groups_1) & set(groups_2)
        } else {
            union(groups_1, groups_2)
            // set(groups_1) | set(groups_2)
        };

        if groups_1.len() > 0 && groups_2.len() > 0 && groups.len() == 0 {
            // NOTE: Intersection generated nothing, we will raise an InvalidClassification exception
            return Err(Errors::InvalidClassification(format!("Could not find any intersection between the groups. {groups_1:?} & {groups_2:?}")))
        }

        return Ok(groups)
    }

    pub fn max(&self, other: &Self) -> Result<Self> {
        let level = self.level.max(other.level);
        let required = union(&self.required, &other.required);

        let groups = Self::_max_groups(&self.groups, &other.groups)?;
        let subgroups = Self::_max_groups(&self.subgroups, &other.subgroups)?;

        Ok(Self {
            level,
            required,
            groups,
            subgroups,
        })
    }
}

pub struct NormalizeOptions {
    pub long_format: bool,
    pub skip_auto_select: bool,
    pub get_dynamic_groups: bool
}

impl Default for NormalizeOptions {
    fn default() -> Self {
        Self { long_format: true, skip_auto_select: false, get_dynamic_groups: true }
    }
}

impl NormalizeOptions {
    pub fn short() -> Self {
        Self{long_format: false, ..Default::default()}
    }
}

#[cfg(test)]
mod test {

    // #[test]
    // fn defaults() {
    //     let option = PartsOptions{long_format: false, ..Default::default()};
    //     assert!(!option.long_format);
    //     assert!(option.get_dynamic_groups);
    // }

    use crate::classification::{NormalizeOptions, ParsedClassification};
    use crate::config::{ClassificationConfig, ClassificationLevel, ClassificationGroup, ClassificationMarking, ClassificationSubGroup};

    use super::{ClassificationParser, Result};

    fn setup_config() -> ClassificationConfig {
        ClassificationConfig{
            enforce: true,
            dynamic_groups: false,
            dynamic_groups_type: crate::config::DynamicGroupType::All,
            levels: vec![
                ClassificationLevel::new(1, "L0".to_owned(), "Level 0".to_owned(), vec!["Open"]),
                ClassificationLevel::new(5, "L1".to_owned(), "Level 1".to_owned(), vec![]),
                ClassificationLevel::new(15, "L2".to_owned(), "Level 2".to_owned(), vec![]),
            ],
            groups: vec![
                ClassificationGroup::new("A".to_owned(), "Group A".to_owned()),
                ClassificationGroup::new("B".to_owned(), "Group B".to_owned()),
                ClassificationGroup{short_name: "X".to_owned(), name: "Group X".to_owned(), solitary_display_name: Some("XX".to_owned()), ..Default::default()},
            ],
            required: vec![
                ClassificationMarking::new("LE".to_owned(), "Legal Department".to_owned(), vec!["Legal"]),
                ClassificationMarking::new("AC".to_owned(), "Accounting".to_owned(), vec!["Acc"]),
                ClassificationMarking::new_required("orcon".to_owned(), "Originator Controlled".to_owned()),
                ClassificationMarking::new_required("nocon".to_owned(), "No Contractor Access".to_owned()),
            ],
            subgroups: vec![
                ClassificationSubGroup{short_name: "R1".to_owned(), name: "Reserve One".to_owned(), aliases: vec!["R0".to_owned()], ..Default::default()},
                ClassificationSubGroup{short_name: "R2".to_owned(), name: "Reserve Two".to_owned(), require_group: Some("X".to_owned()), ..Default::default()},
            ],
            restricted: "L2".to_owned(),
            unrestricted: "L0".to_owned(),
        }
    }

    fn setup() -> ClassificationParser {
        ClassificationParser::new(setup_config()).unwrap()
    }

    #[test]
    fn load_yaml() {
        let yaml = serde_yaml::to_string(&setup_config()).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), yaml).unwrap();
        assert_eq!(ClassificationParser::load(file.path()).unwrap(), setup());
    }

    #[test]
    fn load_json() {
        let json = serde_json::to_string(&setup_config()).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), json).unwrap();
        assert_eq!(ClassificationParser::load(file.path()).unwrap(), setup());
    }

    #[test]
    fn invalid_classifications() {
        let mut config = setup_config();

        // bad short names
        assert!(ClassificationParser::new(config.clone()).is_ok());
        config.levels[1].short_name = "INV".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());
        config.levels[1].short_name = "NULL".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // bad long names
        let mut config = setup_config();
        config.levels[1].name = "INV".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());
        config.levels[1].name = "NULL".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping level names
        let mut config = setup_config();
        config.levels[0].short_name = "L0".to_owned();
        config.levels[1].short_name = "L0".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping level
        let mut config = setup_config();
        config.levels[0].lvl = 100;
        config.levels[1].lvl = 100;
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping required names
        let mut config = setup_config();
        config.required[0].short_name = "AA".to_owned();
        config.required[1].short_name = "AA".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping required names
        let mut config = setup_config();
        config.required[0].name = "AA".to_owned();
        config.required[1].name = "AA".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping groups names
        let mut config = setup_config();
        config.groups[0].short_name = "AA".to_owned();
        config.groups[1].short_name = "AA".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping groups names
        let mut config = setup_config();
        config.groups[0].name = "AA".to_owned();
        config.groups[1].name = "AA".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping subgroups names
        let mut config = setup_config();
        config.subgroups[0].short_name = "AA".to_owned();
        config.subgroups[1].short_name = "AA".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // overlapping subgroups names
        let mut config = setup_config();
        config.subgroups[0].name = "AA".to_owned();
        config.subgroups[1].name = "AA".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // missing restricted
        let mut config = setup_config();
        config.restricted = "XF".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // missing unrestricted
        let mut config = setup_config();
        config.unrestricted = "XF".to_owned();
        assert!(ClassificationParser::new(config.clone()).is_err());

        // Use levels outside of range
        let mut config = setup_config();
        config.levels[0].lvl = 0;
        assert!(ClassificationParser::new(config.clone()).is_err());
        config.levels[0].lvl = 10002;
        assert!(ClassificationParser::new(config.clone()).is_err());
    }

    #[test]
    fn bad_commas() {
        let ce = setup();

        assert!(ce.is_valid("L1//REL A, B/ORCON/NOCON"));
        assert!(!ce.is_valid("L1//REL A, B/ORCON,NOCON"));
        assert!(!ce.is_valid("L1//ORCON,NOCON/REL A, B"));

        assert_eq!(ce.normalize_classification_options("L1//REL A, B/ORCON/NOCON", NormalizeOptions::short()).unwrap(), "L1//NOCON/ORCON/REL A, B");

        // todo, these aren't rejected, instead produce bad outputs
        // assert!(ce.normalize_classification("L1//REL A, B/ORCON,NOCON").is_err());
        // assert!(ce.normalize_classification("L1//ORCON,NOCON/REL A, B").is_err());
    }

    #[test]
    fn minimums() {
        let ce = setup();

        // level only
        assert_eq!(ce.min_classification("L0", "L0", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L0", "L0", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L0", "L1", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L0", "L1", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L0", "L2", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L0", "L2", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L1", "L0", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L1", "L0", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L1", "L1", false).unwrap(), "L1");
        assert_eq!(ce.min_classification("L1", "L1", true).unwrap(), "LEVEL 1");
        assert_eq!(ce.min_classification("L1", "L2", false).unwrap(), "L1");
        assert_eq!(ce.min_classification("L1", "L2", true).unwrap(), "LEVEL 1");
        assert_eq!(ce.min_classification("L2", "L0", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L2", "L0", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L2", "L1", false).unwrap(), "L1");
        assert_eq!(ce.min_classification("L2", "L1", true).unwrap(), "LEVEL 1");
        assert_eq!(ce.min_classification("L2", "L2", false).unwrap(), "L2");
        assert_eq!(ce.min_classification("L2", "L2", true).unwrap(), "LEVEL 2");
        assert_eq!(ce.min_classification("OPEN", "L2", false).unwrap(), "L0");

        // Group operations
        assert_eq!(ce.min_classification("L0//REL A, B", "L0", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L0//REL A", "L0", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L0", "L2//REL A, B", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L0", "L1//REL A", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L0//REL A, B", "L1//REL A, B", false).unwrap(), "L0//REL A, B");
        assert_eq!(ce.min_classification("L0//REL A, B", "L0//REL A", true).unwrap(), "LEVEL 0//REL TO GROUP A, GROUP B");
        assert_eq!(ce.min_classification("L0//REL B", "L0//REL B, A", true).unwrap(), "LEVEL 0//REL TO GROUP A, GROUP B");

        // Subgroups
        assert_eq!(ce.min_classification("L0//R1/R2", "L0", false).unwrap(), "L0");
        assert_eq!(ce.min_classification("L0//R1", "L0", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.min_classification("L0//R1/R2", "L1//R1/R2", false).unwrap(), "L0//XX/R1/R2");
        assert_eq!(ce.min_classification("L0//R1/R2", "L0//R1", true).unwrap(), "LEVEL 0//XX/RESERVE ONE/RESERVE TWO");
    }

    #[test]
    fn maximums() {
        let ce = setup();

        // level only
        assert_eq!(ce.max_classification("L0", "L0", false).unwrap(), "L0");
        assert_eq!(ce.max_classification("L0", "L0", true).unwrap(), "LEVEL 0");
        assert_eq!(ce.max_classification("L0", "L1", false).unwrap(), "L1");
        assert_eq!(ce.max_classification("L0", "L1", true).unwrap(), "LEVEL 1");
        assert_eq!(ce.max_classification("L0", "L2", false).unwrap(), "L2");
        assert_eq!(ce.max_classification("L0", "L2", true).unwrap(), "LEVEL 2");
        assert_eq!(ce.max_classification("L1", "L0", false).unwrap(), "L1");
        assert_eq!(ce.max_classification("L1", "L0", true).unwrap(), "LEVEL 1");
        assert_eq!(ce.max_classification("L1", "L1", false).unwrap(), "L1");
        assert_eq!(ce.max_classification("L1", "L1", true).unwrap(), "LEVEL 1");
        assert_eq!(ce.max_classification("L1", "L2", false).unwrap(), "L2");
        assert_eq!(ce.max_classification("L1", "L2", true).unwrap(), "LEVEL 2");
        assert_eq!(ce.max_classification("L2", "L0", false).unwrap(), "L2");
        assert_eq!(ce.max_classification("L2", "L0", true).unwrap(), "LEVEL 2");
        assert_eq!(ce.max_classification("L2", "L1", false).unwrap(), "L2");
        assert_eq!(ce.max_classification("L2", "L1", true).unwrap(), "LEVEL 2");
        assert_eq!(ce.max_classification("L2", "L2", false).unwrap(), "L2");
        assert_eq!(ce.max_classification("L2", "L2", true).unwrap(), "LEVEL 2");

        // Group operations
        assert_eq!(ce.max_classification("L0//REL A, B", "L0", false).unwrap(), "L0//REL A, B");
        assert_eq!(ce.max_classification("L0//REL A", "L1", true).unwrap(), "LEVEL 1//REL TO GROUP A");
        assert_eq!(ce.max_classification("L0", "L2//REL A, B", false).unwrap(), "L2//REL A, B");
        assert_eq!(ce.max_classification("L0", "L1//REL A", true).unwrap(), "LEVEL 1//REL TO GROUP A");
        assert_eq!(ce.max_classification("L0//REL A, B", "L1//REL A, B", false).unwrap(), "L1//REL A, B");
        assert_eq!(ce.max_classification("L0//REL A, B", "L0//REL A", true).unwrap(), "LEVEL 0//REL TO GROUP A");
        assert_eq!(ce.max_classification("L0//REL B", "L0//REL B, A", true).unwrap(), "LEVEL 0//REL TO GROUP B");
        assert!(ce.max_classification("L0//REL B", "L0//REL A", true).is_err());
        assert!(ce.max_classification("L0//REL B", "L0//REL A", false).is_err());

        // Subgroups
        assert_eq!(ce.max_classification("L0//R1/R2", "L0", false).unwrap(), "L0//XX/R1/R2");
        assert_eq!(ce.max_classification("L0//R1", "L0", true).unwrap(), "LEVEL 0//RESERVE ONE");
        assert_eq!(ce.max_classification("L0//R1/R2", "L1//R1/R2", false).unwrap(), "L1//XX/R1/R2");
        assert_eq!(ce.max_classification("L0//R1/R2", "L0//R1", true).unwrap(), "LEVEL 0//XX/RESERVE ONE"); // TODO ???
    }

    #[test]
    fn multi_group_alias() {
        let mut config = setup_config();
        config.groups[0].aliases.push("Alphabet Gang".to_owned());
        config.groups[1].aliases.push("Alphabet Gang".to_owned());
        let ce = ClassificationParser::new(config).unwrap();

        assert_eq!(ce.normalize_classification_options("L0//REL A", NormalizeOptions::short()).unwrap(), "L0//REL A");
        assert_eq!(ce.normalize_classification_options("L0//REL A, B", NormalizeOptions::short()).unwrap(), "L0//REL ALPHABET GANG");
    }

    #[test]
    fn auto_select_group() {
        let mut config = setup_config();
        config.groups[0].auto_select = true;
        let ce = ClassificationParser::new(config).unwrap();

        assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::short()).unwrap(), "L0");
        assert_eq!(ce.normalize_classification_options("L0//REL A", NormalizeOptions::short()).unwrap(), "L0//REL A");
        assert_eq!(ce.normalize_classification_options("L0//REL B", NormalizeOptions::short()).unwrap(), "L0//REL A, B");
        assert_eq!(ce.normalize_classification_options("L0//REL A, B", NormalizeOptions::short()).unwrap(), "L0//REL A, B");
        assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::default()).unwrap(), "LEVEL 0");
        assert_eq!(ce.normalize_classification_options("L0//REL A", NormalizeOptions::default()).unwrap(), "LEVEL 0//REL TO GROUP A");
        assert_eq!(ce.normalize_classification_options("L0//REL B", NormalizeOptions::default()).unwrap(), "LEVEL 0//REL TO GROUP A, GROUP B");
        assert_eq!(ce.normalize_classification_options("L0//REL A, B", NormalizeOptions::default()).unwrap(), "LEVEL 0//REL TO GROUP A, GROUP B");
    }

    #[test]
    fn auto_select_subgroup() {
        let mut config = setup_config();
        config.subgroups[0].auto_select = true;
        let ce = ClassificationParser::new(config).unwrap();

        assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::short()).unwrap(), "L0");
        assert_eq!(ce.normalize_classification_options("L0//R0", NormalizeOptions::short()).unwrap(), "L0//R1");
        assert_eq!(ce.normalize_classification_options("L0//R2", NormalizeOptions::short()).unwrap(), "L0//XX/R1/R2");
        assert_eq!(ce.normalize_classification_options("L0//R1/R2", NormalizeOptions::short()).unwrap(), "L0//XX/R1/R2");
        assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::default()).unwrap(), "LEVEL 0");
        assert_eq!(ce.normalize_classification_options("L0//R1", NormalizeOptions::default()).unwrap(), "LEVEL 0//RESERVE ONE");
        assert_eq!(ce.normalize_classification_options("L0//R2", NormalizeOptions::default()).unwrap(), "LEVEL 0//XX/RESERVE ONE/RESERVE TWO");
        assert_eq!(ce.normalize_classification_options("L0//R1/R2", NormalizeOptions::default()).unwrap(), "LEVEL 0//XX/RESERVE ONE/RESERVE TWO");
    }

    // #[test]
    // fn subgroup_single() {
    //     let mut config = setup_config();
    //     config.subgroups[0].solitary_display_name = Some("RR".to_owned());
    //     let ce = ClassificationParser::new(config).unwrap();

    //     assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::short()).unwrap(), "L0");
    //     assert_eq!(ce.normalize_classification_options("L0//R0", NormalizeOptions::short()).unwrap(), "L0//RR");
    //     assert_eq!(ce.normalize_classification_options("L0//R2", NormalizeOptions::short()).unwrap(), "L0//XX/R1/R2");
    //     assert_eq!(ce.normalize_classification_options("L0//R1/R2", NormalizeOptions::short()).unwrap(), "L0//XX/R1/R2");
    //     assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::default()).unwrap(), "LEVEL 0");
    //     assert_eq!(ce.normalize_classification_options("L0//R1", NormalizeOptions::default()).unwrap(), "LEVEL 0//RR");
    //     assert_eq!(ce.normalize_classification_options("L0//R2", NormalizeOptions::default()).unwrap(), "LEVEL 0//XX/RESERVE ONE/RESERVE TWO");
    //     assert_eq!(ce.normalize_classification_options("L0//R1/R2", NormalizeOptions::default()).unwrap(), "LEVEL 0//XX/RESERVE ONE/RESERVE TWO");
    // }

    #[test]
    fn parts() {
        let ce = setup();

        // level only
        assert_eq!(ce.get_classification_parts("L0", None, None).unwrap(), ParsedClassification{level: 1, ..Default::default()});
        assert_eq!(ce.get_classification_parts("LEVEL 0", None, None).unwrap(), ParsedClassification{level: 1, ..Default::default()});
        assert_eq!(ce.get_classification_parts("L1", None, None).unwrap(), ParsedClassification{level: 5, ..Default::default()});
        assert_eq!(ce.get_classification_parts("LEVEL 1", None, None).unwrap(), ParsedClassification{level: 5, ..Default::default()});
        assert_eq!(ce.get_classification_parts("L0", false, None).unwrap(), ParsedClassification{level: 1, ..Default::default()});
        assert_eq!(ce.get_classification_parts("LEVEL 0", false, None).unwrap(), ParsedClassification{level: 1, ..Default::default()});
        assert_eq!(ce.get_classification_parts("L1", false, None).unwrap(), ParsedClassification{level: 5, ..Default::default()});
        assert_eq!(ce.get_classification_parts("LEVEL 1", false, None).unwrap(), ParsedClassification{level: 5, ..Default::default()});

        // Group operations
        assert_eq!(ce.get_classification_parts("L0//REL A", None, None).unwrap(), ParsedClassification{level: 1, groups: vec!["GROUP A".to_owned()], ..Default::default()});
        assert_eq!(ce.get_classification_parts("LEVEL 0//REL Group A", None, None).unwrap(), ParsedClassification{level: 1, groups: vec!["GROUP A".to_owned()], ..Default::default()});
        assert_eq!(ce.get_classification_parts("L0//REL A", false, None).unwrap(), ParsedClassification{level: 1, groups: vec!["A".to_owned()], ..Default::default()});
        assert_eq!(ce.get_classification_parts("LEVEL 0//REL Group A", false, None).unwrap(), ParsedClassification{level: 1, groups: vec!["A".to_owned()], ..Default::default()});
    }

    #[test]
    fn normalize() {
        let ce = setup();

        // level only
        assert_eq!(ce.normalize_classification_options("L0", NormalizeOptions::short()).unwrap(), "L0");
        assert_eq!(ce.normalize_classification("L1").unwrap(), "LEVEL 1");

        // Group operations
        assert_eq!(ce.normalize_classification("L0//REL A, B").unwrap(), "LEVEL 0//REL TO GROUP A, GROUP B");
        assert_eq!(ce.normalize_classification_options("L0//REL A, B", NormalizeOptions::short()).unwrap(), "L0//REL A, B");
        assert_eq!(ce.normalize_classification("L0//REL A").unwrap(), "LEVEL 0//REL TO GROUP A");
        assert_eq!(ce.normalize_classification_options("L0//REL A", NormalizeOptions::short()).unwrap(), "L0//REL A");
        assert_eq!(ce.normalize_classification("L2//REL A, B").unwrap(), "LEVEL 2//REL TO GROUP A, GROUP B");
        assert_eq!(ce.normalize_classification_options("L2//REL A, B", NormalizeOptions::short()).unwrap(), "L2//REL A, B");
        assert_eq!(ce.normalize_classification("L1//REL A").unwrap(), "LEVEL 1//REL TO GROUP A");
        assert_eq!(ce.normalize_classification_options("L1//REL A", NormalizeOptions::short()).unwrap(), "L1//REL A");
        assert_eq!(ce.normalize_classification("L0//REL B").unwrap(), "LEVEL 0//REL TO GROUP B");
        assert_eq!(ce.normalize_classification_options("L0//REL B", NormalizeOptions::short()).unwrap(), "L0//REL B");
        assert_eq!(ce.normalize_classification("L0//REL B, A").unwrap(), "LEVEL 0//REL TO GROUP A, GROUP B");
        assert_eq!(ce.normalize_classification_options("L0//REL B, A", NormalizeOptions::short()).unwrap(), "L0//REL A, B");

        //
        assert_eq!(ce.normalize_classification("L1//LE").unwrap(), "LEVEL 1//LEGAL DEPARTMENT");

        // bad inputs
        assert!(ce.normalize_classification("GARBO").is_err());
        // assert!(ce.normalize_classification("L1//GARBO").is_err());
        // assert!(ce.normalize_classification("L1//LE//GARBO").is_err());
    }

    #[test]
    fn access_control() -> Result<()> {
        let ce = setup();

        // Access limits due to level
        assert!(ce.is_accessible("L0", "L0")?);
        assert!(!ce.is_accessible("L0", "L1")?);
        assert!(!ce.is_accessible("L0", "L2")?);
        assert!(ce.is_accessible("L1", "L0")?);
        assert!(ce.is_accessible("L1", "L1")?);
        assert!(!ce.is_accessible("L1", "L2")?);
        assert!(ce.is_accessible("L2", "L0")?);
        assert!(ce.is_accessible("L2", "L1")?);
        assert!(ce.is_accessible("L2", "L2")?);

        // Access limits due to control system markings
        assert!(!ce.is_accessible("L2", "L0//LE")?);
        assert!(ce.is_accessible("L2//LE", "L0//LE")?);

        assert!(!ce.is_accessible("L2", "L2//LE/AC")?);
        assert!(!ce.is_accessible("L2//LE", "L2//LE/AC")?);
        assert!(!ce.is_accessible("L2//AC", "L2//LE/AC")?);
        assert!(ce.is_accessible("L2//LE/AC", "L2//LE/AC")?);

        // Access limits due to dissemination
        assert!(!ce.is_accessible("L2", "L2//ORCON/NOCON")?);
        assert!(!ce.is_accessible("L2//ORCON", "L2//ORCON/NOCON")?);
        assert!(!ce.is_accessible("L2//NOCON", "L2//ORCON/NOCON")?);
        assert!(ce.is_accessible("L2//ORCON/NOCON", "L2//ORCON/NOCON")?);

        // Access limits due to releasability
        assert!(!ce.is_accessible("L2", "L2//REL A")?);
        assert!(!ce.is_accessible("L2//REL B", "L2//REL A")?);
        assert!(ce.is_accessible("L2//REL B", "L2//REL A, B")?);
        assert!(ce.is_accessible("L2//REL B", "L2//REL B")?);
        assert!(ce.is_accessible("L2//REL B", "L2")?);

        Ok(())
    }
}