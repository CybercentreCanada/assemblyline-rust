use std::collections::HashMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use struct_metadata::{Described, MetadataKind};

use crate::messages::task::TagEntry;
use crate::types::ja4::is_ja4;
use crate::types::md5::is_md5;
use crate::types::sha1::is_sha1;
use crate::types::sha256::is_sha256;
use crate::types::json_validation::{transform_string_with, validate_lowercase, validate_lowercase_with, validate_number, validate_rule_mapping, validate_string, validate_string_with, validate_uppercase, validate_uppercase_with};
use crate::types::ssdeep::is_ssdeep_hash;
use crate::types::strings::{check_domain, check_email, check_uri, is_ip, is_mac, is_phone_number, is_unc_path, is_uri_path};
use crate::types::JsonMap;
use crate::ElasticMeta;

// MARK: Tag Value
/// A thin wrapper over the generic JSON value type to enforce tag specific behaviours we want
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct TagValue(serde_json::Value);

// When we convert tags to strings we don't want to include quotes on raw strings
impl std::fmt::Display for TagValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            serde_json::Value::String(string) => f.write_str(string),
            other => f.write_fmt(format_args!("{other}"))
        }
    }
}

impl From<String> for TagValue {
    fn from(value: String) -> Self {
        Self(serde_json::Value::String(value))
    }
}

// MARK: Tag Processors
#[derive(Debug)]
enum TagProcessor {
    // Generic strings
    String,
    Uppercase,
    Lowercase,

    // Special purpose strings
    PhoneNumber,
    RuleMapping, // HashMap<String, Vec<String>>
    Domain,
    IpAddress,
    Uri,
    Mac,
    UNCPath,
    UriPath,
    EmailAddress,

    // hashes
    Sha256,
    Sha1,
    MD5,
    SSDeepHash,
    JA4,

    // numbers
    U16,
    I32,
    // I64,
}


impl TagProcessor {
    pub fn apply(&self, value: serde_json::Value) -> Result<serde_json::Value, serde_json::Value> {
        match self {
            TagProcessor::String => validate_string(value),
            TagProcessor::Uppercase => validate_uppercase(value),
            TagProcessor::Lowercase => validate_lowercase(value),
            TagProcessor::PhoneNumber => validate_string_with(value, is_phone_number),
            TagProcessor::RuleMapping => validate_rule_mapping(value),
            TagProcessor::Domain => transform_string_with(value, |domain| check_domain(domain).ok()),
            TagProcessor::IpAddress => validate_uppercase_with(value, is_ip),
            TagProcessor::Uri => transform_string_with(value, |uri| check_uri(uri).ok()),
            TagProcessor::Mac => validate_lowercase_with(value, is_mac),
            TagProcessor::UNCPath => validate_string_with(value, is_unc_path),
            TagProcessor::UriPath => validate_string_with(value, is_uri_path),
            TagProcessor::EmailAddress => transform_string_with(value, |email| check_email(email).ok()),
            TagProcessor::Sha256 => validate_lowercase_with(value, is_sha256),
            TagProcessor::Sha1 => validate_lowercase_with(value, is_sha1),
            TagProcessor::MD5 => validate_lowercase_with(value, is_md5),
            TagProcessor::SSDeepHash => validate_string_with(value, is_ssdeep_hash),
            TagProcessor::JA4 => validate_lowercase_with(value, is_ja4),
            TagProcessor::U16 => validate_number::<u16>(value),
            TagProcessor::I32 => validate_number::<i32>(value),
            // TagProcessor::I64 => validate_number::<i64>(value),
        }
    }
}

// MARK: Tag Information
#[derive(Debug)]
pub struct TagInformation {
    name: &'static [&'static str],
    description: &'static str,
    processor: TagProcessor,
}

impl Eq for TagInformation {}

impl PartialEq for TagInformation {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl std::hash::Hash for TagInformation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl TagInformation {
    const fn new(name: &'static [&'static str], description: &'static str, processor: TagProcessor) -> Self {
        Self {
            name,
            description,
            processor
        }
    }

    pub fn full_path(&self) -> String {
        self.name.join(".")
    }

    pub fn metadata_type(&self) -> struct_metadata::Descriptor<ElasticMeta> {
        use struct_metadata::{Kind, Descriptor};
        let metadata = ElasticMeta{copyto: Some("__text__"), ..Default::default()};
        match self.processor {
            TagProcessor::RuleMapping => struct_metadata::Descriptor { 
                docs: None, 
                metadata, 
                kind: Kind::Mapping(
                    Box::new(Descriptor { docs: None, metadata: Default::default(), kind: Kind::String }), 
                    Box::new(Descriptor { docs: None, metadata: Default::default(), kind: Kind::Sequence(
                        Box::new(Descriptor { docs: None, metadata: ElasticMeta{copyto: Some("__text__"), ..Default::default()}, kind: Kind::String }),    
                    )})
                )
            },
            // TagProcessor::I64 => struct_metadata::Descriptor { docs: None, metadata: Default::default(), kind: Kind::I64},
            TagProcessor::U16 => struct_metadata::Descriptor { docs: None, metadata: Default::default(), kind: Kind::U16},
            TagProcessor::I32 => struct_metadata::Descriptor { docs: None, metadata: Default::default(), kind: Kind::I32},
            TagProcessor::IpAddress => struct_metadata::Descriptor { 
                docs: None, 
                metadata: ElasticMeta { mapping: Some("ip"), ..metadata }, 
                kind: Kind::String
            },
            TagProcessor::SSDeepHash => struct_metadata::Descriptor { 
                docs: None, 
                metadata: ElasticMeta{mapping: Some("text"), analyzer: Some("text_fuzzy"), ..metadata}, 
                kind: Kind::String
            },
            TagProcessor::Lowercase | TagProcessor::Sha1 | TagProcessor::MD5 | TagProcessor::Sha256 => struct_metadata::Descriptor { 
                docs: None, 
                metadata: ElasticMeta { normalizer: Some("lowercase_normalizer"), ..metadata }, 
                kind: Kind::String 
            },            
            _ => struct_metadata::Descriptor { docs: None, metadata, kind: Kind::String }
        }
    }
}


// MARK: Tag List
/// The list of all tags we are willing to accept. 
/// This includes their path within a tagging dict, a textual description and how they should be processed for validation or normalization
static ALL_VALID_TAGS: [TagInformation; 210] = [
    TagInformation::new(&["attribution", "actor"], "Attribution Actor", TagProcessor::Uppercase),
    TagInformation::new(&["attribution", "campaign"], "Attribution Campaign", TagProcessor::Uppercase),
    TagInformation::new(&["attribution", "category"], "Attribution Category", TagProcessor::Uppercase),
    TagInformation::new(&["attribution", "exploit"], "Attribution Exploit", TagProcessor::Uppercase),
    TagInformation::new(&["attribution", "implant"], "Attribution Implant", TagProcessor::Uppercase),
    TagInformation::new(&["attribution", "family"], "Attribution Family", TagProcessor::Uppercase),
    TagInformation::new(&["attribution", "network"], "Attribution Network", TagProcessor::Uppercase),

    TagInformation::new(&["av", "heuristic"], "List of heuristics", TagProcessor::String),
    TagInformation::new(&["av", "virus_name"], "Collection of virus names identified by antivirus tools", TagProcessor::String),

    TagInformation::new(&["cert", "extended_key_usage"], "Extended Key Usage", TagProcessor::String),
    TagInformation::new(&["cert", "issuer"], "Issuer", TagProcessor::String),
    TagInformation::new(&["cert", "key_usage"], "Key Usage", TagProcessor::String),
    TagInformation::new(&["cert", "owner"], "Owner", TagProcessor::String),
    TagInformation::new(&["cert", "serial_no"], "Serial Number", TagProcessor::String),
    TagInformation::new(&["cert", "signature_algo"], "Signature Algorithm", TagProcessor::String),
    TagInformation::new(&["cert", "subject"], "Subject Name", TagProcessor::String),
    TagInformation::new(&["cert", "subject_alt_name"], "Alternative Subject Name", TagProcessor::String),
    TagInformation::new(&["cert", "thumbprint"], "Thumbprint", TagProcessor::String),
    TagInformation::new(&["cert", "valid", "start"], "Start date of certificate validity", TagProcessor::String),
    TagInformation::new(&["cert", "valid", "end"], "End date of certificate validity", TagProcessor::String),
    TagInformation::new(&["cert", "version"], "Version", TagProcessor::String),

    TagInformation::new(&["dynamic", "autorun_location"], "Autorun location", TagProcessor::String),
    TagInformation::new(&["dynamic", "dos_device"], "DOS Device", TagProcessor::String),
    TagInformation::new(&["dynamic", "mutex"], "Mutex", TagProcessor::String),
    TagInformation::new(&["dynamic", "registry_key"], "Registy Keys", TagProcessor::String),
    TagInformation::new(&["dynamic", "process", "command_line"], "Commandline", TagProcessor::String),
    TagInformation::new(&["dynamic", "process", "file_name"], "Filename", TagProcessor::String),
    TagInformation::new(&["dynamic", "process", "shortcut"], "Shortcut", TagProcessor::String),
    TagInformation::new(&["dynamic", "signature", "category"], "Signature Category", TagProcessor::String),
    TagInformation::new(&["dynamic", "signature", "family"], "Signature Family", TagProcessor::String),
    TagInformation::new(&["dynamic", "signature", "name"], "Signature Name", TagProcessor::String),
    TagInformation::new(&["dynamic", "ssdeep", "cls_ids"], "CLSIDs", TagProcessor::SSDeepHash),
    TagInformation::new(&["dynamic", "ssdeep", "dynamic_classes"], "Dynamic Classes", TagProcessor::SSDeepHash),
    TagInformation::new(&["dynamic", "ssdeep", "regkeys"], "Registry Keys", TagProcessor::SSDeepHash),
    TagInformation::new(&["dynamic", "window", "cls_ids"], "CLSIDs", TagProcessor::String),
    TagInformation::new(&["dynamic", "window", "dynamic_classes"], "Dynamic Classes", TagProcessor::String),
    TagInformation::new(&["dynamic", "window", "regkeys"], "Registry Keys", TagProcessor::String),
    TagInformation::new(&["dynamic", "operating_system", "platform"], "Platform", TagProcessor::String),
    TagInformation::new(&["dynamic", "operating_system", "version"], "Version", TagProcessor::String),
    TagInformation::new(&["dynamic", "operating_system", "processor"], "Processor", TagProcessor::String),
    TagInformation::new(&["dynamic", "processtree_id"], "Process Tree ID", TagProcessor::String),

    TagInformation::new(&["info", "phone_number"], "Phone Number", TagProcessor::PhoneNumber),
    TagInformation::new(&["info", "password"], "Suspected Password", TagProcessor::String),

    TagInformation::new(&["file", "ancestry"], "File Genealogy", TagProcessor::String),
    TagInformation::new(&["file", "behavior"], "File Behaviour", TagProcessor::String),
    TagInformation::new(&["file", "compiler"], "Compiler of File", TagProcessor::String),
    TagInformation::new(&["file", "config"], "File Configuration", TagProcessor::String),
    TagInformation::new(&["file", "date", "creation"], "File Creation Date", TagProcessor::String),
    TagInformation::new(&["file", "date", "last_modified"], "File Last Modified Date", TagProcessor::String),
    TagInformation::new(&["file", "elf", "libraries"], "ELF File Properties: Libraries", TagProcessor::String),
    TagInformation::new(&["file", "elf", "interpreter"], "ELF File Properties: Interpreter", TagProcessor::String),
    TagInformation::new(&["file", "elf", "sections", "name"], "ELF File Properties: Section Name", TagProcessor::String),
    TagInformation::new(&["file", "elf", "segments", "type"], "ELF File Properties: Segment Types", TagProcessor::String),
    TagInformation::new(&["file", "elf", "notes", "name"], "ELF File Properties: Note name", TagProcessor::String),
    TagInformation::new(&["file", "elf", "notes", "type"], "ELF File Properties: Note type", TagProcessor::String),
    TagInformation::new(&["file", "elf", "notes", "type_core"], "ELF File Properties: Note type core", TagProcessor::String),
    TagInformation::new(&["file", "lib"], "File Libraries", TagProcessor::String),
    TagInformation::new(&["file", "lsh"], "File LSH hashes", TagProcessor::String),
    TagInformation::new(&["file", "name", "anomaly"], "File Anomaly Name", TagProcessor::String),
    TagInformation::new(&["file", "name", "extracted"], "File Extracted Name", TagProcessor::String),
    TagInformation::new(&["file", "path"], "File Path", TagProcessor::String),
    TagInformation::new(&["file", "rule"], "Rule/Signature File", TagProcessor::RuleMapping),
    TagInformation::new(&["file", "string", "api"], "File API Strings", TagProcessor::String),
    TagInformation::new(&["file", "string", "blacklisted"], "File Known Bad Strings", TagProcessor::String),
    TagInformation::new(&["file", "string", "decoded"], "File Decoded Strings", TagProcessor::String),
    TagInformation::new(&["file", "string", "extracted"], "File Extracted Strings", TagProcessor::String),
    TagInformation::new(&["file", "apk", "activity"], "APK File Properties: Activity", TagProcessor::String),
    TagInformation::new(&["file", "apk", "app", "label"], "APK File Properties: APK Application Information: Label", TagProcessor::String),
    TagInformation::new(&["file", "apk", "app", "version"], "APK File Properties: APK Application Information: Version", TagProcessor::String),
    TagInformation::new(&["file", "apk", "feature"], "APK File Properties: Features", TagProcessor::String),
    TagInformation::new(&["file", "apk", "locale"], "APK File Properties: Locale", TagProcessor::String),
    TagInformation::new(&["file", "apk", "permission"], "APK File Properties: Permissions", TagProcessor::String),
    TagInformation::new(&["file", "apk", "pkg_name"], "APK File Properties: Package Name", TagProcessor::String),
    TagInformation::new(&["file", "apk", "provides_component"], "APK File Properties: Components Provided", TagProcessor::String),
    TagInformation::new(&["file", "apk", "sdk", "min"], "APK File Properties: APK SDK minimum OS required", TagProcessor::String),
    TagInformation::new(&["file", "apk", "sdk", "target"], "APK File Properties: APK SDK target OS", TagProcessor::String),
    TagInformation::new(&["file", "apk", "used_library"], "APK File Properties: Libraries Used", TagProcessor::String),
    TagInformation::new(&["file", "jar", "main_class"], "JAR File Properties: Main Class", TagProcessor::String),
    TagInformation::new(&["file", "jar", "main_package"], "JAR File Properties: Main Package", TagProcessor::String),
    TagInformation::new(&["file", "jar", "imported_package"], "JAR File Properties: Imported package", TagProcessor::String),
    TagInformation::new(&["file", "img", "exif_tool", "creator_tool"], "Image File Properties: Exiftool Information: Image Creation Tool", TagProcessor::String),
    TagInformation::new(&["file", "img", "exif_tool", "derived_document_id"], "Image File Properties: Exiftool Information: Derived Document ID", TagProcessor::String),
    TagInformation::new(&["file", "img", "exif_tool", "document_id"], "Image File Properties: Exiftool Information: Document ID", TagProcessor::String),
    TagInformation::new(&["file", "img", "exif_tool", "instance_id"], "Image File Properties: Exiftool Information: Instance ID", TagProcessor::String),
    TagInformation::new(&["file", "img", "exif_tool", "toolkit"], "Image File Properties: Exiftool Information: Toolkit", TagProcessor::String),
    TagInformation::new(&["file", "img", "mega_pixels"], "Image File Properties: Megapixels", TagProcessor::String),
    TagInformation::new(&["file", "img", "mode"], "Image File Properties: Image Mode", TagProcessor::String),
    TagInformation::new(&["file", "img", "size"], "Image File Properties: Image Size", TagProcessor::String),
    TagInformation::new(&["file", "img", "sorted_metadata_hash"], "Image File Properties: Sorted Metadata Hash", TagProcessor::String),
    TagInformation::new(&["file", "ole", "macro", "sha256"], "OLE File Properties: OLE Macro: SHA256 of Macro", TagProcessor::Sha256),
    TagInformation::new(&["file", "ole", "macro", "suspicious_string"], "OLE File Properties: OLE Macro: Suspicious Strings", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "author"], "OLE File Properties: OLE Summary: Author", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "codepage"], "OLE File Properties: OLE Summary: Code Page", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "comment"], "OLE File Properties: OLE Summary: Comment", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "company"], "OLE File Properties: OLE Summary: Company", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "create_time"], "OLE File Properties: OLE Summary: Creation Time", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "last_printed"], "OLE File Properties: OLE Summary: Date Last Printed", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "last_saved_by"], "OLE File Properties: OLE Summary: User Last Saved By", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "last_saved_time"], "OLE File Properties: OLE Summary: Date Last Saved", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "manager"], "OLE File Properties: OLE Summary: Manager", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "subject"], "OLE File Properties: OLE Summary: Subject", TagProcessor::String),
    TagInformation::new(&["file", "ole", "summary", "title"], "OLE File Properties: OLE Summary: Title", TagProcessor::String),
    TagInformation::new(&["file", "ole", "clsid"], "OLE File Properties: CLSID", TagProcessor::String),
    TagInformation::new(&["file", "ole", "dde_link"], "OLE File Properties: DDE Link", TagProcessor::String),
    TagInformation::new(&["file", "ole", "fib_timestamp"], "OLE File Properties: FIB Timestamp", TagProcessor::String),
    TagInformation::new(&["file", "pe", "api_vector"], "PE File Properties: API Vector", TagProcessor::String),
    TagInformation::new(&["file", "pe", "authenticode", "spc_sp_opus_info", "program_name"], "PE File Properties: PE Authenticode Information: Program name", TagProcessor::String),
    TagInformation::new(&["file", "pe", "debug", "guid"], "PE File Properties: PE Debug Information: GUID", TagProcessor::String),
    TagInformation::new(&["file", "pe", "exports", "function_name"], "PE File Properties: PE Exports Information: Function Name", TagProcessor::String),
    TagInformation::new(&["file", "pe", "exports", "module_name"], "PE File Properties: PE Exports Information: Module Name", TagProcessor::String),
    TagInformation::new(&["file", "pe", "imports", "fuzzy"], "PE File Properties: PE Imports Information: Fuzzy", TagProcessor::SSDeepHash),
    TagInformation::new(&["file", "pe", "imports", "md5"], "PE File Properties: PE Imports Information: MD5", TagProcessor::MD5),
    TagInformation::new(&["file", "pe", "imports", "imphash"], "PE File Properties: PE Imports Information: Imphash", TagProcessor::MD5),
    TagInformation::new(&["file", "pe", "imports", "sorted_fuzzy"], "PE File Properties: PE Imports Information: Sorted Fuzzy", TagProcessor::SSDeepHash),
    TagInformation::new(&["file", "pe", "imports", "sorted_sha1"], "PE File Properties: PE Imports Information: Sorted SHA1", TagProcessor::Sha1),
    TagInformation::new(&["file", "pe", "imports", "gimphash"], "PE File Properties: PE Imports Information: Go Import hash", TagProcessor::Sha256),
    TagInformation::new(&["file", "pe", "imports", "suspicious"], "PE File Properties: PE Imports Information: Suspicious", TagProcessor::String),
    TagInformation::new(&["file", "pe", "linker", "timestamp"], "PE File Properties: PE Linker Information: timestamp", TagProcessor::String),
    TagInformation::new(&["file", "pe", "oep", "bytes"], "PE File Properties: PE OEP Information: Bytes", TagProcessor::String),
    TagInformation::new(&["file", "pe", "oep", "hexdump"], "PE File Properties: PE OEP Information: Hex Dump", TagProcessor::String),
    TagInformation::new(&["file", "pe", "pdb_filename"], "PE File Properties: PDB Filename", TagProcessor::String),
    TagInformation::new(&["file", "pe", "resources", "language"], "PE File Properties: PE Resources Information: Language", TagProcessor::String),
    TagInformation::new(&["file", "pe", "resources", "name"], "PE File Properties: PE Resources Information: Name", TagProcessor::String),
    TagInformation::new(&["file", "pe", "rich_header", "hash"], "PE File Properties: PE Rich Header Information: Hash", TagProcessor::String),
    TagInformation::new(&["file", "pe", "sections", "hash"], "PE File Properties: PE Sections Information: Hash", TagProcessor::String),
    TagInformation::new(&["file", "pe", "sections", "name"], "PE File Properties: PE Sections Information: Name", TagProcessor::String),
    TagInformation::new(&["file", "pe", "versions", "description"], "PE File Properties: PE Versions Information: Description", TagProcessor::String),
    TagInformation::new(&["file", "pe", "versions", "filename"], "PE File Properties: PE Versions Information: Filename", TagProcessor::String),
    TagInformation::new(&["file", "pdf", "date", "modified"], "PDF File Properties: PDF Date Information: Date Modified", TagProcessor::String),
    TagInformation::new(&["file", "pdf", "date", "pdfx"], "PDF File Properties: PDF Date Information: PDFx", TagProcessor::String),
    TagInformation::new(&["file", "pdf", "date", "source_modified"], "PDF File Properties: PDF Date Information: Date Source Modified", TagProcessor::String),
    TagInformation::new(&["file", "pdf", "javascript", "sha1"], "PDF File Properties: PDF Javascript Information: SHA1 of javascript", TagProcessor::Sha1),
    TagInformation::new(&["file", "pdf", "stats", "sha1"], "PDF File Properties: PDF Statistics Information: SHA1 of statistics", TagProcessor::Sha1),
    TagInformation::new(&["file", "plist", "installer_url"], "PList File Properties: Installer URL", TagProcessor::String),
    TagInformation::new(&["file", "plist", "min_os_version"], "PList File Properties: Minimum OS Version", TagProcessor::String),
    TagInformation::new(&["file", "plist", "requests_open_access"], "PList File Properties: Requests Open Access", TagProcessor::String),
    TagInformation::new(&["file", "plist", "build", "machine_os"], "PList File Properties: Build Information: Machine OS", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "development_region"], "PList File Properties: CF Bundle Information: Development Region", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "display_name"], "PList File Properties: CF Bundle Information: Display Name", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "executable"], "PList File Properties: CF Bundle Information: Executable Name", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "identifier"], "PList File Properties: CF Bundle Information: Identifier Name", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "name"], "PList File Properties: CF Bundle Information: Bundle Name", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "pkg_type"], "PList File Properties: CF Bundle Information: Package Type", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "signature"], "PList File Properties: CF Bundle Information: Signature", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "url_scheme"], "PList File Properties: CF Bundle Information: URL Scheme", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "version", "long"], "PList File Properties: CF Bundle Information: Bundle Version Information: Long Version", TagProcessor::String),
    TagInformation::new(&["file", "plist", "cf_bundle", "version", "short"], "PList File Properties: CF Bundle Information: Bundle Version Information: Short Version", TagProcessor::String),
    TagInformation::new(&["file", "plist", "dt", "compiler"], "PList File Properties: DT Information: Compiler", TagProcessor::String),
    TagInformation::new(&["file", "plist", "dt", "platform", "build"], "PList File Properties: DT Information: Platform Information: Build", TagProcessor::String),
    TagInformation::new(&["file", "plist", "dt", "platform", "name"], "PList File Properties: DT Information: Platform Information: Name", TagProcessor::String),
    TagInformation::new(&["file", "plist", "dt", "platform", "version"], "PList File Properties: DT Information: Platform Information: Version", TagProcessor::String),
    TagInformation::new(&["file", "plist", "ls", "background_only"], "PList File Properties: LS Information: Background Only", TagProcessor::String),
    TagInformation::new(&["file", "plist", "ls", "min_system_version"], "PList File Properties: LS Information: Minimum System Versuion", TagProcessor::String),
    TagInformation::new(&["file", "plist", "ns", "apple_script_enabled"], "PList File Properties: NS Information: Apple Script Enabled", TagProcessor::String),
    TagInformation::new(&["file", "plist", "ns", "principal_class"], "PList File Properties: NS Information: Principal Class", TagProcessor::String),
    TagInformation::new(&["file", "plist", "ui", "background_modes"], "PList File Properties: UI Information: Background Modes", TagProcessor::String),
    TagInformation::new(&["file", "plist", "ui", "requires_persistent_wifi"], "PList File Properties: UI Information: Requires Persistent WIFI", TagProcessor::String),
    TagInformation::new(&["file", "plist", "wk", "app_bundle_identifier"], "PList File Properties: WK Information: App Bundle ID", TagProcessor::String),
    TagInformation::new(&["file", "powershell", "cmdlet"], "PowerShell File Properties: Cmdlet", TagProcessor::String),
    TagInformation::new(&["file", "shortcut", "command_line"], "Shortcut File Properties: Command Line", TagProcessor::String),
    TagInformation::new(&["file", "shortcut", "icon_location"], "Shortcut File Properties: Icon Location", TagProcessor::String),
    TagInformation::new(&["file", "shortcut", "machine_id"], "Shortcut File Properties: Machine ID", TagProcessor::String),
    TagInformation::new(&["file", "shortcut", "tracker_mac"], "Shortcut File Properties: Possible MAC address from the Tracker block", TagProcessor::String),
    TagInformation::new(&["file", "swf", "header", "frame", "count"], "SWF File Properties: Header Information: Header Frame Information: Number of Frames", TagProcessor::I32),
    TagInformation::new(&["file", "swf", "header", "frame", "rate"], "SWF File Properties: Header Information: Header Frame Information: Speed of Animation", TagProcessor::String),
    TagInformation::new(&["file", "swf", "header", "frame", "size"], "SWF File Properties: Header Information: Header Frame Information: Size of Frame", TagProcessor::String),
    TagInformation::new(&["file", "swf", "header", "version"], "SWF File Properties: Header Information: Version", TagProcessor::String),
    TagInformation::new(&["file", "swf", "tags_ssdeep"], "SWF File Properties: Tags SSDeep", TagProcessor::SSDeepHash),
    
    TagInformation::new(&["network", "attack"], "Network: Attack", TagProcessor::String),
    TagInformation::new(&["network", "dynamic", "domain"], "Network: Dynamic IOCs: Domain", TagProcessor::Domain),
    TagInformation::new(&["network", "dynamic", "ip"], "Network: Dynamic IOCs: IP", TagProcessor::IpAddress),
    TagInformation::new(&["network", "dynamic", "unc_path"], "Network: Dynamic IOCs: UNC Path", TagProcessor::UNCPath),
    TagInformation::new(&["network", "dynamic", "uri"], "Network: Dynamic IOCs: URI", TagProcessor::Uri),
    TagInformation::new(&["network", "dynamic", "uri_path"], "Network: Dynamic IOCs: URI Path", TagProcessor::UriPath),
    TagInformation::new(&["network", "email", "address"], "Network: Email: Email Address", TagProcessor::EmailAddress),
    TagInformation::new(&["network", "email", "date"], "Network: Email: Date", TagProcessor::String),
    TagInformation::new(&["network", "email", "subject"], "Network: Email: Subject", TagProcessor::String),
    TagInformation::new(&["network", "email", "msg_id"], "Network: Email: Message ID", TagProcessor::String),
    TagInformation::new(&["network", "mac_address"], "Network: MAC Address", TagProcessor::Mac),
    TagInformation::new(&["network", "port"], "Network: Port", TagProcessor::U16),
    TagInformation::new(&["network", "protocol"], "Network: Protocol", TagProcessor::String),
    TagInformation::new(&["network", "signature", "signature_id"], "Network: Signatures: ID", TagProcessor::String),
    TagInformation::new(&["network", "signature", "message"], "Network: Signatures: Message", TagProcessor::String),
    TagInformation::new(&["network", "static", "domain"], "Network: Static IOCs: Domain", TagProcessor::Domain),
    TagInformation::new(&["network", "static", "ip"], "Network: Static IOCs: IP", TagProcessor::IpAddress),
    TagInformation::new(&["network", "static", "unc_path"], "Network: Static IOCs: UNC Path", TagProcessor::UNCPath),
    TagInformation::new(&["network", "static", "uri"], "Network: Static IOCs: URI", TagProcessor::Uri),
    TagInformation::new(&["network", "static", "uri_path"], "Network: Static IOCs: URI Path", TagProcessor::UriPath),
    TagInformation::new(&["network", "tls", "ja3_hash"], "Network: TLS: JA3 Hash", TagProcessor::Lowercase),
    TagInformation::new(&["network", "tls", "ja3_string"], "Network: TLS: JA3 String", TagProcessor::String),
    TagInformation::new(&["network", "tls", "ja3s_hash"], "Network: TLS: JA3S Hash", TagProcessor::Lowercase),
    TagInformation::new(&["network", "tls", "ja3s_string"], "Network: TLS: JA3S String", TagProcessor::String),
    TagInformation::new(&["network", "tls", "ja4_hash"], "Network: TLS: JA4 Hash", TagProcessor::JA4),
    TagInformation::new(&["network", "tls", "ja4s_hash"], "Network: TLS: JA4S Hash", TagProcessor::String),
    TagInformation::new(&["network", "tls", "sni"], "Network: TLS: SNI", TagProcessor::String),
    TagInformation::new(&["network", "user_agent"], "Network: User Agent", TagProcessor::String),

    TagInformation::new(&["source"], "Source", TagProcessor::String),

    TagInformation::new(&["technique", "comms_routine"], "Technique: Communication Routine", TagProcessor::String),
    TagInformation::new(&["technique", "config"], "Technique: Configuration", TagProcessor::String),
    TagInformation::new(&["technique", "crypto"], "Technique: Cryptography", TagProcessor::String),
    TagInformation::new(&["technique", "exploit"], "Technique: Technique", TagProcessor::String),
    TagInformation::new(&["technique", "keylogger"], "Technique: Keylogger", TagProcessor::String),
    TagInformation::new(&["technique", "macro"], "Technique: Macro", TagProcessor::String),
    TagInformation::new(&["technique", "masking_algo"], "Technique: Masking Algorithm", TagProcessor::String),
    TagInformation::new(&["technique", "obfuscation"], "Technique: Obfuscation", TagProcessor::String),
    TagInformation::new(&["technique", "packer"], "Technique: Packer", TagProcessor::String),
    TagInformation::new(&["technique", "persistence"], "Technique: Persistence", TagProcessor::String),
    TagInformation::new(&["technique", "shellcode"], "Technique: Shell Code", TagProcessor::String),
    TagInformation::new(&["technique", "string"], "Technique: String", TagProcessor::String),

    TagInformation::new(&["vector"], "Vector", TagProcessor::String),
];


pub fn get_tag_information(label: &str) -> Option<&'static TagInformation> {
    static TAGS: LazyLock<HashMap<String, &'static TagInformation>> = LazyLock::new(|| {
        let mut table: HashMap<String, &'static TagInformation> = Default::default();
        for tag in &ALL_VALID_TAGS {
            if let Some(collision) = table.insert(tag.full_path(), tag) {
                panic!("Collision on tag name: {}", collision.full_path());
            }
        }
        table
    });
    TAGS.get(label).copied()
}

// MARK: Nested Tag Container
/// Container for a dictionary set of tags
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(transparent)]
pub struct Tagging(JsonMap);

impl Described<ElasticMeta> for Tagging {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {

        let mut catagories: HashMap<&'static str, Vec<_>> = HashMap::new();
        for tag in &ALL_VALID_TAGS {
            catagories.entry(tag.name[0]).or_default().push((&tag.name[1..], tag))
        }

        fn make_entry(label: &'static str, elements: &[(&[&'static str], &TagInformation)]) -> struct_metadata::Entry<ElasticMeta> {
            if elements.len() == 1 && elements[0].0.is_empty() {
                struct_metadata::Entry {
                    label,
                    docs: None,
                    metadata: ElasticMeta { index: Some(true), store: Some(false), ..Default::default() },
                    type_info: elements[0].1.metadata_type(),
                    has_default: false,
                    aliases: &[],
                }
            } else {

                let mut catagories: HashMap<&'static str, Vec<_>> = HashMap::new();
                for (path, tag) in elements {
                    catagories.entry(path[0]).or_default().push((&path[1..], *tag))
                }

                let mut children = vec![];
                let metadata = ElasticMeta::default();
                for (label, elements) in catagories {
                    let mut entry = make_entry(label, &elements);
                    entry.metadata.forward_propagate_entry_defaults(&metadata, &entry.type_info.metadata);
                    children.push(entry)
                }

                let type_info = struct_metadata::Descriptor {
                    docs: None,
                    metadata,
                    kind: struct_metadata::Kind::Struct { 
                        name: label, 
                        children, 
                    },
                };

                struct_metadata::Entry {
                    label,
                    docs: None,
                    metadata: Default::default(),
                    type_info,
                    has_default: false,
                    aliases: &[]
                }
            }
        }

        let mut children = vec![];
        let metadata = ElasticMeta::default();
        for (label, elements) in catagories {
            let mut entry = make_entry(label, &elements);
            entry.metadata.forward_propagate_entry_defaults(&metadata, &entry.type_info.metadata);
            children.push(entry);
        }

        struct_metadata::Descriptor {
            docs: None,
            metadata,
            kind: struct_metadata::Kind::Struct { 
                name: "Tagging", 
                children, 
            },
        }
    }
}

impl Tagging {

//     pub fn flatten(&self) -> Result<FlatTags, serde_json::Error> {
//         let data = serde_json::to_value(self)?;
//         let serde_json::Value::Object(data) = data else {
//             return Err(serde_json::Error::custom("struct must become object"))
//         };
//         Ok(flatten_tags(data, None))
//     }

    pub fn to_list(&self, safelisted: Option<bool>) -> Result<Vec<TagEntry>, serde_json::Error> {

        fn flatten_inner(output: &mut Vec<TagEntry>, path: &[&str], ) {

        }

        todo!()
        // // let safelisted = safelisted.unwrap_or(false);
        // let mut out = vec![];
        
        // let tag_dict = if let serde_json::Value::Object(obj) = serde_json::to_value(self)? {
        //     flatten_tags(obj, None)
        // } else {
        //     return Err(serde_json::Error::custom("tags couldn't fold to json"));
        // };

        // for (tag_type, values) in tag_dict.into_iter() {
        //     for tag_value in values {
        //         out.push(TagEntry { 
        //             score: 0, 
        //             tag_type: tag_type.clone(), 
        //             value: tag_value, 
        //         }); // {'safelisted': safelisted, 'type': k, 'value': t, 'short_type': k.rsplit(".", 1)[-1]})
        //     }
        // }
        // Ok(out)
    }
}


// MARK: Flat Tag Container
/// List of validated tags
#[derive(Debug, Default)]
pub struct FlatTags {
    tags: HashMap<&'static TagInformation, Vec<TagValue>>,
}

impl IntoIterator for FlatTags {
    type Item = (&'static TagInformation, Vec<TagValue>);
    type IntoIter = <HashMap<&'static TagInformation, Vec<TagValue>> as std::iter::IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter { self.tags.into_iter() }
}

impl std::ops::Deref for FlatTags {
    type Target = HashMap<&'static TagInformation, Vec<TagValue>>;

    fn deref(&self) -> &Self::Target { &self.tags }
}

impl std::ops::DerefMut for FlatTags {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.tags }
}

#[derive(Debug, thiserror::Error)]
#[error("A tag name collision on {0} prevented a tagging document from being completed")]
pub struct TagNameCollision(String);


impl FlatTags {
    pub fn to_tagging(self) -> Result<Tagging, TagNameCollision> {
        let mut output = JsonMap::default();
        fn insert(info: &'static TagInformation, output: &mut JsonMap, name: &[&str], values: Vec<TagValue>) -> Result<(), TagNameCollision> {
            if name.len() == 1 {
                let inner = output.entry(name[0])
                    .or_insert_with(|| serde_json::Value::Array(vec![]));
                match inner.as_array_mut() {
                    Some(obj) => { obj.extend(values.into_iter().map(|tag| tag.0)); Ok(()) },
                    None => Err(TagNameCollision(info.full_path())),
                }
            } else {
                let inner = output.entry(name[0])
                    .or_insert_with(|| serde_json::Value::Object(JsonMap::default()));
                match inner.as_object_mut() {
                    Some(obj) => insert(info, obj, &name[1..], values),
                    None => Err(TagNameCollision(info.full_path())),
                }
            }
        }
        for (info, values) in self.tags {
            insert(info, &mut output, info.name, values)?;
        }
        Ok(Tagging(output))
    }
}


pub fn load_tags(data: HashMap<String, Vec<serde_json::Value>>) -> (FlatTags, Vec<(String, String)>) {

    let mut accepted = FlatTags::default();
    let mut rejected = vec![];

    for (name, values) in data {
        match get_tag_information(&name) {
            Some(info) => {
                for value in values {
                    match info.processor.apply(value) {
                        Ok(value) => accepted.entry(info).or_default().push(TagValue(value)),
                        Err(value) => rejected.push((name.to_string(), TagValue(value).to_string()))
                    }
                }
            },
            None => {
                for value in values {
                    rejected.push((name.to_string(), TagValue(value).to_string()))
                }
            }
        }
    }

    (accepted, rejected)
}

// MARK: Tests

/// test for invalid tag names
#[test]
fn tag_names() {
    use serde_json::{Value, json};

    let mut input: HashMap<String, Vec<Value>> = HashMap::new();
    input.insert("attribution.actor".to_string(), vec![json!("abc"), json!("Big hats!"), json!([]), json!(100), json!(Option::<()>::None)]);
    input.insert("av.heuristic".to_string(), vec![json!("abc"), json!("Big hats!"), json!([]), json!(100), json!(Option::<()>::None)]);
    input.insert("av.heuristic.".to_string(), vec![json!("100000")]);
    input.insert(".av.heuristic".to_string(), vec![json!("100000")]);
    input.insert("av".to_string(), vec![json!("100000")]);
    // input.insert("network.tls.ja3_hash".to_string(), vec![json!("abc"), json!("Big hats!"), json!([]), json!(100), json!(None)]);

    let (accepted, mut rejected) = load_tags(input);

    assert_eq!(accepted.len(), 2);
    assert_eq!(rejected.len(), 7);

    assert_eq!(*accepted.get(get_tag_information("attribution.actor").unwrap()).unwrap(), vec![TagValue(json!("ABC")), TagValue(json!("BIG HATS!")), TagValue(json!("100"))]);
    assert_eq!(*accepted.get(get_tag_information("av.heuristic").unwrap()).unwrap(), vec![TagValue(json!("abc")), TagValue(json!("Big hats!")), TagValue(json!("100"))]);

    rejected.sort_unstable();
    assert_eq!(rejected, vec![
        (".av.heuristic".to_string(), "100000".to_string()),
        ("attribution.actor".to_string(), "[]".to_string()),
        ("attribution.actor".to_string(), "null".to_string()),
        ("av".to_string(), "100000".to_string()),
        ("av.heuristic".to_string(), "[]".to_string()),
        ("av.heuristic".to_string(), "null".to_string()),
        ("av.heuristic.".to_string(), "100000".to_string()),
    ]);
}


/// Test parsing basic strings and uppercased strings
#[test]
fn string_tag_parsing() {
    use serde_json::{Value, json};

    let proc = TagProcessor::String;
    assert_eq!(proc.apply(json!("abc")), Ok(json!("abc")));
    assert_eq!(proc.apply(json!("Big Hats!")), Ok(json!("Big Hats!")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Ok(json!("100")));
    assert_eq!(proc.apply(Value::Null), Err(Value::Null));

    let proc = TagProcessor::Lowercase;
    assert_eq!(proc.apply(json!("abc")), Ok(json!("abc")));
    assert_eq!(proc.apply(json!("Big Hats!")), Ok(json!("big hats!")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Ok(json!("100")));
    assert_eq!(proc.apply(Value::Null), Err(Value::Null));

    let proc = TagProcessor::Uppercase;
    assert_eq!(proc.apply(json!("abc")), Ok(json!("ABC")));
    assert_eq!(proc.apply(json!("Big Hats!")), Ok(json!("BIG HATS!")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Ok(json!("100")));
    assert_eq!(proc.apply(Value::Null), Err(Value::Null));

}

#[test]
fn hash_tag_parsing() {
    use serde_json::json;
    
    let proc = TagProcessor::MD5;
    assert_eq!(proc.apply(json!("00000000000000000000000000000000")), Ok(json!("00000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")), Err(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")));
    assert_eq!(proc.apply(json!("t13d1517h2_8daaf6152771_b0da82dd1658")), Err(json!("t13d1517h2_8daaf6152771_b0da82dd1658")));
    assert_eq!(proc.apply(json!(999)), Err(json!(999)));
    
    let proc = TagProcessor::Sha1;
    assert_eq!(proc.apply(json!("00000000000000000000000000000000")), Err(json!("00000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000")), Ok(json!("0000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")), Err(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")));
    assert_eq!(proc.apply(json!("t13d1517h2_8daaf6152771_b0da82dd1658")), Err(json!("t13d1517h2_8daaf6152771_b0da82dd1658")));
    assert_eq!(proc.apply(json!(999)), Err(json!(999)));

    let proc = TagProcessor::Sha256;
    assert_eq!(proc.apply(json!("00000000000000000000000000000000")), Err(json!("00000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000000000000000000000000000")), Ok(json!("0000000000000000000000000000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")), Err(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")));
    assert_eq!(proc.apply(json!("t13d1517h2_8daaf6152771_b0da82dd1658")), Err(json!("t13d1517h2_8daaf6152771_b0da82dd1658")));
    assert_eq!(proc.apply(json!(999)), Err(json!(999)));

    let proc = TagProcessor::SSDeepHash;
    assert_eq!(proc.apply(json!("00000000000000000000000000000000")), Err(json!("00000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")), Ok(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")));
    assert_eq!(proc.apply(json!("t13d1517h2_8daaf6152771_b0da82dd1658")), Err(json!("t13d1517h2_8daaf6152771_b0da82dd1658")));
    assert_eq!(proc.apply(json!(999)), Err(json!(999)));

    let proc = TagProcessor::JA4;
    assert_eq!(proc.apply(json!("00000000000000000000000000000000")), Err(json!("00000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("0000000000000000000000000000000000000000000000000000000000000000")), Err(json!("0000000000000000000000000000000000000000000000000000000000000000")));
    assert_eq!(proc.apply(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")), Err(json!("24576:c+bnyhC57zhu0Nbs2p/ojPgZmAnShaLOZHzYX20:zwQB3bN/MkNbOZS20")));
    assert_eq!(proc.apply(json!("t13d1517h2_8daaf6152771_b0da82dd1658")), Ok(json!("t13d1517h2_8daaf6152771_b0da82dd1658")));
    assert_eq!(proc.apply(json!(999)), Err(json!(999)));

}

#[test]
fn network_tag_parsing() {
    use serde_json::json;

    let proc = TagProcessor::Domain;
    assert_eq!(proc.apply(json!("www.google.com")), Ok(json!("www.google.com")));
    assert_eq!(proc.apply(json!("www.GooGle.com")), Ok(json!("www.google.com")));
    assert_eq!(proc.apply(json!("172.0.0.1")), Err(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("")), Err(json!("")));

    let proc = TagProcessor::IpAddress;
    assert_eq!(proc.apply(json!("www.google.com")), Err(json!("www.google.com")));
    assert_eq!(proc.apply(json!("www.GooGle.com")), Err(json!("www.GooGle.com")));
    assert_eq!(proc.apply(json!("172.0.0.1")), Ok(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("1234:5678:9ABC:0000:0000:1234:5678:9abc")), Ok(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")));
    
    let proc = TagProcessor::UNCPath;
    assert_eq!(proc.apply(json!("www.google.com")), Err(json!("www.google.com")));
    assert_eq!(proc.apply(json!("www.GooGle.com")), Err(json!("www.GooGle.com")));
    assert_eq!(proc.apply(json!("172.0.0.1")), Err(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")), Err(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")));
    assert_eq!(proc.apply(json!(r"\\ComputerName\SharedFolder\Resource")), Ok(json!(r"\\ComputerName\SharedFolder\Resource")));
    assert_eq!(proc.apply(json!(r"\\hostname@SSL@100\SharedFolder\Resource")), Ok(json!(r"\\hostname@SSL@100\SharedFolder\Resource")));
 
    let proc = TagProcessor::Uri;
    assert_eq!(proc.apply(json!("www.GooGle.com")), Err(json!("www.GooGle.com")));
    assert_eq!(proc.apply(json!("172.0.0.1")), Err(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("1234:5678:9ABC:0000:0000:1234:5678:9abc")), Err(json!("1234:5678:9ABC:0000:0000:1234:5678:9abc")));
    assert_eq!(proc.apply(json!("method://172.0.0.1")), Ok(json!("method://172.0.0.1")));
    assert_eq!(proc.apply(json!("s3://1234:5678:9ABC:0000:0000:1234:5678:9abc")), Ok(json!("s3://1234:5678:9ABC:0000:0000:1234:5678:9ABC")));
    assert_eq!(proc.apply(json!("https://www.google.com")), Ok(json!("https://www.google.com")));
    assert_eq!(proc.apply(json!("https://www.GooGle.com/hellow?x=100&x=red")), Ok(json!("https://www.google.com/hellow?x=100&x=red")));
    assert_eq!(proc.apply(json!("https://172.0.0.1")), Ok(json!("https://172.0.0.1")));
    assert_eq!(proc.apply(json!("HTTPS://172.0.0.1/path/woith%20/components")), Ok(json!("HTTPS://172.0.0.1/path/woith%20/components")));
    assert_eq!(proc.apply(json!("ftp://1234:5678:9ABC:0000:0000:1234:5678:9abc")), Ok(json!("ftp://1234:5678:9ABC:0000:0000:1234:5678:9ABC")));

    let proc = TagProcessor::UriPath;
    assert_eq!(proc.apply(json!("www.google.com")), Err(json!("www.google.com")));
    assert_eq!(proc.apply(json!("www.GooGle.com")), Err(json!("www.GooGle.com")));
    assert_eq!(proc.apply(json!("172.0.0.1")), Err(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")), Err(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")));
    assert_eq!(proc.apply(json!(r"\\ComputerName\SharedFolder\Resource")), Err(json!(r"\\ComputerName\SharedFolder\Resource")));
    assert_eq!(proc.apply(json!(r"\\hostname@SSL@100\SharedFolder\Resource")), Err(json!(r"\\hostname@SSL@100\SharedFolder\Resource")));
    assert_eq!(proc.apply(json!(r"/path%20/words1/")), Ok(json!(r"/path%20/words1/")));


    let proc = TagProcessor::EmailAddress;
    assert_eq!(proc.apply(json!("www.google.com")), Err(json!("www.google.com")));
    assert_eq!(proc.apply(json!("www.GooGle.com")), Err(json!("www.GooGle.com")));
    assert_eq!(proc.apply(json!("172.0.0.1")), Err(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("user@www.google.com")), Ok(json!("user@www.google.com")));
    assert_eq!(proc.apply(json!("user@www.GooGle.com")), Ok(json!("user@www.google.com")));
    assert_eq!(proc.apply(json!("user@172.0.0.1")), Err(json!("user@172.0.0.1")));


    let proc = TagProcessor::Mac;
    assert_eq!(proc.apply(json!("172.0.0.1")), Err(json!("172.0.0.1")));
    assert_eq!(proc.apply(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")), Err(json!("1234:5678:9ABC:0000:0000:1234:5678:9ABC")));
    assert_eq!(proc.apply(json!("00:1b:63:84:45:e6")), Ok(json!("00:1b:63:84:45:e6")));
    assert_eq!(proc.apply(json!("00-1B-63-84-45-E6")), Ok(json!("00-1b-63-84-45-e6")));

}


#[test]
fn misc_tag_parsing() {
    use serde_json::{Value, json};

    let proc = TagProcessor::PhoneNumber;
    assert_eq!(proc.apply(json!("abc")), Err(json!("abc")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Err(json!(100)));
    // algerian phone number
    // assert_eq!(proc.apply(json!("+213 21 55 55 55")), Err(json!("")));
    assert_eq!(proc.apply(json!("+1 100 100 1000")), Ok(json!("+1 100 100 1000")));
    // assert_eq!(proc.apply(json!("+61 5555 5555")), Ok(json!("+61 5555 5555")));

    let proc = TagProcessor::RuleMapping;
    assert_eq!(proc.apply(json!("abc")), Err(json!("abc")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Err(json!(100)));
    assert_eq!(proc.apply(json!({})), Ok(json!({})));
    assert_eq!(proc.apply(json!({"123": {}})), Err(json!({"123": {}})));
    assert_eq!(proc.apply(json!({"123": []})), Ok(json!({"123": []})));
    assert_eq!(proc.apply(json!({"123": [[], "abc", 12]})), Ok(json!({"123": ["[]", "abc", "12"]})));
}

#[test]
fn number_tag_parsing() {
    use serde_json::{Value, json};

    let proc = TagProcessor::I32;
    assert_eq!(proc.apply(json!("abc")), Err(json!("abc")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Ok(json!(100)));
    assert_eq!(proc.apply(json!(-100)), Ok(json!(-100)));
    assert_eq!(proc.apply(json!(Option::<()>::None)), Err(Value::Null));
    assert_eq!(proc.apply(json!("55")), Ok(json!(55)));
    assert_eq!(proc.apply(json!(10_000_000_000u64)), Err(json!("10000000000")));

    let proc = TagProcessor::U16;
    assert_eq!(proc.apply(json!("abc")), Err(json!("abc")));
    assert_eq!(proc.apply(json!([])), Err(json!([])));
    assert_eq!(proc.apply(json!(100)), Ok(json!(100)));
    assert_eq!(proc.apply(json!(-100)), Err(json!("-100")));
    assert_eq!(proc.apply(json!(Option::<()>::None)), Err(Value::Null));
    assert_eq!(proc.apply(json!("55")), Ok(json!(55)));
    assert_eq!(proc.apply(json!(1_000_000)), Err(json!("1000000")));

}