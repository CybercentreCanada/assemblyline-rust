use std::collections::HashMap;

use serde::de::Error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use struct_metadata::Described;

use crate::messages::task::TagEntry;
use crate::{Domain, ElasticMeta, Email, JsonMap, Mac, PhoneNumber, Platform, Processor, SSDeepHash, Sha1, Sha256, UNCPath, UpperString, Uri, UriPath, IP, MD5};

/// Attribution Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct Attribution {
    /// Attribution Actor
    pub actor: Option<Vec<UpperString>>,
    /// Attribution Campaign
    pub campaign: Option<Vec<UpperString>>,
    /// Attribution Category
    pub category: Option<Vec<UpperString>>,
    /// Attribution Exploit
    pub exploit: Option<Vec<UpperString>>,
    /// Attribution Implant
    pub implant: Option<Vec<UpperString>>,
    /// Attribution Family
    pub family: Option<Vec<UpperString>>,
    /// Attribution Network
    pub network: Option<Vec<UpperString>>,
}

/// Antivirus Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct AV {
    /// List of heuristics
    pub heuristic: Option<Vec<String>>,
    /// Collection of virus names identified by antivirus tools
    pub virus_name: Option<Vec<String>>,
}

/// Valid Certificate Period
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct CertValid {
    /// Start date of certificate validity
    pub start: Option<Vec<String>>,
    /// End date of certificate validity
    pub end: Option<Vec<String>>,
}

/// Certificate Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct Cert {
    /// Extended Key Usage
    pub extended_key_usage: Option<Vec<String>>,
    /// Issuer
    pub issuer: Option<Vec<String>>,
    /// Key Usage
    pub key_usage: Option<Vec<String>>,
    /// Owner
    pub owner: Option<Vec<String>>,
    /// Serial Number
    pub serial_no: Option<Vec<String>>,
    /// Signature Algorithm
    pub signature_algo: Option<Vec<String>>,
    /// Subject Name
    pub subject: Option<Vec<String>>,
    /// Alternative Subject Name
    pub subject_alt_name: Option<Vec<String>>,
    /// Thumbprint
    pub thumbprint: Option<Vec<String>>,
    /// Validity Information
    pub valid: Option<CertValid>,
    /// Version
    pub version: Option<Vec<String>>,
}

/// Dynamic Process
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct DynamicProcess {
    /// Commandline
    pub command_line: Option<Vec<String>>,
    /// Filename
    pub file_name: Option<Vec<String>>,
    /// Shortcut
    pub shortcut: Option<Vec<String>>,
}

/// Signatures
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct DynamicSignature {
    /// Signature Category
    pub category: Option<Vec<String>>,
    /// Signature Family
    pub family: Option<Vec<String>>,
    /// Signature Name
    pub name: Option<Vec<String>>,
}

/// SSDeep
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct DynamicSSDeep {
    /// CLSIDs
    pub cls_ids: Option<Vec<SSDeepHash>>,
    /// Dynamic Classes
    pub dynamic_classes: Option<Vec<SSDeepHash>>,
    /// Registry Keys
    pub regkeys: Option<Vec<SSDeepHash>>,
}

/// Windows
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct DynamicWindow {
    /// CLSIDs
    pub cls_ids: Option<Vec<String>>,
    /// Dynamic Classes
    pub dynamic_classes: Option<Vec<String>>,
    /// Registry Keys
    pub regkeys: Option<Vec<String>>,
}

/// Operating System
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct DynamicOperatingSystem {
    /// Platform
    pub platform: Option<Vec<Platform>>,
    /// Version
    pub version: Option<Vec<String>>,
    /// Processor
    pub processor: Option<Vec<Processor>>,
}

/// Dynamic Tag Model. Commonly Used by Dynamic Analysis
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct Dynamic {
    /// Autorun location
    pub autorun_location: Option<Vec<String>>,
    /// DOS Device
    pub dos_device: Option<Vec<String>>,
    /// Mutex
    pub mutex: Option<Vec<String>>,
    /// Registy Keys
    pub registry_key: Option<Vec<String>>,
    /// Sandbox Processes
    pub process: Option<DynamicProcess>,
    /// Sandbox Signatures
    pub signature: Option<DynamicSignature>,
    /// Sandbox SSDeep
    pub ssdeep: Option<DynamicSSDeep>,
    /// Sandbox Window
    pub window: Option<DynamicWindow>,
    /// Sandbox Operating System
    pub operating_system: Option<DynamicOperatingSystem>,
    /// Process Tree ID
    pub processtree_id: Option<Vec<String>>,
}

/// General Information Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct TaggingInfo {
    pub phone_number: Option<Vec<PhoneNumber>>,
    /// Password
    pub password: Option<Vec<String>>,
}

/// APK Application Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileAPKApp {
    /// Label
    pub label: Option<Vec<String>>,
    /// Version
    pub version: Option<Vec<String>>,
}

/// APK SDK Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileAPKSDK {
    /// Minimum OS required
    pub min: Option<Vec<String>>,
    /// Target OS
    pub target: Option<Vec<String>>,
}

/// APK File Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileAPK {
    /// Activity
    pub activity: Option<Vec<String>>,
    /// APK Application Information
    pub app: Option<FileAPKApp>,
    /// Features
    pub feature: Option<Vec<String>>,
    /// Locale
    pub locale: Option<Vec<String>>,
    /// Permissions
    pub permission: Option<Vec<String>>,
    /// Package Name
    pub pkg_name: Option<Vec<String>>,
    /// Components Provided
    pub provides_component: Option<Vec<String>>,
    /// APK SDK Information
    pub sdk: Option<FileAPKSDK>,
    /// Libraries Used
    pub used_library: Option<Vec<String>>,
}

/// File Date Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileDate {
    /// File Creation Date
    pub creation: Option<Vec<String>>,
    /// File Last Modified Date
    pub last_modified: Option<Vec<String>>,
}

/// ELF Sections
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileELFSections {
    /// Section Name
    pub name: Option<Vec<String>>,
}

/// ELF Segments
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileELFSegments {
    /// Segment Type
    #[serde(rename = "type")]
    pub segment_type: Option<Vec<String>>,
}

/// ELF Notes
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileELFNotes {
    /// Note Name
    pub name: Option<Vec<String>>,
    /// Note Type
    #[serde(rename = "type")]
    pub note_type: Option<Vec<String>>,
    /// Note Core Type
    pub type_core: Option<Vec<String>>,
}

/// ELF File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileELF {
    /// Libraries
    pub libraries: Option<Vec<String>>,
    /// Interpreter
    pub interpreter: Option<Vec<String>>,
    /// ELF Sections
    pub sections: Option<FileELFSections>,
    /// ELF Segments
    pub segments: Option<FileELFSegments>,
    /// ELF Notes
    pub notes: Option<FileELFNotes>,
}

/// Exiftool Information Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileIMGExiftool {
    /// Image Creation Tool
    pub creator_tool: Option<Vec<String>>,
    /// Derived Document ID
    pub derived_document_id: Option<Vec<String>>,
    /// Document ID
    pub document_id: Option<Vec<String>>,
    /// Instance ID
    pub instance_id: Option<Vec<String>>,
    /// Toolkit
    pub toolkit: Option<Vec<String>>,
}

/// Image File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileIMG {
    /// Exiftool Information
    pub exif_tool: Option<FileIMGExiftool>,
    /// Megapixels
    pub mega_pixels: Option<Vec<String>>,
    /// Image Mode
    pub mode: Option<Vec<String>>,
    /// Image Size
    pub size: Option<Vec<String>>,
    /// Sorted Metadata Hash
    pub sorted_metadata_hash: Option<Vec<String>>,
}

/// JAR File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileJAR {
    /// Main Class
    pub main_class: Option<Vec<String>>,
    /// Main Package
    pub main_package: Option<Vec<String>>,
}

/// File Name Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileName {
    /// Name of Anomaly
    pub anomaly: Option<Vec<String>>,
    /// Name of Extracted
    pub extracted: Option<Vec<String>>,
}

/// OLE Macro Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileOLEMacro {
    /// SHA256 of Macro
    pub sha256: Option<Vec<Sha256>>,
    /// Suspicious Strings
    pub suspicious_string: Option<Vec<String>>,
}

/// OLE Summary Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileOLESummary {
    /// Author
    pub author: Option<Vec<String>>,
    /// Code Page
    pub codepage: Option<Vec<String>>,
    /// Comment
    pub comment: Option<Vec<String>>,
    /// Company
    pub company: Option<Vec<String>>,
    /// Creation Time
    pub create_time: Option<Vec<String>>,
    /// Date Last Printed
    pub last_printed: Option<Vec<String>>,
    /// User Last Saved By
    pub last_saved_by: Option<Vec<String>>,
    /// Date Last Saved
    pub last_saved_time: Option<Vec<String>>,
    /// Manager
    pub manager: Option<Vec<String>>,
    /// Subject
    pub subject: Option<Vec<String>>,
    /// Title
    pub title: Option<Vec<String>>,
}

/// OLE File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileOLE {
    /// OLE Macro
    #[serde(rename = "macro")]
    pub ole_macro: Option<FileOLEMacro>,
    /// OLE Summary
    pub summary: Option<FileOLESummary>,
    /// CLSID
    pub clsid: Option<Vec<String>>,
    /// DDE Link
    pub dde_link: Option<Vec<String>>,
    /// FIB Timestamp
    pub fib_timestamp: Option<Vec<String>>,
}

/// PDF Date Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePDFDate {
    /// Date Modified
    pub modified: Option<Vec<String>>,
    /// PDFx
    pub pdfx: Option<Vec<String>>,
    /// Date Source Modified
    pub source_modified: Option<Vec<String>>,
}

/// PDF Javascript Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePDFJavascript {
    /// SHA1 of Javascript
    pub sha1: Option<Vec<Sha1>>,
}

/// PDF Statistics Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePDFStats {
    /// SHA1 of Statistics
    pub sha1: Option<Vec<Sha1>>,
}

/// PDF File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePDF {
    /// PDF Date Information
    pub date: Option<FilePDFDate>,
    /// PDF Javascript Information
    pub javascript: Option<FilePDFJavascript>,
    /// PDF Statistics Information
    pub stats: Option<FilePDFStats>,
}

/// PE Debug Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePEDebug {
    /// GUID
    pub guid: Option<Vec<String>>,
}

/// PE Exports Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePEExports {
    /// Function Name
    pub function_name: Option<Vec<String>>,
    /// Module Name
    pub module_name: Option<Vec<String>>,
}

/// PE Imports Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePEImports {
    /// Fuzzy
    pub fuzzy: Option<Vec<SSDeepHash>>,
    /// MD5
    pub md5: Option<Vec<MD5>>,
    /// Imphash
    pub imphash: Option<Vec<MD5>>,
    /// Sorted Fuzzy
    pub sorted_fuzzy: Option<Vec<SSDeepHash>>,
    /// Sorted SHA1
    pub sorted_sha1: Option<Vec<Sha1>>,
    /// Go Import hash
    pub gimphash: Option<Vec<Sha256>>,
    /// Suspicious
    pub suspicious: Option<Vec<String>>,
}

/// PE Linker Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePELinker {
    /// Timestamp
    pub timestamp: Option<Vec<String>>,
}

/// PE OEP Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePEOEP {
    /// Bytes
    pub bytes: Option<Vec<String>>,
    /// Hex Dump
    pub hexdump: Option<Vec<String>>,
}

/// PE Resources Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePEResources {
    /// Language
    pub language: Option<Vec<String>>,
    /// Name
    pub name: Option<Vec<String>>,
}

/// PE Rich Header Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePERichHeader {
    /// Hash
    pub hash: Option<Vec<String>>,
}

/// PE Sections Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePESections {
    /// Hash
    pub hash: Option<Vec<String>>,
    /// Name
    pub name: Option<Vec<String>>,
}

/// PE Versions Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePEVersions {
    /// Description
    pub description: Option<Vec<String>>,
    /// Filename
    pub filename: Option<Vec<String>>,
}

/// PE File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePE {
    /// API Vector
    pub api_vector: Option<Vec<String>>,
    /// PE Debug Information
    pub debug: Option<FilePEDebug>,
    /// PE Exports Information
    pub exports: Option<FilePEExports>,
    /// PE Imports Information
    pub imports: Option<FilePEImports>,
    /// PE Linker Information
    pub linker: Option<FilePELinker>,
    /// PE OEP Information
    pub oep: Option<FilePEOEP>,
    /// PDB Filename
    pub pdb_filename: Option<Vec<String>>,
    /// PE Resources Information
    pub resources: Option<FilePEResources>,
    /// PE Rich Header Information
    pub rich_header: Option<FilePERichHeader>,
    /// PE Sections Information
    pub sections: Option<FilePESections>,
    /// PE Versions Information
    pub versions: Option<FilePEVersions>,
}

/// PList Build Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListBuild {
    /// Machine OS
    pub machine_os: Option<Vec<String>>,
}

/// PList CF Bundle Version Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListCFBundleVersion {
    /// Long Version
    pub long: Option<Vec<String>>,
    /// Short Version
    pub short: Option<Vec<String>>,
}

/// PList CF Bundle Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListCFBundle {
    /// Development Region
    pub development_region: Option<Vec<String>>,
    /// Display Name
    pub display_name: Option<Vec<String>>,
    /// Executable Name
    pub executable: Option<Vec<String>>,
    /// Identifier Name
    pub identifier: Option<Vec<String>>,
    /// Bundle Name
    pub name: Option<Vec<String>>,
    /// Package Type
    pub pkg_type: Option<Vec<String>>,
    /// Signature
    pub signature: Option<Vec<String>>,
    /// URL Scheme
    pub url_scheme: Option<Vec<String>>,
    /// Bundle Version Information
    pub version: Option<FilePListCFBundleVersion>,
}

/// PList DT Platform Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListDTPlatform {
    /// Build
    pub build: Option<Vec<String>>,
    /// Name
    pub name: Option<Vec<String>>,
    /// Version
    pub version: Option<Vec<String>>,
}

/// PList DT Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListDT {
    /// Compiler
    pub compiler: Option<Vec<String>>,
    /// Platform Information
    pub platform: Option<FilePListDTPlatform>,
}

/// PList LS Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListLS {
    /// Background Only
    pub background_only: Option<Vec<String>>,
    /// Minimum System Versuion
    pub min_system_version: Option<Vec<String>>,
}

/// PList NS Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListNS {
    /// Apple Script Enabled
    pub apple_script_enabled: Option<Vec<String>>,
    /// Principal Class
    pub principal_class: Option<Vec<String>>,
}

/// PList UI Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListUI {
    /// Background Modes
    pub background_modes: Option<Vec<String>>,
    /// Requires Persistent WIFI
    pub requires_persistent_wifi: Option<Vec<String>>,
}

/// PList WK Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePListWK {
    /// App Bundle ID
    pub app_bundle_identifier: Option<Vec<String>>,
}

/// PList File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePList {
    /// Installer URL
    pub installer_url: Option<Vec<String>>,
    /// Minimum OS Version
    pub min_os_version: Option<Vec<String>>,
    /// Requests Open Access
    pub requests_open_access: Option<Vec<String>>,
    /// Build Information
    pub build: Option<FilePListBuild>,
    /// CF Bundle Information
    pub cf_bundle: Option<FilePListCFBundle>,
    /// DT Information
    pub dt: Option<FilePListDT>,
    /// LS Information
    pub ls: Option<FilePListLS>,
    /// NS Information
    pub ns: Option<FilePListNS>,
    /// UI Information
    pub ui: Option<FilePListUI>,
    /// WK Information
    pub wk: Option<FilePListWK>,
}

/// PowerShell File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FilePowerShell {
    /// Cmdlet
    pub cmdlet: Option<Vec<String>>,
}

/// Shortcut File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileShortcut {
    /// Command Line
    pub command_line: Option<Vec<String>>,
    /// Icon Location
    pub icon_location: Option<Vec<String>>,
    /// Machine ID
    pub machine_id: Option<Vec<String>>,
    /// Possible MAC address from the Tracker block
    pub tracker_mac: Option<Vec<String>>,
}

/// Strings File Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileStrings {
    /// API
    pub api: Option<Vec<String>>,
    /// Blacklisted
    pub blacklisted: Option<Vec<String>>,
    /// Decoded
    pub decoded: Option<Vec<String>>,
    /// Extracted
    pub extracted: Option<Vec<String>>,
}

/// SWF Header Frame
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileSWFHeaderFrame {
    /// Number of Frames
    pub count: Option<Vec<i64>>,
    /// Speed of Animation
    pub rate: Option<Vec<String>>,
    /// Size of Frame
    pub size: Option<Vec<String>>,
}

/// SWF Header Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileSWFHeader {
    /// Header Frame Information
    pub frame: Option<FileSWFHeaderFrame>,
    /// Version
    pub version: Option<Vec<String>>,
}

/// SWF File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct FileSWF {
    /// Header Information
    pub header: Option<FileSWFHeader>,
    /// Tags SSDeep
    pub tags_ssdeep: Option<Vec<SSDeepHash>>,
}

/// Network IOC Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct NetworkIOCs {
    /// Domain
    pub domain: Option<Vec<Domain>>,
    /// IP
    pub ip: Option<Vec<IP>>,
    /// UNC Path
    pub unc_path: Option<Vec<UNCPath>>,
    /// URI
    pub uri: Option<Vec<Uri>>,
    /// URI Path
    pub uri_path: Option<Vec<UriPath>>,
}

/// Network Email Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct NetworkEmail {
    /// Email Address
    pub address: Option<Vec<Email>>,
    /// Date
    pub date: Option<Vec<String>>,
    /// Subject
    pub subject: Option<Vec<String>>,
    /// Message ID
    pub msg_id: Option<Vec<String>>,
}

/// Network Signature Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct NetworkSignature {
    /// Signature ID
    pub signature_id: Option<Vec<String>>,
    /// Signature Message
    pub message: Option<Vec<String>>,
}

/// Network TLS Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct NetworkTLS {
    /// JA3 Hash
    pub ja3_hash: Option<Vec<MD5>>,
    /// JA3 String
    pub ja3_string: Option<Vec<String>>,
    /// SNI
    pub sni: Option<Vec<String>>,
}

/// Network Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct Network {
    /// Attack
    pub attack: Option<Vec<String>>,
    /// Dynamic IOCs
    pub dynamic: Option<NetworkIOCs>,
    /// Email
    pub email: Option<NetworkEmail>,
    /// MAC Address
    pub mac_address: Option<Vec<Mac>>,
    /// Port
    pub port: Option<Vec<i64>>,
    /// Protocol
    pub protocol: Option<Vec<String>>,
    /// Signatures
    pub signature: Option<NetworkSignature>,
    /// Static IOCs
    #[serde(rename = "static")]
    pub static_ioc: Option<NetworkIOCs>,
    /// TLS
    pub tls: Option<NetworkTLS>,
    /// User Agent
    pub user_agent: Option<Vec<String>>,
}

/// Technique Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct Technique {
    /// Communication Routine
    pub comms_routine: Option<Vec<String>>,
    /// Configuration
    pub config: Option<Vec<String>>,
    /// Cryptography
    pub crypto: Option<Vec<String>>,
    /// Keylogger
    pub keylogger: Option<Vec<String>>,
    /// Macro
    #[serde(rename = "macro")]
    pub macro_string: Option<Vec<String>>,
    /// Masking Algorithm
    pub masking_algo: Option<Vec<String>>,
    /// Obfuscation
    pub obfuscation: Option<Vec<String>>,
    /// Packer
    pub packer: Option<Vec<String>>,
    /// Persistence
    pub persistence: Option<Vec<String>>,
    /// Shell Code
    pub shellcode: Option<Vec<String>>,
    /// String
    pub string: Option<Vec<String>>,
}

/// File Tag Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct TaggingFile {
    /// File Genealogy
    pub ancestry: Option<Vec<String>>,
    /// File Behaviour
    pub behavior: Option<Vec<String>>,
    /// Compiler of File
    pub compiler: Option<Vec<String>>,
    /// File Configuration
    pub config: Option<Vec<String>>,
    /// File's Date Information
    pub date: Option<FileDate>,
    /// ELF File Properties
    pub elf: Option<FileELF>,
    /// File Libraries
    pub lib: Option<Vec<String>>,
    /// File LSH hashes
    pub lsh: Option<Vec<String>>,
    /// File Name
    pub name: Option<FileName>,
    /// File Path
    pub path: Option<Vec<String>>,
    /// Rule/Signature File
    pub rule: Option<HashMap<String, Vec<String>>>,
    /// File Strings Properties
    pub string: Option<FileStrings>,
    /// APK File Properties
    pub apk: Option<FileAPK>,
    /// JAR File Properties
    pub jar: Option<FileJAR>,
    /// Image File Properties
    pub img: Option<FileIMG>,
    /// OLE File Properties
    pub ole: Option<FileOLE>,
    /// PE File Properties
    pub pe: Option<FilePE>,
    /// PDF File Properties
    pub pdf: Option<FilePDF>,
    /// PList File Properties
    pub plist: Option<FilePList>,
    /// PowerShell File Properties
    pub powershell: Option<FilePowerShell>,
    /// Shortcut File Properties
    pub shortcut: Option<FileShortcut>,
    /// SWF File Properties
    pub swf: Option<FileSWF>,
}

/// Tagging Model
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(copyto="__text__")]
pub struct Tagging {
    /// Attribution Tagging
    pub attribution: Option<Box<Attribution>>,
    /// Antivirus Tagging
    pub av: Option<Box<AV>>,
    /// Certificate Tagging
    pub cert: Option<Box<Cert>>,
    /// Dynamic Analysis Tagging
    pub dynamic: Option<Box<Dynamic>>,
    /// Informational Tagging
    pub info: Option<Box<TaggingInfo>>,
    /// File Tagging
    pub file: Option<Box<TaggingFile>>,
    /// Network Tagging
    pub network: Option<Box<Network>>,
    /// Source Tagging
    pub source: Option<Box<Vec<String>>>,
    /// Technique Tagging
    pub technique: Option<Box<Technique>>,
    /// Vector Tagging
    pub vector: Option<Box<Vec<String>>>,
}


fn into_string(value: serde_json::Value) -> String {
    if let serde_json::Value::String(string) = value {
        string
    } else {
        value.to_string()
    }
}


pub fn flatten_tags(data: JsonMap, parent_key: Option<&str>) -> FlatTags {
    let mut items = vec![];
    for (k, v) in data {
        let cur_key = match parent_key {
            Some(parent_key) => format!("{parent_key}.{k}"),
            None => k,
        };

        if let serde_json::Value::Object(obj) = v {
            items.extend(flatten_tags(obj, Some(&cur_key)).into_iter())
        } else if let serde_json::Value::Array(arr) = v {
            let mut mapped = vec![];
            for item in arr {
                if item.is_null() { continue }
                mapped.push(into_string(item))
            }
            if !mapped.is_empty() {
                items.push((cur_key, mapped));
            }
        } else if v.is_null() {

        } else {
            items.push((cur_key, vec![into_string(v)]));
        }
    }

    items.into_iter().collect()
}

impl Tagging {

    pub fn flatten(&self) -> Result<FlatTags, serde_json::Error> {
        let data = serde_json::to_value(self)?;
        let serde_json::Value::Object(data) = data else {
            return Err(serde_json::Error::custom("struct must become object"))
        };
        Ok(flatten_tags(data, None))
    }

    pub fn to_list(&self, _safelisted: Option<bool>) -> Result<Vec<TagEntry>, serde_json::Error> {
        // let safelisted = safelisted.unwrap_or(false);
        let mut out = vec![];
        
        let tag_dict = if let serde_json::Value::Object(obj) = serde_json::to_value(self)? {
            flatten_tags(obj, None)
        } else {
            return Err(serde_json::Error::custom("tags couldn't fold to json"));
        };

        for (tag_type, values) in tag_dict.into_iter() {
            for tag_value in values {
                out.push(TagEntry { 
                    score: 0, 
                    tag_type: tag_type.clone(), 
                    value: tag_value, 
                }); // {'safelisted': safelisted, 'type': k, 'value': t, 'short_type': k.rsplit(".", 1)[-1]})
            }
        }
        Ok(out)
    }
}

// MARK: Flat tags
#[derive(Debug, Default)]
pub struct FlatTags(HashMap<String, Vec<String>>);

impl From<HashMap<String, Vec<String>>> for FlatTags {
    fn from(value: HashMap<String, Vec<String>>) -> Self { Self(value) }
}

impl FromIterator<(String, Vec<String>)> for FlatTags {
    fn from_iter<T: IntoIterator<Item = (String, Vec<String>)>>(iter: T) -> Self { Self(iter.into_iter().collect())}
}

impl IntoIterator for FlatTags {
    type Item = (String, Vec<String>);
    type IntoIter = std::collections::hash_map::IntoIter<String, Vec<String>>;
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

impl std::ops::Deref for FlatTags {
    type Target = HashMap<String, Vec<String>>;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::DerefMut for FlatTags {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

impl FlatTags {
    pub fn unflatten(&self) -> JsonMap {
        let mut finished = JsonMap::new();
        let mut nested_parts: HashMap<String, FlatTags> = Default::default();
        
        for (tag, values) in &self.0 {
            match tag.split_once(".") {
                Some((root, extension)) => {
                    let entry = nested_parts.entry(root.to_string()).or_default();
                    entry.0.insert(extension.to_string(), values.clone());
                },
                None => {
                    finished.insert(tag.to_string(), json!(values));
                }
            }
        }

        for (root, parts) in nested_parts {
            finished.insert(root.to_string(), serde_json::Value::Object(parts.unflatten()));
        }

        finished
    }

    pub fn to_tagging(&self) -> Result<Tagging, serde_json::Error> {
        serde_json::from_value(serde_json::Value::Object(self.unflatten()))
    }

    pub fn into_inner(self) -> HashMap<String, Vec<String>> {
        self.0
    }
}