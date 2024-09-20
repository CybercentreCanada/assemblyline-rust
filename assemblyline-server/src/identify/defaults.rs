use std::collections::HashMap;
use std::sync::OnceLock;

use uuid::Uuid;
use uuid::uuid;

/// Regex patterns used to find Assemblyline type in the reported magic labels
/// Magic bytes translated to possible libmagic labels: https://en.wikipedia.org/wiki/List_of_file_signatures
pub const MAGIC_PATTERNS: [(&str, &str); 97] = [
    ("network/tnef", r"Transport Neutral Encapsulation Format"),
    ("archive/chm", r"MS Windows HtmlHelp Data"),
    ("executable/web/wasm", r"WebAssembly \\(wasm\\) binary module"),
    ("executable/windows/dll64", r"pe32\\+[^\\|]+dll[^\\|]+x86\\-64"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L159
    ("executable/windows/dll64", r"pe32\\+[^\\|]+dll[^\\|]+windows"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L157
    ("executable/windows/dll32", r"pe32[^\\|]+dll"),
    ("executable/windows/pe64", r"pe32\\+[^\\|]+x86\\-64[^\\|]+windows"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L155
    ("executable/windows/pe64", r"pe32\\+[^\\|]+windows"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L153
    ("executable/windows/pe32", r"pe32[^\\|]+windows"),
    ("executable/windows/ia/dll64", r"pe32\\+?[^\\|]+dll[^\\|]+Intel Itanium[^\\|]+windows"),
    ("executable/windows/ia/pe64", r"pe32\\+?[^\\|]+Intel Itanium[^\\|]+windows"),
    ("executable/windows/arm/dll64", r"pe32\\+?[^\\|]+dll[^\\|]+Aarch64[^\\|]+windows"),
    ("executable/windows/arm/pe64", r"pe32\\+?[^\\|]+Aarch64[^\\|]+windows"),
    ("executable/windows/pe", r"pe unknown[^\\|]+windows"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L183
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L185
    ("executable/windows/dos", r"(ms-)?dos executable"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L187
    ("executable/windows/com", r"^com executable"),
    ("executable/windows/dos", r"^8086 relocatable"),
    ("executable/windows/coff", r"^MS Windows COFF"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_elf.yara
    ("executable/linux/elf32", r"^elf 32-bit (l|m)sb executable"),
    ("executable/linux/elf64", r"^elf 64-bit (l|m)sb executable"),
    ("executable/linux/pie32", r"^elf 32-bit (l|m)sb pie executable"),
    ("executable/linux/pie64", r"^elf 64-bit (l|m)sb pie executable"),
    ("executable/linux/so32", r"^elf 32-bit (l|m)sb +shared object"),
    ("executable/linux/so64", r"^elf 64-bit (l|m)sb +shared object"),
    ("executable/linux/coff32", r"^(Intel 80386|i386|80386) COFF"),
    ("executable/linux/coff64", r"^64-bit XCOFF"),
    ("executable/linux/ia/coff64", r"^Intel ia64 COFF"),
    ("executable/linux/misp/ecoff", r"^MIPS[^\\|]+ ECOFF"),
    ("executable/linux/a.out", r"^a.out"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_macho.yara
    ("executable/mach-o", r"^Mach-O"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L171
    ("archive/7-zip", r"^7-zip archive data"),
    ("archive/ace", r"^ACE archive data"),
    ("archive/asar", r"^Electron ASAR archive"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L173
    ("archive/bzip2", r"^bzip2 compressed data"),
    ("archive/cabinet", r"^installshield cab"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_cab.yara
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L199
    ("archive/cabinet", r"^microsoft cabinet archive data"),
    ("archive/cpio", r"cpio archive"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L175
    ("archive/gzip", r"^gzip compressed data"),
    ("archive/iso", r"ISO 9660"),
    ("archive/lzma", r"^LZMA compressed data"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_rar.yara
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L179
    ("archive/rar", r"^rar archive data"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_tar.yara
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L177
    ("archive/tar", r"^(GNU|POSIX) tar archive"),
    ("archive/ar", r"^current ar archive"),
    ("archive/vhd", r"^Microsoft Disk Image"),
    ("archive/vmdk", r"^VMware4? disk image"),
    ("archive/xz", r"^XZ compressed data"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_zip.yara
    ("archive/zip", r"^zip archive data"),
    ("archive/zstd", r"^Zstandard compressed data"),
    ("archive/zpaq", r"^ZPAQ file"),
    ("network/tcpdump", r"^(tcpdump|pcap)"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_pdf.yara
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L149
    ("document/pdf", r"^pdf document"),
    ("document/epub", r"^EPUB document"),
    ("document/mobi", r"^Mobipocket E-book"),
    ("resource/map/warcraft3", r"^Warcraft III map file$"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L169
    ("image/bmp", r"^pc bitmap"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L145
    ("image/gif", r"^gif image data"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L147
    ("image/jpg", r"^jpeg image data"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L151
    ("image/png", r"^png image data"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L167
    ("image/tiff", r"^TIFF image data"),
    ("image/webp", r"Web/P image"),
    ("document/installer/windows", r"(Installation Database|Windows Installer)"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L141
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L143
    ("document/office/excel", r"Microsoft[^\\|]+Excel"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L135
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L137
    ("document/office/powerpoint", r"Microsoft.*PowerPoint"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L131
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L133
    ("document/office/word", r"Microsoft[^\\|]+Word"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_rtf.yara
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L139
    ("document/office/rtf", r"Rich Text Format"),
    ("document/office/ole", r"OLE 2"),
    ("document/office/hwp", r"Hangul \\(Korean\\) Word Processor File"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_ole_cf.yara
    ("document/office/unknown", r"Composite Document File|CDFV2"),
    ("document/office/unknown", r"Microsoft[^\\|]+(OOXML|Document)"),
    ("document/office/unknown", r"Number of (Characters|Pages|Words)"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_swf.yara
    ("audiovisual/flash", r"Macromedia Flash"),
    ("code/autorun", r"microsoft windows autorun"),
    ("code/batch", r"dos batch file"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L181
    ("java/jar", r"[ (]Jar[) ]"),
    // Supported by https://github.com/EmersonElectricCo/fsf/blob/15303aa298414397f9aa5d19ca343040a0fe0bbd/fsf-server/yara/ft_java_class.yara
    ("java/class", r"java class data"),
    ("resource/pyc", r"python [^\\|]+byte"),
    ("resource/pyc", r"^Byte-compiled Python module"),
    ("android/apk", r"Android package \\(APK\\)"),
    ("code/xml", r"OpenGIS KML"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L161
    ("code/xml", r"xml"),
    ("image/tim", r"TIM image"),
    ("network/sff", r"Frame Format"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L197
    ("shortcut/windows", r"^MS Windows shortcut"),
    ("document/email", r"Mime entity text"),
    ("document/email", r"MIME entity, ASCII text"),
    ("metadata/sysmon/evt", r"MS Windows Vista Event Log"),
    ("metadata/sysmon/evt", r"MS Windows 10-11 Event Log"),
    ("metadata/minidump", r"Mini DuMP crash report"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L165
    ("image/emf", r"Windows Enhanced Metafile"),
    ("resource/msvc", r"MSVC \\.res"),
    ("pgp/pubkey", r"^PGP public key"),
    ("pgp/privkey", r"^PGP private key block"),
    ("pgp/encrypted", r"^PGP RSA encrypted session key"),
    ("pgp/message", r"^PGP message Public-Key Encrypted Session Key"),
    ("gpg/symmetric", r"^GPG symmetrically encrypted data"),
    ("video/asf", r"^Microsoft ASF"),
    // Supported by https://github.com/mitre/multiscanner/blob/86e0145ba3c4a34611f257dc78cd2482ed6358db/multiscanner/modules/Metadata/fileextensions.py#L201
    ("code/php", r"^PHP script"),
];


/// LibMagic mimetypes that we blindly trust to assign an Assemblyline type
pub const TRUSTED_MIMES: [(&str, &str); 148] = [
    // Mpeg Audio
    ("audio/mp2", "audio/mp2"),
    ("audio/x-mp2", "audio/mp2"),
    ("audio/mpeg", "audio/mp3"),
    ("audio/mp3", "audio/mp3"),
    ("audio/mpg", "audio/mp3"),
    ("audio/x-mpeg", "audio/mp3"),
    ("audio/x-mp3", "audio/mp3"),
    ("audio/x-mpg", "audio/mp3"),
    ("audio/x-mp4a-latm", "audio/mp4"),
    ("audio/x-m4a", "audio/mp4"),
    ("audio/m4a", "audio/mp4"),
    // Wav Audio
    ("audio/x-wav", "audio/wav"),
    ("audio/wav", "audio/wav"),
    ("audio/vnd.wav", "audio/wav"),
    // Ogg Audio
    ("audio/ogg", "audio/ogg"),
    ("audio/x-ogg", "audio/ogg"),
    // S3M Audio
    ("audio/s3m", "audio/s3m"),
    ("audio/x-s3m", "audio/s3m"),
    // MIDI Audio
    ("audio/midi", "audio/midi"),
    ("audio/x-midi", "audio/midi"),
    // Mpeg video
    ("video/mp4", "video/mp4"),
    // Avi video
    ("video/x-msvideo", "video/avi"),
    ("video/x-avi", "video/avi"),
    ("video/avi", "video/avi"),
    ("video/msvideo", "video/avi"),
    // Divx video
    ("video/divx", "video/divx"),
    ("video/vnd.divx", "video/divx"),
    // Quicktime video
    ("video/quicktime", "video/quicktime"),
    // ASF video
    ("video/x-ms-asf", "video/asf"),
    // Source code C/C++
    ("text/x-c++", "text/plain"),
    ("text/x-c", "text/plain"),
    // Configuration file
    ("application/x-wine-extension-ini", "text/ini"),
    // Python
    ("text/x-python", "code/python"),
    ("text/x-script.python", "code/python"),
    ("application/x-bytecode.python", "resource/pyc"),
    ("text/x-bytecode.python", "resource/pyc"),
    // PHP
    ("text/x-php", "code/php"),
    // XML file
    ("text/xml", "code/xml"),
    // SGML file
    ("text/sgml", "code/sgml"),
    // HTML file
    ("text/html", "text/plain"),
    // Shell script
    ("text/x-shellscript", "code/shell"),
    // RTF
    ("text/rtf", "document/office/rtf"),
    // Troff
    ("text/troff", "text/troff"),
    // Java
    // The text/x-java mime type is not a trusted mime to map to code/java as there are false positives with this.
    // But it is good enough to confirm that the type is at least text/plain.
    // A type of text/plain will then get sent to the yara identification stage.
    ("text/x-java", "text/plain"),
    // Batch
    ("text/x-msdos-batch", "code/batch"),
    // Registry file
    ("text/x-ms-regedit", "text/windows/registry"),
    // Sysmon EVTX file
    ("metadata/sysmon/evt", "application/x-ms-evtx"),
    // JSON file
    ("application/json", "text/json"),
    // Autorun files
    ("application/x-setupscript", "code/autorun"),
    // Bittorrent files
    ("application/x-bittorrent", "application/torrent"),
    ("application/x-torrent", "application/torrent"),
    // Database files
    ("application/x-dbf", "db/dbf"),
    ("application/x-sqlite3", "db/sqlite"),
    // Font
    ("application/vnd.ms-opentype", "resource/font/opentype"),
    ("application/x-font-sfn", "resource/font/x11"),
    // Image Icon
    ("image/vnd.microsoft.icon", "image/icon"),
    ("application/ico", "image/icon"),
    ("image/ico", "image/icon"),
    ("image/icon", "image/icon"),
    ("image/x-ico", "image/icon"),
    ("image/x-icon", "image/icon"),
    ("text/ico", "image/icon"),
    ("image/x-icns", "image/icon"),
    // Image gif
    ("image/gif", "image/gif"),
    // Image WebP
    ("image/webp", "image/webp"),
    // Image BMP
    ("image/bmp", "image/bmp"),
    ("image/x-bmp", "image/bmp"),
    ("image/x-ms-bmp", "image/bmp"),
    // Image metafile
    ("image/wmf", "image/wmf"),
    // Image SVG
    ("image/svg", "image/svg"),
    ("image/svg+xml", "image/svg"),
    // Image JPEG
    ("image/jpeg", "image/jpg"),
    ("image/pjpeg", "image/jpg"),
    // Image PNG
    ("image/png", "image/png"),
    // Image TGA
    ("image/x-tga", "image/tga"),
    ("image/x-icb", "image/tga"),
    // Image TIFF
    ("image/tiff", "image/tiff"),
    // Image Cursor
    ("image/x-win-bitmap", "image/cursor"),
    // Office Outlook email
    ("application/vnd.ms-outlook", "document/office/email"),
    // Office Powerpoint
    ("application/vnd.openxmlformats-officedocument.presentationml.presentation", "document/office/powerpoint"),
    ("application/vnd.ms-powerpoint", "document/office/powerpoint"),
    // Office Excel
    ("application/vnd.ms-excel", "document/office/excel"),
    ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "document/office/excel"),
    // Office Word
    ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "document/office/word"),
    ("application/msword", "document/office/word"),
    // Office encrypted docs
    ("application/encrypted", "document/office/passwordprotected"),
    // MSI file
    ("application/vnd.ms-msi", "document/installer/windows"),
    ("application/x-msi", "document/installer/windows"),
    // PDF Document
    ("application/pdf", "document/pdf"),
    // Postscript document
    ("application/postscript", "document/ps"),
    // Open Document files
    ("application/vnd.oasis.opendocument.chart", "document/odt/chart"),
    ("application/vnd.oasis.opendocument.chart-template", "document/odt/chart"),
    ("application/vnd.oasis.opendocument.database", "db/odt"),
    ("application/vnd.oasis.opendocument.formula", "document/odt/formula"),
    ("application/vnd.oasis.opendocument.formula-template", "document/odt/formula"),
    ("application/vnd.oasis.opendocument.graphics", "document/odt/graphics"),
    ("application/vnd.oasis.opendocument.graphics-flat-xml", "document/odt/graphics"),
    ("application/vnd.oasis.opendocument.graphics-template", "document/odt/graphics"),
    ("application/vnd.oasis.opendocument.presentation", "document/odt/presentation"),
    ("application/vnd.oasis.opendocument.presentation-flat-xml", "document/odt/presentation"),
    ("application/vnd.oasis.opendocument.presentation-template", "document/odt/presentation"),
    ("application/vnd.oasis.opendocument.spreadsheet", "document/odt/spreadsheet"),
    ("application/vnd.oasis.opendocument.spreadsheet-flat-xml", "document/odt/spreadsheet"),
    ("application/vnd.oasis.opendocument.spreadsheet-template", "document/odt/spreadsheet"),
    ("application/vnd.oasis.opendocument.text", "document/odt/text"),
    ("application/vnd.oasis.opendocument.text-flat-xml", "document/odt/text"),
    ("application/vnd.oasis.opendocument.text-template", "document/odt/text"),
    ("application/vnd.oasis.opendocument.text-master", "document/odt/text"),
    ("application/vnd.oasis.opendocument.text-master-template", "document/odt/text"),
    ("application/vnd.oasis.opendocument.web", "document/odt/web"),
    // Archives
    ("application/x-7z-compressed", "archive/7-zip"),
    ("application/x-tar", "archive/tar"),
    ("application/x-tarapplication/x-dbt", "archive/tar"),
    ("application/gzip", "archive/gzip"),
    ("application/vnd.ms-tnef", "archive/tnef"),
    ("application/x-cpio", "archive/cpio"),
    ("application/x-archive", "archive/ar"),
    ("application/zip", "archive/zip"),
    ("application/zlib", "archive/zlib"),
    ("application/x-arj", "archive/arj"),
    ("application/x-lzip", "archive/lzip"),
    ("application/x-lzh-compressed", "archive/lzh"),
    ("application/x-ms-compress-szdd", "archive/szdd"),
    ("application/x-arc", "archive/arc"),
    ("application/x-iso9660-image", "archive/iso"),
    ("application/x-rar", "archive/rar"),
    ("application/x-virtualbox-vhd", "archive/vhd"),
    ("application/x-xz", "archive/xz"),
    ("application/vnd.ms-cab-compressed", "archive/cabinet"),
    ("application/zstd", "archive/zstd"),
    ("application/x-zstd", "archive/zstd"),
    // Inspired by https://github.com/CAPESandbox/sflock/blob/1fe3cf32d01d66c4ad38696c609b13d4f4bc9ea3/sflock/ident.py#L116
    ("application/x-7z-compressed", "archive/7-zip"),
    ("application/x-bzip2", "archive/bzip2"),
    ("application/java-archive", "java/jar"),
    // JAVA Class
    ("application/x-java-applet", "java/class"),
    // EPUB
    ("application/epub+zip", "document/epub"),
    // Packet capture
    ("application/vnd.tcpdump.pcap", "network/tcpdump"),
    ("message/rfc822", "document/email"),
    ("text/calendar", "text/calendar"),
    ("application/x-mach-binary", "executable/mach-o"),
    ("application/x-iso9660-image", "archive/iso"),
    ("application/x-gettext-translation", "resource/mo"),
    ("application/x-hwp", "document/office/hwp"),
    ("application/vnd.iccprofile", "metadata/iccprofile"),
    ("application/vnd.lotus-1-2-3", "document/lotus/spreadsheet"),
    // Firefox modules
    ("application/x-xpinstall", "application/mozilla/extension"),
    // Chrome extensions
    ("application/x-chrome-extension", "application/chrome/extension"),
    // Android
    ("application/vnd.android.package-archive", "android/apk"),
];

// GUID Used to further identify office documents
pub fn ole_clsid_guids() -> &'static HashMap<Uuid, String> {
    static GUIDS: OnceLock<HashMap<Uuid, String>> = OnceLock::new();
    GUIDS.get_or_init(|| {
        let mut table: HashMap<Uuid, String> = Default::default();

        // GUID v0 (0)
        table.insert(uuid!("00020803-0000-0000-C000-000000000046"), "document/office/word".to_string());  // "MS Graph Chart"
        table.insert(uuid!("00020900-0000-0000-C000-000000000046"), "document/office/word".to_string());  // "MS Word95"
        table.insert(uuid!("00020901-0000-0000-C000-000000000046"), "document/office/word".to_string());  // "MS Word 6.0 - 7.0 Picture"
        table.insert(uuid!("00020906-0000-0000-C000-000000000046"), "document/office/word".to_string());  // "MS Word97"
        table.insert(uuid!("00020907-0000-0000-C000-000000000046"), "document/office/word".to_string());  // "MS Word"
        table.insert(uuid!("00020C01-0000-0000-C000-000000000046"), "document/office/excel".to_string());  // "Excel"
        table.insert(uuid!("00020821-0000-0000-C000-000000000046"), "document/office/excel".to_string());  // "Excel"
        table.insert(uuid!("00020820-0000-0000-C000-000000000046"), "document/office/excel".to_string());  // "Excel97"
        table.insert(uuid!("00020810-0000-0000-C000-000000000046"), "document/office/excel".to_string());  // "Excel95"
        table.insert(uuid!("00021a14-0000-0000-C000-000000000046"), "document/office/visio".to_string());  // "Visio"
        table.insert(uuid!("0002CE02-0000-0000-C000-000000000046"), "document/office/equation".to_string());  // "MS Equation 3.0"
        table.insert(uuid!("0003000A-0000-0000-C000-000000000046"), "document/office/paintbrush".to_string());  // "Paintbrush Picture",
        table.insert(uuid!("0003000C-0000-0000-C000-000000000046"), "document/office/package".to_string());  // "Package"
        table.insert(uuid!("000C1084-0000-0000-C000-000000000046"), "document/installer/windows".to_string());  // "Installer Package (MSI)"
        table.insert(uuid!("00020D0B-0000-0000-C000-000000000046"), "document/office/email".to_string());  // "MailMessage"
        // GUID v1 (Timestamp & MAC-48)
        table.insert(uuid!("29130400-2EED-1069-BF5D-00DD011186B7"), "document/office/wordpro".to_string());  // "Lotus WordPro"
        table.insert(uuid!("46E31370-3F7A-11CE-BED6-00AA00611080"), "document/office/word".to_string());  // "MS Forms 2.0 MultiPage"
        table.insert(uuid!("5512D110-5CC6-11CF-8D67-00AA00BDCE1D"), "document/office/word".to_string());  // "MS Forms 2.0 HTML SUBMIT"
        table.insert(uuid!("5512D11A-5CC6-11CF-8D67-00AA00BDCE1D"), "document/office/word".to_string());  // "MS Forms 2.0 HTML TEXT"
        table.insert(uuid!("5512D11C-5CC6-11CF-8D67-00AA00BDCE1D"), "document/office/word".to_string());  // "MS Forms 2.0 HTML Hidden"
        table.insert(uuid!("64818D10-4F9B-11CF-86EA-00AA00B929E8"), "document/office/powerpoint".to_string());  // "MS PowerPoint Presentation"
        table.insert(uuid!("64818D11-4F9B-11CF-86EA-00AA00B929E8"), "document/office/powerpoint".to_string());  // "MS PowerPoint Presentation"
        table.insert(uuid!("11943940-36DE-11CF-953E-00C0A84029E9"), "document/office/word".to_string());  // "MS Photo Editor 3.0 Photo"
        table.insert(uuid!("B801CA65-A1FC-11D0-85AD-444553540000"), "document/pdf".to_string());  // Adobe Acrobat Document"
        table.insert(uuid!("A25250C4-50C1-11D3-8EA3-0090271BECDD"), "document/office/wordperfect".to_string());  // "WordPerfect Office"
        table.insert(uuid!("C62A69F0-16DC-11CE-9E98-00AA00574A4F"), "document/office/word".to_string());  // "Microsoft Forms 2.0 Form"
        table.insert(uuid!("F4754C9B-64F5-4B40-8AF4-679732AC0607"), "document/office/word".to_string());  // Word.Document.12
        table.insert(uuid!("BDD1F04B-858B-11D1-B16A-00C0F0283628"), "document/office/word".to_string());  // Doc (see CVE2012-0158)

        table
    })
}

// # Assemblyline type to file extension mapping
// type_to_extension = {
//     "archive/chm": ".chm",
//     "archive/iso": ".iso",
//     "archive/rar": ".rar",
//     "archive/udf": ".udf",
//     "archive/vhd": ".vhd",
//     "archive/zip": ".zip",
//     "archive/7-zip": ".7z",
//     "audiovisual/flash": ".swf",
//     "code/a3x": ".a3x",
//     "code/batch": ".bat",
//     "code/c": ".c",
//     "code/csharp": ".cs",
//     "code/hta": ".hta",
//     "code/html": ".html",
//     "code/java": ".java",
//     "code/javascript": ".js",
//     "code/jscript": ".js",
//     "code/pdfjs": ".js",
//     "code/perl": ".pl",
//     "code/php": ".php",
//     "code/ps1": ".ps1",
//     "code/python": ".py",
//     "code/ruby": ".rb",
//     "code/shell": ".sh",
//     "code/vbe": ".vbe",
//     "code/vbs": ".vbs",
//     "code/wsf": ".wsf",
//     "document/installer/windows": ".msi",
//     "document/office/excel": ".xls",
//     "document/office/mhtml": ".mhtml",
//     "document/office/ole": ".doc",
//     "document/office/powerpoint": ".ppt",
//     "document/office/rtf": ".doc",
//     "document/office/unknown": ".doc",
//     "document/office/visio": ".vsd",
//     "document/office/word": ".doc",
//     "document/office/wordperfect": "wp",
//     "document/office/wordpro": "lwp",
//     "document/office/onenote": ".one",
//     "document/pdf": ".pdf",
//     "document/email": ".eml",
//     "executable/web/wasm": ".wasm",
//     "executable/windows/pe32": ".exe",
//     "executable/windows/pe64": ".exe",
//     "executable/windows/dll32": ".dll",
//     "executable/windows/dll64": ".dll",
//     "executable/windows/dos": ".exe",
//     "executable/windows/com": ".exe",
//     "executable/linux/elf32": ".elf",
//     "executable/linux/elf64": ".elf",
//     "executable/linux/so32": ".so",
//     "executable/linux/so64": ".so",
//     "java/jar": ".jar",
//     "silverlight/xap": ".xap",
//     "shortcut/windows": ".lnk",
//     "text/windows/registry": ".reg",
// }