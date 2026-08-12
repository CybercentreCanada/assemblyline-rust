// Workarounds for Azure SDK for Rust issues
//
// Support for StorageSharedKeyCredential is not yet implemented in the Azure SDK for Rust (1.0.0)
// This is a workaround using a per-try Policy that signs requests with the
// Azure Storage shared-key v2 scheme (HMAC-SHA256).
// Pending resolution of: https://github.com/Azure/azure-sdk-for-rust/issues/2975
// Implementation is based on work by @bentheiii mentioned in the above issue

use std::borrow::Cow;
use std::sync::Arc;

use azure_core::credentials::Secret;
use azure_core::http::headers::{HeaderName, Headers, CONTENT_LENGTH};
use azure_core::http::policies::{Policy, PolicyResult};
use azure_core::http::Method;
use azure_core::http::{Context, Request};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use url::Url;

// Static credentials for Azurite testing, reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-emulator#authenticating-requests-against-the-storage-emulator
pub const EMULATOR_ACCOUNT_NAME: &str = "devstoreaccount1";
pub const EMULATOR_ACCOUNT_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

#[derive(Debug)]
pub struct SharedKeyAuthorizationPolicy {
    account: String,
    key: Secret,
}

impl SharedKeyAuthorizationPolicy {
    pub fn new(account: String, key: String) -> Arc<Self> {
        Arc::new(Self {
            account,
            key: Secret::new(key),
        })
    }

    pub fn emulator() -> Arc<Self> {
        Self::new(
            EMULATOR_ACCOUNT_NAME.to_string(),
            EMULATOR_ACCOUNT_KEY.to_string(),
        )
    }
}

#[async_trait::async_trait]
impl Policy for SharedKeyAuthorizationPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        // Add x-ms-date if not already present (required for shared key auth)
        if request.headers().get_optional_str(&HeaderName::from_static("x-ms-date")).is_none() {
            let date_str = httpdate::fmt_http_date(std::time::SystemTime::now());
            request.insert_header("x-ms-date", date_str);
        }

        // Ensure content-length is set before signing, as the transport layer
        // may add it later which would cause a signature mismatch.
        if request.headers().get_optional_str(&CONTENT_LENGTH).is_none() {
            if let Some(body_len) = request.body().len() {
                request.insert_header(CONTENT_LENGTH, body_len.to_string());
            }
        }

        let auth = generate_authorization(
            request.headers(),
            request.url(),
            &request.method(),
            &self.account,
            &self.key,
        );
        request.insert_header("authorization", auth);
        next[0].send(ctx, request, &next[1..]).await
    }
}

fn generate_authorization(
    headers: &Headers,
    url: &Url,
    method: &Method,
    account: &str,
    key: &Secret,
) -> String {
    let sig = string_to_sign(account, headers, url, method);
    let auth = hmac_sha256(&sig, key);
    format!("SharedKey {account}:{auth}")
}

fn hmac_sha256(data: &str, key: &Secret) -> String {
    let decoded_key = BASE64_STANDARD.decode(key.secret()).expect("valid base64 key");
    let mut mac = Hmac::<Sha256>::new_from_slice(&decoded_key).expect("valid HMAC key length");
    mac.update(data.as_bytes());
    BASE64_STANDARD.encode(mac.finalize().into_bytes())
}

fn string_to_sign(account: &str, h: &Headers, u: &Url, method: &Method) -> String {
    let content_length = h
        .get_optional_str(&CONTENT_LENGTH)
        .filter(|&v| v != "0")
        .unwrap_or_default();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_ref(),
        header_or_empty(h, "content-encoding"),
        header_or_empty(h, "content-language"),
        content_length,
        header_or_empty(h, "content-md5"),
        header_or_empty(h, "content-type"),
        header_or_empty(h, "date"),
        header_or_empty(h, "if-modified-since"),
        header_or_empty(h, "if-match"),
        header_or_empty(h, "if-none-match"),
        header_or_empty(h, "if-unmodified-since"),
        header_or_empty(h, "range"),
        canonicalize_headers(h),
        canonicalized_resource(account, u),
    )
}

#[inline]
fn header_or_empty<'a>(h: &'a Headers, name: &'static str) -> &'a str {
    h.get_optional_str(&HeaderName::from_static(name)).unwrap_or("")
}

fn canonicalize_headers(headers: &Headers) -> String {
    let mut names: Vec<_> = headers
        .iter()
        .filter_map(|(k, _)| k.as_str().starts_with("x-ms").then_some(k))
        .collect();
    names.sort_unstable();

    let mut result = String::new();
    for name in names {
        let value = headers.get_optional_str(name).unwrap_or("");
        result.push_str(name.as_str());
        result.push(':');
        result.push_str(value);
        result.push('\n');
    }
    result
}

fn canonicalized_resource(account: &str, uri: &Url) -> String {
    let mut can_res = String::new();
    can_res.push('/');
    can_res.push_str(account);

    for segment in uri.path_segments().into_iter().flatten() {
        can_res.push('/');
        can_res.push_str(segment);
    }
    can_res.push('\n');

    // Collect unique query parameter names
    let query_pairs: Vec<(Cow<str>, Cow<str>)> = uri.query_pairs().collect();
    let mut param_names: Vec<String> = Vec::new();
    for (name, _) in &query_pairs {
        if !param_names.iter().any(|n| n == name.as_ref()) {
            param_names.push(name.to_string());
        }
    }
    param_names.sort();

    for param in &param_names {
        let mut values: Vec<&str> = query_pairs
            .iter()
            .filter(|(k, _)| k.as_ref() == param)
            .map(|(_, v)| v.as_ref())
            .collect();
        values.sort_unstable();

        can_res.push_str(&param.to_lowercase());
        can_res.push(':');
        can_res.push_str(&values.join(","));
        can_res.push('\n');
    }

    // Remove trailing newline
    if can_res.ends_with('\n') {
        can_res.pop();
    }
    can_res
}