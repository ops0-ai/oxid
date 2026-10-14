//! OCI Object Storage remote state (Terraform `backend "oci"`).
//!
//! Authenticates with an OCI API signing key, read from the OCI CLI config
//! file (`~/.oci/config`, or `OCI_CLI_CONFIG_FILE`) and overridable from the
//! backend block, and signs requests with the OCI HTTP signature scheme
//! (RSA-SHA256 over `date`, `(request-target)` and `host`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::{Pkcs1v15Sign, RsaPrivateKey};
use sha2::{Digest, Sha256};

const DEFAULT_PROFILE: &str = "DEFAULT";

/// Backend block attributes relevant to fetching state.
pub struct OciBackend<'a> {
    pub bucket: &'a str,
    pub namespace: &'a str,
    pub key: &'a str,
    pub region: Option<&'a str>,
    pub tenancy_ocid: Option<&'a str>,
    pub user_ocid: Option<&'a str>,
    pub fingerprint: Option<&'a str>,
    pub private_key_path: Option<&'a str>,
    pub config_file_profile: Option<&'a str>,
    pub auth: Option<&'a str>,
}

/// Fully resolved API key credentials.
#[derive(Debug, PartialEq)]
struct Credentials {
    tenancy: String,
    user: String,
    fingerprint: String,
    key_file: PathBuf,
    region: String,
}

/// Fetch the Terraform state object from OCI Object Storage.
pub async fn fetch_state(backend: &OciBackend<'_>) -> Result<String> {
    if let Some(auth) = backend.auth {
        if !auth.eq_ignore_ascii_case("apikey") {
            bail!(
                "OCI backend auth '{}' is not supported yet; only APIKey is.\n\
                 Workaround: terraform state pull > terraform.tfstate && oxid sync --state-file terraform.tfstate",
                auth
            );
        }
    }

    let profile = load_profile(backend.config_file_profile)?;
    let creds = resolve_credentials(backend, &profile)?;
    let key_pem = std::fs::read_to_string(&creds.key_file).context(format!(
        "Failed to read OCI private key {}",
        creds.key_file.display()
    ))?;
    let private_key = parse_private_key(&key_pem)?;

    let host = format!("objectstorage.{}.oraclecloud.com", creds.region);
    let path = object_path(backend.namespace, backend.bucket, backend.key);
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let key_id = format!("{}/{}/{}", creds.tenancy, creds.user, creds.fingerprint);
    let authorization = authorization_header(&private_key, &key_id, "get", &path, &host, &date)?;

    let url = format!("https://{}{}", host, path);
    let resp = reqwest::Client::new()
        .get(&url)
        .header("date", &date)
        .header("authorization", authorization)
        .send()
        .await
        .with_context(|| format!("Failed to fetch state from {}", url))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .context("Failed to read OCI response body")?;
    if !status.is_success() {
        bail!(
            "OCI Object Storage returned {} for oci://{}/{}/{}: {}",
            status,
            backend.namespace,
            backend.bucket,
            backend.key,
            body.trim()
        );
    }

    Ok(body)
}

/// Load a profile from the OCI CLI config file. Keys missing from the
/// profile fall back to the DEFAULT section, as the OCI SDKs do.
fn load_profile(profile: Option<&str>) -> Result<HashMap<String, String>> {
    let path = config_file_path();
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        // The backend block may carry every credential itself
        Err(_) if profile.is_none() => return Ok(HashMap::new()),
        Err(e) => return Err(e).context(format!("Failed to read OCI config {}", path.display())),
    };
    let sections = parse_config(&content);
    let name = profile.unwrap_or(DEFAULT_PROFILE);

    let mut values = sections.get(DEFAULT_PROFILE).cloned().unwrap_or_default();
    match sections.get(name) {
        Some(section) => values.extend(section.clone()),
        None if name != DEFAULT_PROFILE => {
            bail!("Profile '{}' not found in {}", name, path.display())
        }
        None => {}
    }
    Ok(values)
}

fn config_file_path() -> PathBuf {
    if let Ok(p) = std::env::var("OCI_CLI_CONFIG_FILE") {
        return expand_home(&p);
    }
    expand_home("~/.oci/config")
}

fn expand_home(path: &str) -> PathBuf {
    match (path.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => Path::new(&home).join(rest),
        _ => PathBuf::from(path),
    }
}

/// Parse the INI-style OCI config file into section → key → value.
fn parse_config(content: &str) -> HashMap<String, HashMap<String, String>> {
    let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut current: Option<String> = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let name = name.trim().to_string();
            sections.entry(name.clone()).or_default();
            current = Some(name);
        } else if let (Some(section), Some((k, v))) = (&current, line.split_once('=')) {
            sections
                .entry(section.clone())
                .or_default()
                .insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    sections
}

/// Combine backend attributes (highest precedence) with the config profile.
fn resolve_credentials(
    backend: &OciBackend<'_>,
    profile: &HashMap<String, String>,
) -> Result<Credentials> {
    let pick = |attr: Option<&str>, key: &str| -> Result<String> {
        attr.map(str::to_string)
            .or_else(|| profile.get(key).cloned())
            .with_context(|| {
                format!(
                    "OCI backend: '{}' is not set in the backend block or the OCI config profile",
                    key
                )
            })
    };

    Ok(Credentials {
        tenancy: pick(backend.tenancy_ocid, "tenancy")?,
        user: pick(backend.user_ocid, "user")?,
        fingerprint: pick(backend.fingerprint, "fingerprint")?,
        key_file: expand_home(&pick(backend.private_key_path, "key_file")?),
        region: pick(backend.region, "region")?,
    })
}

fn parse_private_key(pem: &str) -> Result<RsaPrivateKey> {
    if pem.contains("ENCRYPTED") {
        bail!("Encrypted OCI private keys are not supported yet; use an unencrypted API key");
    }
    let pem = pem_block(pem);
    RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .context("Failed to parse OCI private key (expected an RSA key in PKCS#8 or PKCS#1 PEM)")
}

/// Extract the PEM block from a key file. Keys downloaded from the OCI
/// console carry a trailing `OCI_API_KEY` marker line after the END line,
/// which strict PEM parsers reject.
fn pem_block(content: &str) -> &str {
    let start = content.find("-----BEGIN").unwrap_or(0);
    let end = content[start..]
        .find("-----END")
        .and_then(|i| {
            let from = start + i + "-----END".len();
            content[from..]
                .find("-----")
                .map(|j| from + j + "-----".len())
        })
        .unwrap_or(content.len());
    &content[start..end]
}

/// Request path for an object. Each segment is percent-encoded, including
/// `/` inside object names (e.g. `Dev/DNS/terraform.tfstate`).
fn object_path(namespace: &str, bucket: &str, key: &str) -> String {
    format!(
        "/n/{}/b/{}/o/{}",
        percent_encode(namespace),
        percent_encode(bucket),
        percent_encode(key)
    )
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn signing_string(method: &str, path: &str, host: &str, date: &str) -> String {
    format!(
        "date: {}\n(request-target): {} {}\nhost: {}",
        date, method, path, host
    )
}

fn authorization_header(
    key: &RsaPrivateKey,
    key_id: &str,
    method: &str,
    path: &str,
    host: &str,
    date: &str,
) -> Result<String> {
    let digest = Sha256::digest(signing_string(method, path, host, date).as_bytes());
    let signature = key
        .sign(Pkcs1v15Sign::new::<Sha256>(), &digest)
        .context("Failed to sign OCI request")?;
    Ok(format!(
        "Signature version=\"1\",keyId=\"{}\",algorithm=\"rsa-sha256\",headers=\"date (request-target) host\",signature=\"{}\"",
        key_id,
        base64::engine::general_purpose::STANDARD.encode(signature)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::RsaPublicKey;

    fn backend<'a>() -> OciBackend<'a> {
        OciBackend {
            bucket: "terraform-iac",
            namespace: "ns",
            key: "Dev/DNS/terraform.tfstate",
            region: None,
            tenancy_ocid: None,
            user_ocid: None,
            fingerprint: None,
            private_key_path: None,
            config_file_profile: None,
            auth: None,
        }
    }

    #[test]
    fn parses_config_sections() {
        let cfg = parse_config(
            "# comment\n[DEFAULT]\nuser=ocid1.user.a\nregion = uk-london-1\n\n[OTHER]\nregion=eu-paris-1\n",
        );
        assert_eq!(cfg["DEFAULT"]["user"], "ocid1.user.a");
        assert_eq!(cfg["DEFAULT"]["region"], "uk-london-1");
        assert_eq!(cfg["OTHER"]["region"], "eu-paris-1");
    }

    #[test]
    fn backend_attributes_override_profile() {
        let profile: HashMap<String, String> = [
            ("tenancy", "ocid1.tenancy.p"),
            ("user", "ocid1.user.p"),
            ("fingerprint", "aa:bb"),
            ("key_file", "/keys/p.pem"),
            ("region", "eu-paris-1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let mut b = backend();
        b.region = Some("uk-london-1");
        b.tenancy_ocid = Some("ocid1.tenancy.b");

        let creds = resolve_credentials(&b, &profile).unwrap();
        assert_eq!(
            creds,
            Credentials {
                tenancy: "ocid1.tenancy.b".into(),
                user: "ocid1.user.p".into(),
                fingerprint: "aa:bb".into(),
                key_file: PathBuf::from("/keys/p.pem"),
                region: "uk-london-1".into(),
            }
        );
    }

    #[test]
    fn missing_credential_is_an_error() {
        let err = resolve_credentials(&backend(), &HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("'tenancy'"), "{}", err);
    }

    #[test]
    fn pem_block_strips_oci_console_marker() {
        let file = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\nOCI_API_KEY";
        assert_eq!(
            pem_block(file),
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"
        );
        let plain = "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n";
        assert_eq!(pem_block(plain), plain.trim_end());
    }

    #[test]
    fn parses_key_with_oci_console_marker() {
        use rsa::pkcs8::EncodePrivateKey;
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let file = format!("{}OCI_API_KEY", pem.as_str());
        assert_eq!(parse_private_key(&file).unwrap(), key);
    }

    #[test]
    fn object_path_encodes_slashes_in_key() {
        assert_eq!(
            object_path("ns", "terraform-iac", "Dev/DNS/terraform.tfstate"),
            "/n/ns/b/terraform-iac/o/Dev%2FDNS%2Fterraform.tfstate"
        );
    }

    #[test]
    fn signature_verifies_with_public_key() {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let date = "Tue, 14 Oct 2026 10:00:00 GMT";
        let path = "/n/ns/b/bucket/o/terraform.tfstate";
        let host = "objectstorage.uk-london-1.oraclecloud.com";

        let header = authorization_header(&key, "t/u/f", "get", path, host, date).unwrap();
        assert!(header.starts_with("Signature version=\"1\",keyId=\"t/u/f\",algorithm=\"rsa-sha256\",headers=\"date (request-target) host\""));

        let sig_b64 = header
            .rsplit("signature=\"")
            .next()
            .unwrap()
            .trim_end_matches('"');
        let sig = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .unwrap();
        let digest = Sha256::digest(signing_string("get", path, host, date).as_bytes());
        RsaPublicKey::from(&key)
            .verify(Pkcs1v15Sign::new::<Sha256>(), &digest, &sig)
            .expect("signature should verify");
    }
}
