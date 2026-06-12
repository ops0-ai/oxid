use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// Information about a provider resolved from the registry.
#[derive(Debug, Clone)]
pub struct ProviderSource {
    pub namespace: String,
    pub provider_type: String,
    pub version: String,
    pub os: String,
    pub arch: String,
    pub download_url: String,
    pub shasum: String,
    pub filename: String,
    pub protocols: Vec<String>,
}

/// Response from the registry versions API.
#[derive(Debug, Deserialize)]
struct VersionsResponse {
    versions: Vec<VersionEntry>,
}

#[derive(Debug, Deserialize)]
struct VersionEntry {
    version: String,
    protocols: Vec<String>,
    #[allow(dead_code)]
    platforms: Vec<PlatformEntry>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct PlatformEntry {
    os: String,
    arch: String,
}

/// Response from the registry download API.
#[derive(Debug, Deserialize)]
struct DownloadResponse {
    os: String,
    arch: String,
    filename: String,
    download_url: String,
    shasum: String,
    protocols: Vec<String>,
}

/// The OpenTofu/Terraform provider registry client.
/// Discovers and downloads provider binaries from registry.opentofu.org
/// or registry.terraform.io.
pub struct RegistryClient {
    http: reqwest::Client,
    base_url: String,
}

impl Default for RegistryClient {
    fn default() -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: "https://registry.terraform.io".to_string(),
        }
    }
}

impl RegistryClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_base_url(base_url: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Parse a provider source string like "hashicorp/aws" or "registry.terraform.io/hashicorp/aws".
    pub fn parse_source(source: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = source.split('/').collect();
        match parts.len() {
            2 => Ok((parts[0].to_string(), parts[1].to_string())),
            3 => Ok((parts[1].to_string(), parts[2].to_string())),
            _ => bail!("Invalid provider source '{}'. Expected format: namespace/type or hostname/namespace/type", source),
        }
    }

    /// List available versions for a provider.
    pub async fn list_versions(
        &self,
        namespace: &str,
        provider_type: &str,
    ) -> Result<Vec<(String, Vec<String>)>> {
        let url = format!(
            "{}/v1/providers/{}/{}/versions",
            self.base_url, namespace, provider_type
        );

        let response = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to query provider registry")?;

        let status = response.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND {
                bail!(
                    "Provider '{}/{}' not found in registry. Did you mean a non-hashicorp provider?\n  - Check the source in required_providers (e.g. source = \"gavinbunney/kubectl\" or source = \"cloudflare/cloudflare\")\n  - Verify the namespace at https://registry.terraform.io",
                    namespace, provider_type
                );
            }
            bail!(
                "Registry returned HTTP {} for provider '{}/{}'",
                status,
                namespace,
                provider_type
            );
        }

        let resp: VersionsResponse = response
            .json()
            .await
            .context("Failed to parse registry response")?;

        Ok(resp
            .versions
            .into_iter()
            .map(|v| (v.version, v.protocols))
            .collect())
    }

    /// Resolve the best version matching a constraint.
    /// Supports: exact ("1.2.3"), prefix ("~> 1.2"), >= (">= 1.0").
    pub async fn resolve_version(
        &self,
        namespace: &str,
        provider_type: &str,
        constraint: &str,
    ) -> Result<String> {
        let versions = self.list_versions(namespace, provider_type).await?;

        if versions.is_empty() {
            bail!(
                "No versions found for provider {}/{}",
                namespace,
                provider_type
            );
        }

        let constraint = constraint.trim();

        // Exact version
        if !constraint.starts_with('~')
            && !constraint.starts_with('>')
            && !constraint.starts_with('<')
            && !constraint.starts_with('=')
        {
            if versions.iter().any(|(v, _)| v == constraint) {
                return Ok(constraint.to_string());
            }
            bail!(
                "Version {} not found for {}/{}",
                constraint,
                namespace,
                provider_type
            );
        }

        // ~> pessimistic constraint: ~> 1.2 means >= 1.2.0, < 2.0.0
        if constraint.starts_with("~>") {
            let version_part = constraint.trim_start_matches("~>").trim();
            let parts: Vec<u64> = version_part
                .split('.')
                .filter_map(|p| p.parse().ok())
                .collect();

            let mut matching: Vec<&str> = versions
                .iter()
                .filter(|(v, _)| {
                    let v_parts: Vec<u64> = v.split('.').filter_map(|p| p.parse().ok()).collect();
                    if v_parts.len() < parts.len() {
                        return false;
                    }
                    // Must match all but last constraint part, and last must be >=
                    for (i, part) in parts[..parts.len().saturating_sub(1)].iter().enumerate() {
                        if v_parts.get(i) != Some(part) {
                            return false;
                        }
                    }
                    let last_idx = parts.len() - 1;
                    v_parts
                        .get(last_idx)
                        .map(|v| *v >= parts[last_idx])
                        .unwrap_or(false)
                })
                .map(|(v, _)| v.as_str())
                .collect();

            matching.sort_by(|a, b| compare_versions(b, a));

            return matching
                .first()
                .map(|v| v.to_string())
                .ok_or_else(|| anyhow::anyhow!("No version matches constraint '{}'", constraint));
        }

        // >= constraint
        if constraint.starts_with(">=") {
            let version_part = constraint.trim_start_matches(">=").trim();
            let min_parts: Vec<u64> = version_part
                .split('.')
                .filter_map(|p| p.parse().ok())
                .collect();

            let mut matching: Vec<&str> = versions
                .iter()
                .filter(|(v, _)| {
                    let v_parts: Vec<u64> = v.split('.').filter_map(|p| p.parse().ok()).collect();
                    compare_version_tuples(&v_parts, &min_parts) != std::cmp::Ordering::Less
                })
                .map(|(v, _)| v.as_str())
                .collect();

            matching.sort_by(|a, b| compare_versions(b, a));

            return matching
                .first()
                .map(|v| v.to_string())
                .ok_or_else(|| anyhow::anyhow!("No version matches constraint '{}'", constraint));
        }

        // Fallback: latest version
        versions
            .first()
            .map(|(v, _)| v.clone())
            .ok_or_else(|| anyhow::anyhow!("No versions available"))
    }

    /// Get the download URL and metadata for a specific provider version.
    pub async fn get_download_info(
        &self,
        namespace: &str,
        provider_type: &str,
        version: &str,
    ) -> Result<ProviderSource> {
        let (os, arch) = detect_platform();

        let url = format!(
            "{}/v1/providers/{}/{}/{}/download/{}/{}",
            self.base_url, namespace, provider_type, version, os, arch
        );

        let response = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to query download URL")?;

        let status = response.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND {
                bail!(
                    "No download available for '{}/{}@{}' on {}/{}",
                    namespace,
                    provider_type,
                    version,
                    os,
                    arch
                );
            }
            bail!(
                "Registry returned HTTP {} for download of '{}/{}@{}'",
                status,
                namespace,
                provider_type,
                version
            );
        }

        let resp: DownloadResponse = response
            .json()
            .await
            .context("Failed to parse download response")?;

        Ok(ProviderSource {
            namespace: namespace.to_string(),
            provider_type: provider_type.to_string(),
            version: version.to_string(),
            os: resp.os,
            arch: resp.arch,
            download_url: resp.download_url,
            shasum: resp.shasum,
            filename: resp.filename,
            protocols: resp.protocols,
        })
    }

    /// Download a provider binary to the specified directory.
    /// Returns the path to the extracted provider binary.
    pub async fn download_provider(
        &self,
        source: &ProviderSource,
        dest_dir: &Path,
    ) -> Result<PathBuf> {
        std::fs::create_dir_all(dest_dir)?;

        let archive_path = dest_dir.join(&source.filename);

        // Download the archive
        let resp = self
            .http
            .get(&source.download_url)
            .send()
            .await
            .context("Failed to download provider archive")?;

        let bytes = resp.bytes().await?;
        std::fs::write(&archive_path, &bytes)?;

        // Extract the archive (zip format for terraform providers)
        let binary_path = extract_provider_archive(&archive_path, dest_dir)?;

        // Clean up the archive
        let _ = std::fs::remove_file(&archive_path);

        Ok(binary_path)
    }
}

/// Extract a provider binary from a zip archive.
fn extract_provider_archive(archive_path: &Path, dest_dir: &Path) -> Result<PathBuf> {
    let file = std::fs::File::open(archive_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    let mut binary_path = None;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();

        // Provider binaries are named terraform-provider-<type>_v<version>
        if name.starts_with("terraform-provider-") {
            let out_path = dest_dir.join(&name);
            let mut outfile = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut outfile)?;

            // Make executable on Unix
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(0o755))?;
            }

            binary_path = Some(out_path);
        }
    }

    binary_path.ok_or_else(|| anyhow::anyhow!("No provider binary found in archive"))
}

/// Detect the current OS and architecture for registry downloads.
fn detect_platform() -> (String, String) {
    let os = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    };

    let arch = if cfg!(target_arch = "x86_64") {
        "amd64"
    } else if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "amd64"
    };

    (os.to_string(), arch.to_string())
}

fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    let a_parts: Vec<u64> = a.split('.').filter_map(|p| p.parse().ok()).collect();
    let b_parts: Vec<u64> = b.split('.').filter_map(|p| p.parse().ok()).collect();
    compare_version_tuples(&a_parts, &b_parts)
}

fn compare_version_tuples(a: &[u64], b: &[u64]) -> std::cmp::Ordering {
    let max_len = a.len().max(b.len());
    for i in 0..max_len {
        let av = a.get(i).copied().unwrap_or(0);
        let bv = b.get(i).copied().unwrap_or(0);
        match av.cmp(&bv) {
            std::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }
    std::cmp::Ordering::Equal
}
