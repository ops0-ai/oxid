use anyhow::{bail, Context, Result};

use crate::config::types::BackendConfig;

/// Fetch Terraform state JSON from a remote backend.
pub async fn fetch_remote_state(backend: &BackendConfig) -> Result<String> {
    match backend {
        BackendConfig::S3 {
            bucket,
            key,
            region,
            endpoint,
            profile,
            ..
        } => {
            fetch_s3_state(
                bucket,
                key,
                region.as_deref(),
                endpoint.as_deref(),
                profile.as_deref(),
            )
            .await
        }
        BackendConfig::Unsupported { backend_type } => {
            bail!(
                "Backend type '{}' is not yet supported for remote state import.\n\
                 Supported backends: s3\n\
                 Workaround: manually download the state file and use 'oxid import tfstate <path>'",
                backend_type
            );
        }
    }
}

#[cfg(feature = "s3-backend")]
async fn fetch_s3_state(
    bucket: &str,
    key: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    profile: Option<&str>,
) -> Result<String> {
    let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(r) = region {
        config_loader = config_loader.region(aws_config::Region::new(r.to_string()));
    }
    if let Some(p) = profile {
        config_loader = config_loader.profile_name(p);
    }

    let aws_config = config_loader.load().await;

    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(ep) = endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(ep);
    }

    let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("Failed to fetch state from s3://{}/{}", bucket, key))?;

    let body = resp
        .body
        .collect()
        .await
        .context("Failed to read S3 response body")?;

    let state_json = String::from_utf8(body.into_bytes().to_vec())
        .context("S3 state file contains invalid UTF-8")?;

    Ok(state_json)
}

#[cfg(not(feature = "s3-backend"))]
async fn fetch_s3_state(
    _bucket: &str,
    _key: &str,
    _region: Option<&str>,
    _endpoint: Option<&str>,
    _profile: Option<&str>,
) -> Result<String> {
    bail!(
        "S3 backend support is not compiled in.\n\
         Rebuild with: cargo build --features s3-backend\n\
         Or download the state file manually and use: oxid import tfstate <path>"
    );
}
