use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

use super::cache::ProviderCache;
use super::protocol::ProviderConnection;
use super::registry::RegistryClient;

/// Manages provider lifecycles: discovery, download, startup, and connection pooling.
pub struct ProviderManager {
    cache: ProviderCache,
    registry: RegistryClient,
    /// Running provider connections keyed by "namespace/type".
    /// Uses RwLock: gRPC calls take read lock (concurrent), startup/configure take write lock.
    connections: Arc<RwLock<HashMap<String, ProviderConnection>>>,
    /// Cached schemas keyed by "namespace/type".
    schemas: Arc<Mutex<HashMap<String, serde_json::Value>>>,
}

impl ProviderManager {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache: ProviderCache::new(cache_dir),
            registry: RegistryClient::new(),
            connections: Arc::new(RwLock::new(HashMap::new())),
            schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_registry(cache_dir: PathBuf, registry_url: &str) -> Self {
        Self {
            cache: ProviderCache::new(cache_dir),
            registry: RegistryClient::with_base_url(registry_url),
            connections: Arc::new(RwLock::new(HashMap::new())),
            schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Ensure a provider is available (downloaded + cached).
    /// Returns the path to the provider binary.
    pub async fn ensure_provider(&self, source: &str, version_constraint: &str) -> Result<PathBuf> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        // Check cache first
        if let Some(cached) = self
            .cache
            .find(&namespace, &provider_type, version_constraint)?
        {
            debug!("Provider {} found in cache: {}", key, cached.display());
            return Ok(cached);
        }

        // Resolve version from registry
        info!(
            event = "provider.resolve",
            provider = %format!("{}/{}", namespace, provider_type),
            version_constraint = %version_constraint,
            "Resolving provider version"
        );
        let version = self
            .registry
            .resolve_version(&namespace, &provider_type, version_constraint)
            .await?;

        // Check cache with resolved version
        if let Some(cached) = self
            .cache
            .find_exact(&namespace, &provider_type, &version)?
        {
            debug!("Provider {}@{} found in cache", key, version);
            return Ok(cached);
        }

        // Download from registry
        info!(
            event = "provider.download",
            provider = %format!("{}/{}", namespace, provider_type),
            version = %version,
            "Downloading provider"
        );
        let download_info = self
            .registry
            .get_download_info(&namespace, &provider_type, &version)
            .await?;

        let dest_dir = self.cache.version_dir(&namespace, &provider_type, &version);

        let binary_path = self
            .registry
            .download_provider(&download_info, &dest_dir)
            .await?;

        info!(
            event = "provider.download.complete",
            provider = %format!("{}/{}", namespace, provider_type),
            version = %version,
            path = %binary_path.display(),
            "Provider downloaded"
        );

        Ok(binary_path)
    }

    /// Get or start a provider connection. Reuses existing connections.
    pub async fn get_connection(&self, source: &str, version_constraint: &str) -> Result<()> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        // Check with read lock first (fast path)
        {
            let conns = self.connections.read().await;
            if conns.contains_key(&key) {
                return Ok(());
            }
        }

        let binary_path = self.ensure_provider(source, version_constraint).await?;

        let conn = ProviderConnection::start(&binary_path)
            .await
            .context(format!("Failed to start provider {}", key))?;

        let mut conns = self.connections.write().await;
        conns.insert(key, conn);
        Ok(())
    }

    /// Get the schema for a provider. Starts the provider if not running.
    pub async fn get_schema(
        &self,
        source: &str,
        version_constraint: &str,
    ) -> Result<serde_json::Value> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        // Check schema cache
        {
            let schemas = self.schemas.lock().await;
            if let Some(schema) = schemas.get(&key) {
                return Ok(schema.clone());
            }
        }

        // Ensure connection exists
        self.get_connection(source, version_constraint).await?;

        // Get schema from provider (returns JSON directly) — needs write lock for caching
        let mut conns = self.connections.write().await;
        let conn = conns
            .get_mut(&key)
            .context(format!("Provider {} not connected", key))?;

        let schema_json = conn.get_schema().await?;

        // Cache it
        {
            let mut schemas = self.schemas.lock().await;
            schemas.insert(key, schema_json.clone());
        }

        Ok(schema_json)
    }

    /// Execute a plan for a single resource.
    /// Uses read lock — multiple plans can run concurrently.
    pub async fn plan_resource(
        &self,
        source: &str,
        type_name: &str,
        prior_state: Option<&serde_json::Value>,
        proposed_new_state: Option<&serde_json::Value>,
        config: &serde_json::Value,
    ) -> Result<super::protocol::PlanResult> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let conns = self.connections.read().await;
        let conn = conns.get(&key).context(format!(
            "Provider {} not connected. Call get_connection first.",
            key
        ))?;

        conn.plan_resource_change(type_name, prior_state, proposed_new_state, config)
            .await
    }

    /// Execute an apply for a single resource.
    /// Uses read lock — multiple applies can run concurrently.
    pub async fn apply_resource(
        &self,
        source: &str,
        type_name: &str,
        prior_state: Option<&serde_json::Value>,
        planned_state: Option<&serde_json::Value>,
        config: &serde_json::Value,
        planned_private: &[u8],
    ) -> Result<super::protocol::ApplyResult> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let conns = self.connections.read().await;
        let conn = conns
            .get(&key)
            .context(format!("Provider {} not connected", key))?;

        conn.apply_resource_change(
            type_name,
            prior_state,
            planned_state,
            config,
            planned_private,
        )
        .await
    }

    /// Read a resource's current state from the provider.
    pub async fn read_resource(
        &self,
        source: &str,
        type_name: &str,
        current_state: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let conns = self.connections.read().await;
        let conn = conns
            .get(&key)
            .context(format!("Provider {} not connected", key))?;

        conn.read_resource(type_name, current_state).await
    }

    /// Read a data source.
    pub async fn read_data_source(
        &self,
        source: &str,
        type_name: &str,
        config: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let conns = self.connections.read().await;
        let conn = conns
            .get(&key)
            .context(format!("Provider {} not connected", key))?;

        conn.read_data_source(type_name, config).await
    }

    /// Get the schema for a specific resource type.
    pub async fn get_resource_schema(
        &self,
        source: &str,
        type_name: &str,
    ) -> Result<Option<serde_json::Value>> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let conns = self.connections.read().await;
        let conn = conns
            .get(&key)
            .context(format!("Provider {} not connected", key))?;

        Ok(conn.get_resource_schema(type_name))
    }

    /// Get the schema for a specific data source type.
    pub async fn get_data_source_schema(
        &self,
        source: &str,
        type_name: &str,
    ) -> Result<Option<serde_json::Value>> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let conns = self.connections.read().await;
        let conn = conns
            .get(&key)
            .context(format!("Provider {} not connected", key))?;

        Ok(conn.get_data_source_schema(type_name))
    }

    /// Configure a running provider. Needs write lock (mutates connection state).
    pub async fn configure_provider(&self, source: &str, config: &serde_json::Value) -> Result<()> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let mut conns = self.connections.write().await;
        let conn = conns
            .get_mut(&key)
            .context(format!("Provider {} not connected", key))?;

        conn.configure("oxid", config).await
    }

    /// Stop all running providers.
    pub async fn stop_all(&self) -> Result<()> {
        let mut conns = self.connections.write().await;
        for (key, mut conn) in conns.drain() {
            info!("Stopping provider {}", key);
            if let Err(e) = conn.stop().await {
                tracing::error!("Failed to stop provider {}: {}", key, e);
            }
        }
        Ok(())
    }

    /// Stop a specific provider.
    pub async fn stop_provider(&self, source: &str) -> Result<()> {
        let (namespace, provider_type) = RegistryClient::parse_source(source)?;
        let key = format!("{}/{}", namespace, provider_type);

        let mut conns = self.connections.write().await;
        if let Some(mut conn) = conns.remove(&key) {
            conn.stop().await?;
        }
        Ok(())
    }

    /// List currently running providers.
    pub async fn list_running(&self) -> Vec<String> {
        let conns = self.connections.read().await;
        conns.keys().cloned().collect()
    }
}

impl Drop for ProviderManager {
    fn drop(&mut self) {
        // Best-effort cleanup — child processes are killed on drop anyway
        // due to `kill_on_drop(true)` in ProviderConnection.
    }
}
