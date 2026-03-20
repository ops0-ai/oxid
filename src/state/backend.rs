use anyhow::Result;
use async_trait::async_trait;

use super::models::{
    ImportResult, Lock, LockInfo, OutputValue, ResourceFilter, ResourceResult, ResourceState,
    RunRecord, SyncResult, Workspace,
};

/// Pluggable state backend trait.
/// Implemented by SQLite (local dev) and PostgreSQL (teams/production).
#[async_trait]
pub trait StateBackend: Send + Sync {
    // ─── Initialization ─────────────────────────────────────────────────────

    /// Initialize the backend (create tables, run migrations).
    async fn initialize(&self) -> Result<()>;

    // ─── Workspace Operations ───────────────────────────────────────────────

    /// Create a new workspace. Returns the workspace ID.
    async fn create_workspace(&self, name: &str) -> Result<String>;

    /// Get a workspace by name.
    async fn get_workspace(&self, name: &str) -> Result<Option<Workspace>>;

    /// List all workspaces.
    async fn list_workspaces(&self) -> Result<Vec<Workspace>>;

    /// Delete a workspace and all its resources.
    async fn delete_workspace(&self, name: &str) -> Result<()>;

    // ─── Resource CRUD ──────────────────────────────────────────────────────

    /// Get a resource by workspace and address.
    async fn get_resource(
        &self,
        workspace_id: &str,
        address: &str,
    ) -> Result<Option<ResourceState>>;

    /// Insert or update a resource.
    async fn upsert_resource(&self, resource: &ResourceState) -> Result<()>;

    /// Delete a resource from state.
    async fn delete_resource(&self, workspace_id: &str, address: &str) -> Result<()>;

    /// List resources with optional filtering.
    async fn list_resources(
        &self,
        workspace_id: &str,
        filter: &ResourceFilter,
    ) -> Result<Vec<ResourceState>>;

    /// Count resources in a workspace.
    async fn count_resources(&self, workspace_id: &str) -> Result<usize>;

    // ─── Dependencies ───────────────────────────────────────────────────────

    /// Set dependencies for a resource (replaces existing).
    async fn set_dependencies(
        &self,
        resource_id: &str,
        depends_on: &[(String, String)], // (depends_on_id, dep_type)
    ) -> Result<()>;

    /// Get all resources that the given resource depends on.
    async fn get_dependencies(&self, resource_id: &str) -> Result<Vec<String>>;

    /// Get all resources that depend on the given resource.
    async fn get_dependents(&self, resource_id: &str) -> Result<Vec<String>>;

    // ─── Locking ────────────────────────────────────────────────────────────

    /// Acquire a lock on a resource address. Returns the lock on success.
    async fn acquire_lock(
        &self,
        address: &str,
        workspace_id: &str,
        info: &LockInfo,
    ) -> Result<Lock>;

    /// Release a lock by lock ID.
    async fn release_lock(&self, lock_id: &str) -> Result<()>;

    /// Force-unlock a resource address (admin operation).
    async fn force_unlock(&self, address: &str, workspace_id: &str) -> Result<()>;

    /// Check if a resource is locked.
    async fn is_locked(&self, address: &str, workspace_id: &str) -> Result<Option<Lock>>;

    // ─── Outputs ────────────────────────────────────────────────────────────

    /// Set an output value.
    async fn set_output(
        &self,
        workspace_id: &str,
        module_path: &str,
        name: &str,
        value: &str,
        sensitive: bool,
    ) -> Result<()>;

    /// Get an output value.
    async fn get_output(
        &self,
        workspace_id: &str,
        module_path: &str,
        name: &str,
    ) -> Result<Option<OutputValue>>;

    /// List all outputs for a workspace (optionally filtered by module path).
    async fn list_outputs(
        &self,
        workspace_id: &str,
        module_path: Option<&str>,
    ) -> Result<Vec<OutputValue>>;

    /// Clear all outputs for a module path.
    async fn clear_outputs(&self, workspace_id: &str, module_path: &str) -> Result<()>;

    // ─── Runs ───────────────────────────────────────────────────────────────

    /// Start a new execution run.
    async fn start_run(
        &self,
        workspace_id: &str,
        operation: &str,
        resources_planned: i32,
    ) -> Result<String>;

    /// Complete an execution run.
    async fn complete_run(
        &self,
        run_id: &str,
        status: &str,
        resources_succeeded: i32,
        resources_failed: i32,
    ) -> Result<()>;

    /// Record a per-resource result within a run.
    async fn record_resource_result(&self, run_id: &str, result: &ResourceResult) -> Result<()>;

    /// Get the latest run for a workspace.
    async fn get_latest_run(&self, workspace_id: &str) -> Result<Option<RunRecord>>;

    /// List recent runs for a workspace.
    async fn list_runs(&self, workspace_id: &str, limit: usize) -> Result<Vec<RunRecord>>;

    // ─── Query ──────────────────────────────────────────────────────────────

    /// Execute a raw SQL query against the state database.
    /// Returns rows as JSON values.
    async fn query_raw(&self, sql: &str) -> Result<Vec<serde_json::Value>>;

    // ─── Import ─────────────────────────────────────────────────────────────

    /// Import resources from a terraform .tfstate JSON string.
    async fn import_tfstate(&self, workspace_id: &str, state_json: &str) -> Result<ImportResult>;

    /// Sync resources from a terraform .tfstate JSON string.
    /// Unlike import, this upserts (overwrites existing) and removes resources
    /// that are no longer present in the state file.
    async fn sync_tfstate(&self, workspace_id: &str, state_json: &str) -> Result<SyncResult>;

    // ─── Providers ──────────────────────────────────────────────────────────

    /// Register a provider used in this workspace.
    async fn register_provider(
        &self,
        workspace_id: &str,
        source: &str,
        version: &str,
    ) -> Result<String>;

    /// List providers for a workspace.
    async fn list_providers(&self, workspace_id: &str) -> Result<Vec<(String, String, String)>>; // (id, source, version)
}
