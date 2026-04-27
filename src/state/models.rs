use serde::{Deserialize, Serialize};

// ─── Resource-Level State ───────────────────────────────────────────────────

/// A resource's state as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceState {
    pub id: String,
    pub workspace_id: String,
    pub module_path: String,
    pub resource_type: String,
    pub resource_name: String,
    pub resource_mode: String,
    pub provider_source: String,
    pub index_key: Option<String>,
    pub address: String,
    pub status: String,
    pub attributes_json: String,
    pub sensitive_attrs: Vec<String>,
    pub schema_version: i32,
    pub created_at: String,
    pub updated_at: String,
}

impl ResourceState {
    /// Create a new resource state with default values.
    pub fn new(
        workspace_id: &str,
        resource_type: &str,
        resource_name: &str,
        address: &str,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: workspace_id.to_string(),
            module_path: String::new(),
            resource_type: resource_type.to_string(),
            resource_name: resource_name.to_string(),
            resource_mode: "managed".to_string(),
            provider_source: String::new(),
            index_key: None,
            address: address.to_string(),
            status: "planned".to_string(),
            attributes_json: "{}".to_string(),
            sensitive_attrs: vec![],
            schema_version: 0,
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

/// Resource status values.
pub mod status {
    pub const PLANNED: &str = "planned";
    pub const CREATING: &str = "creating";
    pub const CREATED: &str = "created";
    pub const UPDATING: &str = "updating";
    pub const DELETING: &str = "deleting";
    pub const DELETED: &str = "deleted";
    pub const TAINTED: &str = "tainted";
    pub const FAILED: &str = "failed";
}

// ─── Workspace ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub created_at: String,
    pub updated_at: String,
}

// ─── Dependencies ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDependency {
    pub resource_id: String,
    pub depends_on_id: String,
    pub dependency_type: String,
}

pub mod dep_type {
    pub const EXPLICIT: &str = "explicit";
    pub const IMPLICIT: &str = "implicit";
}

// ─── Outputs ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputValue {
    pub id: String,
    pub workspace_id: String,
    pub module_path: String,
    pub output_name: String,
    pub output_value: String,
    pub sensitive: bool,
}

// ─── Locking ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lock {
    pub resource_address: String,
    pub workspace_id: String,
    pub locked_at: String,
    pub locked_by: String,
    pub lock_id: String,
    pub operation: String,
    pub expires_at: Option<String>,
    pub info: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LockInfo {
    pub locked_by: String,
    pub operation: String,
    pub info: Option<String>,
    pub ttl_secs: Option<u64>,
}

// ─── Runs ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub workspace_id: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub status: String,
    pub operation: String,
    pub resources_planned: i32,
    pub resources_succeeded: i32,
    pub resources_failed: i32,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceResult {
    pub address: String,
    pub action: String,
    pub status: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error_message: Option<String>,
    pub diff_json: Option<String>,
}

pub mod action {
    pub const CREATE: &str = "create";
    pub const UPDATE: &str = "update";
    pub const DELETE: &str = "delete";
    pub const READ: &str = "read";
    pub const NOOP: &str = "no-op";
    pub const IMPORT: &str = "import";
}

pub mod run_status {
    pub const RUNNING: &str = "running";
    pub const SUCCEEDED: &str = "succeeded";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";
}

// ─── Query Results ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct ResourceFilter {
    pub resource_type: Option<String>,
    pub module_path: Option<String>,
    pub status: Option<String>,
    pub address_pattern: Option<String>,
}

// ─── Import ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ImportResult {
    pub imported: usize,
    pub skipped: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SyncResult {
    pub updated: usize,
    pub added: usize,
    pub removed: usize,
}

/// A snapshot of a resource change for audit history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceHistory {
    pub id: String,
    pub workspace_id: String,
    pub address: String,
    pub action: String,
    pub attributes_json: Option<String>,
    pub run_id: Option<String>,
    pub captured_at: String,
}

// ─── Legacy types (kept for backward compatibility) ─────────────────────────

#[derive(Debug, Clone)]
pub struct ModuleState {
    pub name: String,
    pub source: String,
    pub version: Option<String>,
    pub status: String,
    pub last_plan_at: Option<String>,
    pub last_apply_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ModuleOutput {
    pub module_name: String,
    pub output_key: String,
    pub output_value: String,
}
