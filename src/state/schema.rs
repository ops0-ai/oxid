/// SQL DDL for the oxid state database.
/// This schema supports resource-level state, fine-grained locking,
/// dependency tracking, and SQL queryability.
///
/// Compatible with both SQLite and PostgreSQL (using TEXT for timestamps
/// and TEXT for JSON instead of JSONB to keep dialect-agnostic).
pub const SCHEMA_VERSION: i32 = 1;

pub const CREATE_TABLES_SQL: &str = "
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
);

-- Workspaces
CREATE TABLE IF NOT EXISTS workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Providers used in workspaces
CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    source TEXT NOT NULL,
    version TEXT NOT NULL,
    config_hash TEXT,
    UNIQUE(workspace_id, source),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Resources: the core table for SQL-queryable infrastructure state
CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    module_path TEXT NOT NULL DEFAULT '',
    resource_type TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    resource_mode TEXT NOT NULL DEFAULT 'managed',
    provider_source TEXT NOT NULL DEFAULT '',
    index_key TEXT,
    address TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned',
    attributes_json TEXT NOT NULL DEFAULT '{}',
    sensitive_attrs TEXT NOT NULL DEFAULT '[]',
    schema_version INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(workspace_id, address),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Resource dependencies (DAG edges)
CREATE TABLE IF NOT EXISTS resource_dependencies (
    resource_id TEXT NOT NULL,
    depends_on_id TEXT NOT NULL,
    dependency_type TEXT NOT NULL DEFAULT 'explicit',
    PRIMARY KEY (resource_id, depends_on_id),
    FOREIGN KEY (resource_id) REFERENCES resources(id) ON DELETE CASCADE,
    FOREIGN KEY (depends_on_id) REFERENCES resources(id) ON DELETE CASCADE
);

-- Resource outputs
CREATE TABLE IF NOT EXISTS resource_outputs (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    module_path TEXT NOT NULL DEFAULT '',
    output_name TEXT NOT NULL,
    output_value TEXT NOT NULL,
    sensitive INTEGER NOT NULL DEFAULT 0,
    UNIQUE(workspace_id, module_path, output_name),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Fine-grained resource locks
CREATE TABLE IF NOT EXISTS resource_locks (
    resource_address TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    locked_at TEXT NOT NULL,
    locked_by TEXT NOT NULL,
    lock_id TEXT NOT NULL UNIQUE,
    operation TEXT NOT NULL,
    expires_at TEXT,
    info TEXT,
    PRIMARY KEY (resource_address, workspace_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Execution runs
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    operation TEXT NOT NULL,
    resources_planned INTEGER DEFAULT 0,
    resources_succeeded INTEGER DEFAULT 0,
    resources_failed INTEGER DEFAULT 0,
    error_message TEXT,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Per-resource results within a run
CREATE TABLE IF NOT EXISTS run_resources (
    run_id TEXT NOT NULL,
    resource_address TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT,
    completed_at TEXT,
    error_message TEXT,
    diff_json TEXT,
    PRIMARY KEY (run_id, resource_address),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

-- Resource change history (audit log of every resource change over time)
CREATE TABLE IF NOT EXISTS resource_history (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    address TEXT NOT NULL,
    action TEXT NOT NULL,
    attributes_json TEXT,
    run_id TEXT,
    captured_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);

-- Module-level state (retained for YAML module orchestration compatibility)
CREATE TABLE IF NOT EXISTS modules (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    version TEXT,
    config_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    last_plan_at TEXT,
    last_apply_at TEXT,
    UNIQUE(workspace_id, name),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
";

pub const CREATE_INDEXES_SQL: &str = "
CREATE INDEX IF NOT EXISTS idx_resources_type ON resources(resource_type);
CREATE INDEX IF NOT EXISTS idx_resources_module ON resources(module_path);
CREATE INDEX IF NOT EXISTS idx_resources_workspace ON resources(workspace_id);
CREATE INDEX IF NOT EXISTS idx_resources_status ON resources(status);
CREATE INDEX IF NOT EXISTS idx_resources_address ON resources(address);
CREATE INDEX IF NOT EXISTS idx_resource_deps_depends ON resource_dependencies(depends_on_id);
CREATE INDEX IF NOT EXISTS idx_outputs_workspace ON resource_outputs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_runs_workspace ON runs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_run_resources_run ON run_resources(run_id);
CREATE INDEX IF NOT EXISTS idx_modules_workspace ON modules(workspace_id);
CREATE INDEX IF NOT EXISTS idx_resource_history_address ON resource_history(address);
CREATE INDEX IF NOT EXISTS idx_resource_history_workspace ON resource_history(workspace_id);
CREATE INDEX IF NOT EXISTS idx_resource_history_captured ON resource_history(captured_at);
CREATE INDEX IF NOT EXISTS idx_resource_history_addr_time ON resource_history(address, captured_at);
";
