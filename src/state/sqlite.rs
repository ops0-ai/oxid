use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::path::Path;
use std::sync::Mutex;

use super::backend::StateBackend;
use super::models::*;
use super::schema;

/// SQLite-backed state store for local development and single-user workflows.
pub struct SqliteBackend {
    conn: Mutex<Connection>,
}

impl SqliteBackend {
    /// Open or create the SQLite state database.
    pub fn open(db_path: &str) -> Result<Self> {
        let parent = Path::new(db_path).parent();
        if let Some(dir) = parent {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)?;
            }
        }
        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open state database at {}", db_path))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory database (for testing).
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn now() -> String {
        chrono::Utc::now().to_rfc3339()
    }
}

#[async_trait]
impl StateBackend for SqliteBackend {
    // ─── Initialization ─────────────────────────────────────────────────────

    async fn initialize(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(schema::CREATE_TABLES_SQL)?;
        conn.execute_batch(schema::CREATE_INDEXES_SQL)?;

        // Record schema version
        conn.execute(
            "INSERT OR IGNORE INTO schema_version (version, applied_at, description) VALUES (?1, ?2, ?3)",
            params![schema::SCHEMA_VERSION, Self::now(), "Initial schema"],
        )?;
        Ok(())
    }

    // ─── Workspace Operations ───────────────────────────────────────────────

    async fn create_workspace(&self, name: &str) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![id, name, now, now],
        )?;
        Ok(id)
    }

    async fn get_workspace(&self, name: &str) -> Result<Option<Workspace>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, created_at, updated_at FROM workspaces WHERE name = ?1")?;
        let result = stmt
            .query_row(params![name], |row| {
                Ok(Workspace {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            })
            .ok();
        Ok(result)
    }

    async fn list_workspaces(&self) -> Result<Vec<Workspace>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT id, name, created_at, updated_at FROM workspaces ORDER BY name")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(Workspace {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    async fn delete_workspace(&self, name: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM workspaces WHERE name = ?1", params![name])?;
        Ok(())
    }

    // ─── Resource CRUD ──────────────────────────────────────────────────────

    async fn get_resource(
        &self,
        workspace_id: &str,
        address: &str,
    ) -> Result<Option<ResourceState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, module_path, resource_type, resource_name,
                    resource_mode, provider_source, index_key, address, status,
                    attributes_json, sensitive_attrs, schema_version, created_at, updated_at
             FROM resources WHERE workspace_id = ?1 AND address = ?2",
        )?;
        let result = stmt
            .query_row(params![workspace_id, address], |row| {
                Ok(resource_from_row(row))
            })
            .ok();
        Ok(result)
    }

    async fn upsert_resource(&self, resource: &ResourceState) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let sensitive_json = serde_json::to_string(&resource.sensitive_attrs)?;
        conn.execute(
            "INSERT INTO resources (id, workspace_id, module_path, resource_type, resource_name,
                resource_mode, provider_source, index_key, address, status,
                attributes_json, sensitive_attrs, schema_version, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
             ON CONFLICT(workspace_id, address) DO UPDATE SET
                status = excluded.status,
                attributes_json = excluded.attributes_json,
                sensitive_attrs = excluded.sensitive_attrs,
                schema_version = excluded.schema_version,
                updated_at = excluded.updated_at",
            params![
                resource.id,
                resource.workspace_id,
                resource.module_path,
                resource.resource_type,
                resource.resource_name,
                resource.resource_mode,
                resource.provider_source,
                resource.index_key,
                resource.address,
                resource.status,
                resource.attributes_json,
                sensitive_json,
                resource.schema_version,
                resource.created_at,
                resource.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn delete_resource(&self, workspace_id: &str, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM resources WHERE workspace_id = ?1 AND address = ?2",
            params![workspace_id, address],
        )?;
        Ok(())
    }

    async fn list_resources(
        &self,
        workspace_id: &str,
        filter: &ResourceFilter,
    ) -> Result<Vec<ResourceState>> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT id, workspace_id, module_path, resource_type, resource_name,
                    resource_mode, provider_source, index_key, address, status,
                    attributes_json, sensitive_attrs, schema_version, created_at, updated_at
             FROM resources WHERE workspace_id = ?1",
        );
        let mut param_values: Vec<String> = vec![workspace_id.to_string()];
        let mut param_idx = 2;

        if let Some(ref rt) = filter.resource_type {
            sql.push_str(&format!(" AND resource_type = ?{}", param_idx));
            param_values.push(rt.clone());
            param_idx += 1;
        }
        if let Some(ref mp) = filter.module_path {
            sql.push_str(&format!(" AND module_path = ?{}", param_idx));
            param_values.push(mp.clone());
            param_idx += 1;
        }
        if let Some(ref st) = filter.status {
            sql.push_str(&format!(" AND status = ?{}", param_idx));
            param_values.push(st.clone());
            param_idx += 1;
        }
        if let Some(ref pat) = filter.address_pattern {
            sql.push_str(&format!(" AND address LIKE ?{}", param_idx));
            param_values.push(pat.clone());
            // param_idx not needed after last use
        }

        sql.push_str(" ORDER BY address");

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> = param_values
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| Ok(resource_from_row(row)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    async fn count_resources(&self, workspace_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM resources WHERE workspace_id = ?1",
            params![workspace_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    // ─── Dependencies ───────────────────────────────────────────────────────

    async fn set_dependencies(
        &self,
        resource_id: &str,
        depends_on: &[(String, String)],
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM resource_dependencies WHERE resource_id = ?1",
            params![resource_id],
        )?;
        let mut stmt = conn.prepare(
            "INSERT INTO resource_dependencies (resource_id, depends_on_id, dependency_type) VALUES (?1, ?2, ?3)",
        )?;
        for (dep_id, dep_type) in depends_on {
            stmt.execute(params![resource_id, dep_id, dep_type])?;
        }
        Ok(())
    }

    async fn get_dependencies(&self, resource_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT depends_on_id FROM resource_dependencies WHERE resource_id = ?1")?;
        let rows = stmt
            .query_map(params![resource_id], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(rows)
    }

    async fn get_dependents(&self, resource_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT resource_id FROM resource_dependencies WHERE depends_on_id = ?1")?;
        let rows = stmt
            .query_map(params![resource_id], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(rows)
    }

    // ─── Locking ────────────────────────────────────────────────────────────

    async fn acquire_lock(
        &self,
        address: &str,
        workspace_id: &str,
        info: &LockInfo,
    ) -> Result<Lock> {
        let conn = self.conn.lock().unwrap();
        let now = Self::now();
        let lock_id = uuid::Uuid::new_v4().to_string();
        let expires_at = info
            .ttl_secs
            .map(|ttl| (chrono::Utc::now() + chrono::Duration::seconds(ttl as i64)).to_rfc3339());

        // Clean up expired locks first
        conn.execute(
            "DELETE FROM resource_locks WHERE expires_at IS NOT NULL AND expires_at < ?1",
            params![now],
        )?;

        // Try to insert the lock (will fail if already locked)
        conn.execute(
            "INSERT INTO resource_locks (resource_address, workspace_id, locked_at, locked_by, lock_id, operation, expires_at, info)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                address,
                workspace_id,
                now,
                info.locked_by,
                lock_id,
                info.operation,
                expires_at,
                info.info,
            ],
        ).with_context(|| format!("Resource {} is already locked", address))?;

        Ok(Lock {
            resource_address: address.to_string(),
            workspace_id: workspace_id.to_string(),
            locked_at: now,
            locked_by: info.locked_by.clone(),
            lock_id,
            operation: info.operation.clone(),
            expires_at,
            info: info.info.clone(),
        })
    }

    async fn release_lock(&self, lock_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM resource_locks WHERE lock_id = ?1",
            params![lock_id],
        )?;
        if rows == 0 {
            anyhow::bail!("Lock {} not found", lock_id);
        }
        Ok(())
    }

    async fn force_unlock(&self, address: &str, workspace_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM resource_locks WHERE resource_address = ?1 AND workspace_id = ?2",
            params![address, workspace_id],
        )?;
        Ok(())
    }

    async fn is_locked(&self, address: &str, workspace_id: &str) -> Result<Option<Lock>> {
        let conn = self.conn.lock().unwrap();
        let now = Self::now();

        // Clean expired locks
        conn.execute(
            "DELETE FROM resource_locks WHERE expires_at IS NOT NULL AND expires_at < ?1",
            params![now],
        )?;

        let mut stmt = conn.prepare(
            "SELECT resource_address, workspace_id, locked_at, locked_by, lock_id, operation, expires_at, info
             FROM resource_locks WHERE resource_address = ?1 AND workspace_id = ?2",
        )?;
        let result = stmt
            .query_row(params![address, workspace_id], |row| {
                Ok(Lock {
                    resource_address: row.get(0)?,
                    workspace_id: row.get(1)?,
                    locked_at: row.get(2)?,
                    locked_by: row.get(3)?,
                    lock_id: row.get(4)?,
                    operation: row.get(5)?,
                    expires_at: row.get(6)?,
                    info: row.get(7)?,
                })
            })
            .ok();
        Ok(result)
    }

    // ─── Outputs ────────────────────────────────────────────────────────────

    async fn set_output(
        &self,
        workspace_id: &str,
        module_path: &str,
        name: &str,
        value: &str,
        sensitive: bool,
    ) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO resource_outputs (id, workspace_id, module_path, output_name, output_value, sensitive)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(workspace_id, module_path, output_name) DO UPDATE SET
                output_value = excluded.output_value, sensitive = excluded.sensitive",
            params![id, workspace_id, module_path, name, value, sensitive as i32],
        )?;
        Ok(())
    }

    async fn get_output(
        &self,
        workspace_id: &str,
        module_path: &str,
        name: &str,
    ) -> Result<Option<OutputValue>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, module_path, output_name, output_value, sensitive
             FROM resource_outputs WHERE workspace_id = ?1 AND module_path = ?2 AND output_name = ?3",
        )?;
        let result = stmt
            .query_row(params![workspace_id, module_path, name], |row| {
                Ok(OutputValue {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    module_path: row.get(2)?,
                    output_name: row.get(3)?,
                    output_value: row.get(4)?,
                    sensitive: row.get::<_, i32>(5)? != 0,
                })
            })
            .ok();
        Ok(result)
    }

    async fn list_outputs(
        &self,
        workspace_id: &str,
        module_path: Option<&str>,
    ) -> Result<Vec<OutputValue>> {
        let conn = self.conn.lock().unwrap();
        let (sql, param_values): (String, Vec<String>) = if let Some(mp) = module_path {
            (
                "SELECT id, workspace_id, module_path, output_name, output_value, sensitive
                 FROM resource_outputs WHERE workspace_id = ?1 AND module_path = ?2 ORDER BY output_name".to_string(),
                vec![workspace_id.to_string(), mp.to_string()],
            )
        } else {
            (
                "SELECT id, workspace_id, module_path, output_name, output_value, sensitive
                 FROM resource_outputs WHERE workspace_id = ?1 ORDER BY module_path, output_name"
                    .to_string(),
                vec![workspace_id.to_string()],
            )
        };

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> = param_values
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(OutputValue {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    module_path: row.get(2)?,
                    output_name: row.get(3)?,
                    output_value: row.get(4)?,
                    sensitive: row.get::<_, i32>(5)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    async fn clear_outputs(&self, workspace_id: &str, module_path: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM resource_outputs WHERE workspace_id = ?1 AND module_path = ?2",
            params![workspace_id, module_path],
        )?;
        Ok(())
    }

    // ─── Runs ───────────────────────────────────────────────────────────────

    async fn start_run(
        &self,
        workspace_id: &str,
        operation: &str,
        resources_planned: i32,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO runs (id, workspace_id, started_at, status, operation, resources_planned)
             VALUES (?1, ?2, ?3, 'running', ?4, ?5)",
            params![id, workspace_id, now, operation, resources_planned],
        )?;
        Ok(id)
    }

    async fn complete_run(
        &self,
        run_id: &str,
        status: &str,
        resources_succeeded: i32,
        resources_failed: i32,
    ) -> Result<()> {
        let now = Self::now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE runs SET completed_at = ?2, status = ?3, resources_succeeded = ?4, resources_failed = ?5
             WHERE id = ?1",
            params![run_id, now, status, resources_succeeded, resources_failed],
        )?;
        Ok(())
    }

    async fn record_resource_result(&self, run_id: &str, result: &ResourceResult) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO run_resources (run_id, resource_address, action, status, started_at, completed_at, error_message, diff_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(run_id, resource_address) DO UPDATE SET
                status = excluded.status, completed_at = excluded.completed_at,
                error_message = excluded.error_message, diff_json = excluded.diff_json",
            params![
                run_id,
                result.address,
                result.action,
                result.status,
                result.started_at,
                result.completed_at,
                result.error_message,
                result.diff_json,
            ],
        )?;
        Ok(())
    }

    async fn get_latest_run(&self, workspace_id: &str) -> Result<Option<RunRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, started_at, completed_at, status, operation,
                    resources_planned, resources_succeeded, resources_failed, error_message
             FROM runs WHERE workspace_id = ?1 ORDER BY started_at DESC LIMIT 1",
        )?;
        let result = stmt
            .query_row(params![workspace_id], |row| {
                Ok(RunRecord {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    started_at: row.get(2)?,
                    completed_at: row.get(3)?,
                    status: row.get(4)?,
                    operation: row.get(5)?,
                    resources_planned: row.get(6)?,
                    resources_succeeded: row.get(7)?,
                    resources_failed: row.get(8)?,
                    error_message: row.get(9)?,
                })
            })
            .ok();
        Ok(result)
    }

    async fn list_runs(&self, workspace_id: &str, limit: usize) -> Result<Vec<RunRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, workspace_id, started_at, completed_at, status, operation,
                    resources_planned, resources_succeeded, resources_failed, error_message
             FROM runs WHERE workspace_id = ?1 ORDER BY started_at DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![workspace_id, limit as i64], |row| {
                Ok(RunRecord {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    started_at: row.get(2)?,
                    completed_at: row.get(3)?,
                    status: row.get(4)?,
                    operation: row.get(5)?,
                    resources_planned: row.get(6)?,
                    resources_succeeded: row.get(7)?,
                    resources_failed: row.get(8)?,
                    error_message: row.get(9)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ─── Query ──────────────────────────────────────────────────────────────

    async fn query_raw(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        let rows = stmt.query_map([], |row| {
            let mut map = serde_json::Map::new();
            for (i, col_name) in column_names.iter().enumerate() {
                let value: rusqlite::Result<String> = row.get(i);
                match value {
                    Ok(v) => {
                        // Try to parse as JSON first (for attributes_json etc.)
                        if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&v) {
                            map.insert(col_name.clone(), json_val);
                        } else {
                            map.insert(col_name.clone(), serde_json::Value::String(v));
                        }
                    }
                    Err(_) => {
                        // Try as integer
                        let int_val: rusqlite::Result<i64> = row.get(i);
                        match int_val {
                            Ok(i) => {
                                map.insert(col_name.clone(), serde_json::json!(i));
                            }
                            Err(_) => {
                                map.insert(col_name.clone(), serde_json::Value::Null);
                            }
                        }
                    }
                }
            }
            Ok(serde_json::Value::Object(map))
        })?;

        let result: Vec<serde_json::Value> = rows.filter_map(|r| r.ok()).collect();
        Ok(result)
    }

    // ─── Import ─────────────────────────────────────────────────────────────

    async fn import_tfstate(&self, workspace_id: &str, state_json: &str) -> Result<ImportResult> {
        let state: TfState =
            serde_json::from_str(state_json).context("Failed to parse .tfstate JSON")?;

        let mut imported = 0;
        let mut skipped = 0;
        let mut warnings = Vec::new();
        let now = Self::now();

        let conn = self.conn.lock().unwrap();

        for tf_resource in &state.resources {
            for (idx, instance) in tf_resource.instances.iter().enumerate() {
                let address = if let Some(ref key) = instance.index_key {
                    // count or for_each resource — always include index
                    format!(
                        "{}.{}[{}]",
                        tf_resource.resource_type, tf_resource.name, key
                    )
                } else if tf_resource.instances.len() > 1 {
                    // Multiple instances without explicit index_key — use positional index
                    format!(
                        "{}.{}[{}]",
                        tf_resource.resource_type, tf_resource.name, idx
                    )
                } else {
                    format!("{}.{}", tf_resource.resource_type, tf_resource.name)
                };

                let id = uuid::Uuid::new_v4().to_string();
                let attrs_json = serde_json::to_string(&instance.attributes)
                    .unwrap_or_else(|_| "{}".to_string());
                let sensitive_json = serde_json::to_string(&instance.sensitive_attributes)
                    .unwrap_or_else(|_| "[]".to_string());

                let result = conn.execute(
                    "INSERT INTO resources (id, workspace_id, module_path, resource_type, resource_name,
                        resource_mode, provider_source, index_key, address, status,
                        attributes_json, sensitive_attrs, schema_version, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                     ON CONFLICT(workspace_id, address) DO NOTHING",
                    params![
                        id,
                        workspace_id,
                        "",  // module_path - would need to be extracted from resource
                        tf_resource.resource_type,
                        tf_resource.name,
                        tf_resource.mode,
                        tf_resource.provider,
                        instance.index_key,
                        address,
                        "created",
                        attrs_json,
                        sensitive_json,
                        instance.schema_version.unwrap_or(0),
                        now,
                        now,
                    ],
                );

                match result {
                    Ok(rows) if rows > 0 => imported += 1,
                    Ok(_) => {
                        skipped += 1;
                        warnings.push(format!("Skipped {} (already exists)", address));
                    }
                    Err(e) => {
                        skipped += 1;
                        warnings.push(format!("Failed to import {}: {}", address, e));
                    }
                }
            }
        }

        // Import outputs
        for (name, output) in &state.outputs {
            let id = uuid::Uuid::new_v4().to_string();
            let value_str = serde_json::to_string(&output.value).unwrap_or_default();
            let _ = conn.execute(
                "INSERT INTO resource_outputs (id, workspace_id, module_path, output_name, output_value, sensitive)
                 VALUES (?1, ?2, '', ?3, ?4, ?5)
                 ON CONFLICT(workspace_id, module_path, output_name) DO UPDATE SET
                    output_value = excluded.output_value",
                params![id, workspace_id, name, value_str, output.sensitive.unwrap_or(false) as i32],
            );
        }

        Ok(ImportResult {
            imported,
            skipped,
            warnings,
        })
    }

    // ─── Providers ──────────────────────────────────────────────────────────

    async fn register_provider(
        &self,
        workspace_id: &str,
        source: &str,
        version: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO providers (id, workspace_id, source, version)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(workspace_id, source) DO UPDATE SET version = excluded.version",
            params![id, workspace_id, source, version],
        )?;
        Ok(id)
    }

    async fn list_providers(&self, workspace_id: &str) -> Result<Vec<(String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source, version FROM providers WHERE workspace_id = ?1 ORDER BY source",
        )?;
        let rows = stmt
            .query_map(params![workspace_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

// ─── Helper functions ───────────────────────────────────────────────────────

fn resource_from_row(row: &rusqlite::Row<'_>) -> ResourceState {
    let sensitive_json: String = row.get(11).unwrap_or_default();
    let sensitive_attrs: Vec<String> = serde_json::from_str(&sensitive_json).unwrap_or_default();

    ResourceState {
        id: row.get(0).unwrap_or_default(),
        workspace_id: row.get(1).unwrap_or_default(),
        module_path: row.get(2).unwrap_or_default(),
        resource_type: row.get(3).unwrap_or_default(),
        resource_name: row.get(4).unwrap_or_default(),
        resource_mode: row.get(5).unwrap_or_default(),
        provider_source: row.get(6).unwrap_or_default(),
        index_key: row.get(7).unwrap_or_default(),
        address: row.get(8).unwrap_or_default(),
        status: row.get(9).unwrap_or_default(),
        attributes_json: row.get(10).unwrap_or_default(),
        sensitive_attrs,
        schema_version: row.get(12).unwrap_or_default(),
        created_at: row.get(13).unwrap_or_default(),
        updated_at: row.get(14).unwrap_or_default(),
    }
}

// ─── Terraform state file types for import ──────────────────────────────────

fn deserialize_index_key<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    Ok(v.map(|val| match val {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }))
}

#[derive(Debug, serde::Deserialize)]
struct TfState {
    #[serde(default)]
    resources: Vec<TfStateResource>,
    #[serde(default)]
    outputs: std::collections::HashMap<String, TfOutput>,
}

#[derive(Debug, serde::Deserialize)]
struct TfStateResource {
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(rename = "type")]
    resource_type: String,
    name: String,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    instances: Vec<TfInstance>,
}

fn default_mode() -> String {
    "managed".to_string()
}

#[derive(Debug, serde::Deserialize)]
struct TfInstance {
    #[serde(default, deserialize_with = "deserialize_index_key")]
    index_key: Option<String>,
    #[serde(default)]
    schema_version: Option<i32>,
    #[serde(default)]
    attributes: serde_json::Value,
    #[serde(default)]
    sensitive_attributes: serde_json::Value,
    #[serde(default)]
    #[allow(dead_code)]
    dependencies: Vec<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize)]
struct TfOutput {
    value: serde_json::Value,
    #[serde(rename = "type")]
    _output_type: Option<serde_json::Value>,
    sensitive: Option<bool>,
}
