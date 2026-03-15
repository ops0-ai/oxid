use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row};

use super::backend::StateBackend;
use super::models::*;
use super::postgres_schema;

/// PostgreSQL-backed state store for team/production workflows.
/// Uses connection pooling via sqlx for concurrent access.
/// Uses native Postgres types: JSONB, TIMESTAMPTZ, BOOLEAN.
pub struct PostgresBackend {
    pool: PgPool,
}

impl PostgresBackend {
    /// Connect to a PostgreSQL database and return a backend instance.
    pub async fn connect(connection_string: &str, schema: &str) -> Result<Self> {
        // Append search_path to connection string so ALL pool connections use it
        let conn_str = if schema != "public" && !schema.is_empty() {
            let separator = if connection_string.contains('?') {
                "&"
            } else {
                "?"
            };
            format!(
                "{}{}options=-c search_path%3D{}",
                connection_string, separator, schema
            )
        } else {
            connection_string.to_string()
        };

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&conn_str)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to PostgreSQL at {}",
                    connection_string.split('@').last().unwrap_or("(unknown)")
                )
            })?;

        // Create schema if non-public
        if schema != "public" && !schema.is_empty() {
            sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema))
                .execute(&pool)
                .await
                .ok(); // ignore if it already exists or no permission
        }

        Ok(Self { pool })
    }

    fn now() -> String {
        Utc::now().to_rfc3339()
    }

    fn now_dt() -> DateTime<Utc> {
        Utc::now()
    }
}

// ─── Timestamp helpers ───────────────────────────────────────────────────────

/// Parse an RFC3339 string into a chrono DateTime<Utc> for binding to TIMESTAMPTZ.
fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

/// Read a required TIMESTAMPTZ column as RFC3339 String.
fn read_ts(row: &sqlx::postgres::PgRow, col: &str) -> String {
    row.try_get::<DateTime<Utc>, _>(col)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

/// Read an optional TIMESTAMPTZ column as Option<String>.
fn read_optional_ts(row: &sqlx::postgres::PgRow, col: &str) -> Option<String> {
    row.try_get::<Option<DateTime<Utc>>, _>(col)
        .ok()
        .flatten()
        .map(|dt| dt.to_rfc3339())
}

// ─── JSONB helpers ───────────────────────────────────────────────────────────

/// Parse a JSON string into serde_json::Value for binding to JSONB columns.
fn parse_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or(serde_json::json!({}))
}

/// Read a JSONB column as a String (for model compatibility).
fn read_jsonb_as_string(
    row: &sqlx::postgres::PgRow,
    col: &str,
    default: &str,
) -> String {
    row.try_get::<serde_json::Value, _>(col)
        .map(|v| serde_json::to_string(&v).unwrap_or_else(|_| default.to_string()))
        .unwrap_or_else(|_| default.to_string())
}

#[async_trait]
impl StateBackend for PostgresBackend {
    // ─── Initialization ─────────────────────────────────────────────────────

    async fn initialize(&self) -> Result<()> {
        // Detect old TEXT-based schema and drop it (pre-1.0, safe to recreate)
        let row = sqlx::query(
            "SELECT data_type FROM information_schema.columns
             WHERE table_name = 'resources' AND column_name = 'attributes_json'
             AND table_schema = current_schema()",
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(r) = row {
            let dtype: String = r.get("data_type");
            if dtype == "text" {
                tracing::warn!("Detected old TEXT-based Postgres schema. Migrating to native types...");
                sqlx::query(
                    "DROP TABLE IF EXISTS run_resources, runs, resource_locks,
                     resource_outputs, resource_dependencies, resources,
                     providers, workspaces, modules, schema_version CASCADE",
                )
                .execute(&self.pool)
                .await?;
            }
        }

        // Create tables using Postgres-native DDL
        for statement in postgres_schema::PG_CREATE_TABLES_SQL.split(';') {
            let trimmed = statement.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(&self.pool).await?;
            }
        }

        // Create indexes (B-tree + GIN)
        for statement in postgres_schema::PG_CREATE_INDEXES_SQL.split(';') {
            let trimmed = statement.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(&self.pool).await?;
            }
        }

        // Record schema version
        let now = Self::now_dt();
        sqlx::query(
            "INSERT INTO schema_version (version, applied_at, description)
             VALUES ($1, $2, $3)
             ON CONFLICT (version) DO NOTHING",
        )
        .bind(postgres_schema::PG_SCHEMA_VERSION)
        .bind(now)
        .bind("Postgres-native schema (JSONB, TIMESTAMPTZ)")
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ─── Workspace Operations ───────────────────────────────────────────────

    async fn create_workspace(&self, name: &str) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Self::now_dt();
        sqlx::query(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(&id)
        .bind(name)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    async fn get_workspace(&self, name: &str) -> Result<Option<Workspace>> {
        let row = sqlx::query(
            "SELECT id, name, created_at, updated_at FROM workspaces WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Workspace {
            id: r.get("id"),
            name: r.get("name"),
            created_at: read_ts(&r, "created_at"),
            updated_at: read_ts(&r, "updated_at"),
        }))
    }

    async fn list_workspaces(&self) -> Result<Vec<Workspace>> {
        let rows = sqlx::query(
            "SELECT id, name, created_at, updated_at FROM workspaces ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| Workspace {
                id: r.get("id"),
                name: r.get("name"),
                created_at: read_ts(r, "created_at"),
                updated_at: read_ts(r, "updated_at"),
            })
            .collect())
    }

    async fn delete_workspace(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM workspaces WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ─── Resource CRUD ──────────────────────────────────────────────────────

    async fn get_resource(
        &self,
        workspace_id: &str,
        address: &str,
    ) -> Result<Option<ResourceState>> {
        let row = sqlx::query(
            "SELECT id, workspace_id, module_path, resource_type, resource_name,
                    resource_mode, provider_source, index_key, address, status,
                    attributes_json, sensitive_attrs, schema_version, created_at, updated_at
             FROM resources WHERE workspace_id = $1 AND address = $2",
        )
        .bind(workspace_id)
        .bind(address)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| resource_from_row(&r)))
    }

    async fn upsert_resource(&self, resource: &ResourceState) -> Result<()> {
        let attrs = parse_json(&resource.attributes_json);
        let sensitive = serde_json::to_value(&resource.sensitive_attrs)?;
        let created_at = parse_ts(&resource.created_at);
        let updated_at = parse_ts(&resource.updated_at);

        sqlx::query(
            "INSERT INTO resources (id, workspace_id, module_path, resource_type, resource_name,
                resource_mode, provider_source, index_key, address, status,
                attributes_json, sensitive_attrs, schema_version, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
             ON CONFLICT(workspace_id, address) DO UPDATE SET
                status = EXCLUDED.status,
                attributes_json = EXCLUDED.attributes_json,
                sensitive_attrs = EXCLUDED.sensitive_attrs,
                schema_version = EXCLUDED.schema_version,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(&resource.id)
        .bind(&resource.workspace_id)
        .bind(&resource.module_path)
        .bind(&resource.resource_type)
        .bind(&resource.resource_name)
        .bind(&resource.resource_mode)
        .bind(&resource.provider_source)
        .bind(&resource.index_key)
        .bind(&resource.address)
        .bind(&resource.status)
        .bind(attrs)
        .bind(sensitive)
        .bind(resource.schema_version)
        .bind(created_at)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_resource(&self, workspace_id: &str, address: &str) -> Result<()> {
        sqlx::query("DELETE FROM resources WHERE workspace_id = $1 AND address = $2")
            .bind(workspace_id)
            .bind(address)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_resources(
        &self,
        workspace_id: &str,
        filter: &ResourceFilter,
    ) -> Result<Vec<ResourceState>> {
        let mut sql = String::from(
            "SELECT id, workspace_id, module_path, resource_type, resource_name,
                    resource_mode, provider_source, index_key, address, status,
                    attributes_json, sensitive_attrs, schema_version, created_at, updated_at
             FROM resources WHERE workspace_id = $1",
        );
        let mut param_values: Vec<String> = vec![workspace_id.to_string()];
        let mut param_idx = 2;

        if let Some(ref rt) = filter.resource_type {
            sql.push_str(&format!(" AND resource_type = ${}", param_idx));
            param_values.push(rt.clone());
            param_idx += 1;
        }
        if let Some(ref mp) = filter.module_path {
            sql.push_str(&format!(" AND module_path = ${}", param_idx));
            param_values.push(mp.clone());
            param_idx += 1;
        }
        if let Some(ref st) = filter.status {
            sql.push_str(&format!(" AND status = ${}", param_idx));
            param_values.push(st.clone());
            param_idx += 1;
        }
        if let Some(ref pat) = filter.address_pattern {
            sql.push_str(&format!(" AND address LIKE ${}", param_idx));
            param_values.push(pat.clone());
        }

        sql.push_str(" ORDER BY address");

        let mut query = sqlx::query(&sql);
        for val in &param_values {
            query = query.bind(val);
        }

        let rows = query.fetch_all(&self.pool).await?;
        Ok(rows.iter().map(|r| resource_from_row(r)).collect())
    }

    async fn count_resources(&self, workspace_id: &str) -> Result<usize> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM resources WHERE workspace_id = $1")
            .bind(workspace_id)
            .fetch_one(&self.pool)
            .await?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    // ─── Dependencies ───────────────────────────────────────────────────────

    async fn set_dependencies(
        &self,
        resource_id: &str,
        depends_on: &[(String, String)],
    ) -> Result<()> {
        sqlx::query("DELETE FROM resource_dependencies WHERE resource_id = $1")
            .bind(resource_id)
            .execute(&self.pool)
            .await?;

        for (dep_id, dep_type) in depends_on {
            sqlx::query(
                "INSERT INTO resource_dependencies (resource_id, depends_on_id, dependency_type) VALUES ($1, $2, $3)",
            )
            .bind(resource_id)
            .bind(dep_id)
            .bind(dep_type)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_dependencies(&self, resource_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT depends_on_id FROM resource_dependencies WHERE resource_id = $1",
        )
        .bind(resource_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| r.get("depends_on_id")).collect())
    }

    async fn get_dependents(&self, resource_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT resource_id FROM resource_dependencies WHERE depends_on_id = $1",
        )
        .bind(resource_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| r.get("resource_id")).collect())
    }

    // ─── Locking ────────────────────────────────────────────────────────────

    async fn acquire_lock(
        &self,
        address: &str,
        workspace_id: &str,
        info: &LockInfo,
    ) -> Result<Lock> {
        let now = Self::now_dt();
        let now_str = Self::now();
        let lock_id = uuid::Uuid::new_v4().to_string();
        let expires_at = info
            .ttl_secs
            .map(|ttl| Utc::now() + chrono::Duration::seconds(ttl as i64));
        let expires_at_str = expires_at.map(|dt| dt.to_rfc3339());

        // Clean up expired locks first
        sqlx::query(
            "DELETE FROM resource_locks WHERE expires_at IS NOT NULL AND expires_at < $1",
        )
        .bind(now)
        .execute(&self.pool)
        .await?;

        // Try to insert the lock (will fail if already locked due to PRIMARY KEY)
        sqlx::query(
            "INSERT INTO resource_locks (resource_address, workspace_id, locked_at, locked_by, lock_id, operation, expires_at, info)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(address)
        .bind(workspace_id)
        .bind(now)
        .bind(&info.locked_by)
        .bind(&lock_id)
        .bind(&info.operation)
        .bind(expires_at)
        .bind(&info.info)
        .execute(&self.pool)
        .await
        .with_context(|| format!("Resource {} is already locked", address))?;

        Ok(Lock {
            resource_address: address.to_string(),
            workspace_id: workspace_id.to_string(),
            locked_at: now_str,
            locked_by: info.locked_by.clone(),
            lock_id,
            operation: info.operation.clone(),
            expires_at: expires_at_str,
            info: info.info.clone(),
        })
    }

    async fn release_lock(&self, lock_id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM resource_locks WHERE lock_id = $1")
            .bind(lock_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            anyhow::bail!("Lock {} not found", lock_id);
        }
        Ok(())
    }

    async fn force_unlock(&self, address: &str, workspace_id: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM resource_locks WHERE resource_address = $1 AND workspace_id = $2",
        )
        .bind(address)
        .bind(workspace_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn is_locked(&self, address: &str, workspace_id: &str) -> Result<Option<Lock>> {
        let now = Self::now_dt();

        // Clean expired locks
        sqlx::query(
            "DELETE FROM resource_locks WHERE expires_at IS NOT NULL AND expires_at < $1",
        )
        .bind(now)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT resource_address, workspace_id, locked_at, locked_by, lock_id, operation, expires_at, info
             FROM resource_locks WHERE resource_address = $1 AND workspace_id = $2",
        )
        .bind(address)
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Lock {
            resource_address: r.get("resource_address"),
            workspace_id: r.get("workspace_id"),
            locked_at: read_ts(&r, "locked_at"),
            locked_by: r.get("locked_by"),
            lock_id: r.get("lock_id"),
            operation: r.get("operation"),
            expires_at: read_optional_ts(&r, "expires_at"),
            info: r.get("info"),
        }))
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
        sqlx::query(
            "INSERT INTO resource_outputs (id, workspace_id, module_path, output_name, output_value, sensitive)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT(workspace_id, module_path, output_name) DO UPDATE SET
                output_value = EXCLUDED.output_value, sensitive = EXCLUDED.sensitive",
        )
        .bind(&id)
        .bind(workspace_id)
        .bind(module_path)
        .bind(name)
        .bind(value)
        .bind(sensitive)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_output(
        &self,
        workspace_id: &str,
        module_path: &str,
        name: &str,
    ) -> Result<Option<OutputValue>> {
        let row = sqlx::query(
            "SELECT id, workspace_id, module_path, output_name, output_value, sensitive
             FROM resource_outputs WHERE workspace_id = $1 AND module_path = $2 AND output_name = $3",
        )
        .bind(workspace_id)
        .bind(module_path)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| OutputValue {
            id: r.get("id"),
            workspace_id: r.get("workspace_id"),
            module_path: r.get("module_path"),
            output_name: r.get("output_name"),
            output_value: r.get("output_value"),
            sensitive: r.get::<bool, _>("sensitive"),
        }))
    }

    async fn list_outputs(
        &self,
        workspace_id: &str,
        module_path: Option<&str>,
    ) -> Result<Vec<OutputValue>> {
        let rows = if let Some(mp) = module_path {
            sqlx::query(
                "SELECT id, workspace_id, module_path, output_name, output_value, sensitive
                 FROM resource_outputs WHERE workspace_id = $1 AND module_path = $2 ORDER BY output_name",
            )
            .bind(workspace_id)
            .bind(mp)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT id, workspace_id, module_path, output_name, output_value, sensitive
                 FROM resource_outputs WHERE workspace_id = $1 ORDER BY module_path, output_name",
            )
            .bind(workspace_id)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .iter()
            .map(|r| OutputValue {
                id: r.get("id"),
                workspace_id: r.get("workspace_id"),
                module_path: r.get("module_path"),
                output_name: r.get("output_name"),
                output_value: r.get("output_value"),
                sensitive: r.get::<bool, _>("sensitive"),
            })
            .collect())
    }

    async fn clear_outputs(&self, workspace_id: &str, module_path: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM resource_outputs WHERE workspace_id = $1 AND module_path = $2",
        )
        .bind(workspace_id)
        .bind(module_path)
        .execute(&self.pool)
        .await?;
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
        let now = Self::now_dt();
        sqlx::query(
            "INSERT INTO runs (id, workspace_id, started_at, status, operation, resources_planned)
             VALUES ($1, $2, $3, 'running', $4, $5)",
        )
        .bind(&id)
        .bind(workspace_id)
        .bind(now)
        .bind(operation)
        .bind(resources_planned)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    async fn complete_run(
        &self,
        run_id: &str,
        status: &str,
        resources_succeeded: i32,
        resources_failed: i32,
    ) -> Result<()> {
        let now = Self::now_dt();
        sqlx::query(
            "UPDATE runs SET completed_at = $2, status = $3, resources_succeeded = $4, resources_failed = $5
             WHERE id = $1",
        )
        .bind(run_id)
        .bind(now)
        .bind(status)
        .bind(resources_succeeded)
        .bind(resources_failed)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_resource_result(&self, run_id: &str, result: &ResourceResult) -> Result<()> {
        let started_at = result.started_at.as_deref().map(parse_ts);
        let completed_at = result.completed_at.as_deref().map(parse_ts);
        let diff_json: Option<serde_json::Value> = result
            .diff_json
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok());

        sqlx::query(
            "INSERT INTO run_resources (run_id, resource_address, action, status, started_at, completed_at, error_message, diff_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT(run_id, resource_address) DO UPDATE SET
                status = EXCLUDED.status, completed_at = EXCLUDED.completed_at,
                error_message = EXCLUDED.error_message, diff_json = EXCLUDED.diff_json",
        )
        .bind(run_id)
        .bind(&result.address)
        .bind(&result.action)
        .bind(&result.status)
        .bind(started_at)
        .bind(completed_at)
        .bind(&result.error_message)
        .bind(diff_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_latest_run(&self, workspace_id: &str) -> Result<Option<RunRecord>> {
        let row = sqlx::query(
            "SELECT id, workspace_id, started_at, completed_at, status, operation,
                    resources_planned, resources_succeeded, resources_failed, error_message
             FROM runs WHERE workspace_id = $1 ORDER BY started_at DESC LIMIT 1",
        )
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| run_from_row(&r)))
    }

    async fn list_runs(&self, workspace_id: &str, limit: usize) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, started_at, completed_at, status, operation,
                    resources_planned, resources_succeeded, resources_failed, error_message
             FROM runs WHERE workspace_id = $1 ORDER BY started_at DESC LIMIT $2",
        )
        .bind(workspace_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(|r| run_from_row(r)).collect())
    }

    // ─── Query ──────────────────────────────────────────────────────────────

    async fn query_raw(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;

        let mut result = Vec::new();
        for row in &rows {
            let mut map = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let type_info = col.type_info().to_string();

                let value = match type_info.as_str() {
                    "JSONB" | "JSON" => row
                        .try_get::<serde_json::Value, _>(col.ordinal())
                        .unwrap_or(serde_json::Value::Null),
                    "TIMESTAMPTZ" | "TIMESTAMP" => {
                        match row.try_get::<DateTime<Utc>, _>(col.ordinal()) {
                            Ok(dt) => serde_json::Value::String(dt.to_rfc3339()),
                            Err(_) => serde_json::Value::Null,
                        }
                    }
                    "BOOL" => match row.try_get::<bool, _>(col.ordinal()) {
                        Ok(b) => serde_json::Value::Bool(b),
                        Err(_) => serde_json::Value::Null,
                    },
                    "INT4" | "INT8" | "INT2" => row
                        .try_get::<i64, _>(col.ordinal())
                        .map(|v| serde_json::json!(v))
                        .or_else(|_| {
                            row.try_get::<i32, _>(col.ordinal())
                                .map(|v| serde_json::json!(v))
                        })
                        .unwrap_or(serde_json::Value::Null),
                    _ => row
                        .try_get::<String, _>(col.ordinal())
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                };
                map.insert(name, value);
            }
            result.push(serde_json::Value::Object(map));
        }

        Ok(result)
    }

    // ─── Import ─────────────────────────────────────────────────────────────

    async fn import_tfstate(&self, workspace_id: &str, state_json: &str) -> Result<ImportResult> {
        let state: TfState =
            serde_json::from_str(state_json).context("Failed to parse .tfstate JSON")?;

        let mut imported = 0;
        let mut skipped = 0;
        let mut warnings = Vec::new();
        let now = Self::now_dt();

        for tf_resource in &state.resources {
            for (idx, instance) in tf_resource.instances.iter().enumerate() {
                let address = if let Some(ref key) = instance.index_key {
                    format!(
                        "{}.{}[{}]",
                        tf_resource.resource_type, tf_resource.name, key
                    )
                } else if tf_resource.instances.len() > 1 {
                    format!(
                        "{}.{}[{}]",
                        tf_resource.resource_type, tf_resource.name, idx
                    )
                } else {
                    format!("{}.{}", tf_resource.resource_type, tf_resource.name)
                };

                let id = uuid::Uuid::new_v4().to_string();
                // Bind as serde_json::Value for JSONB columns
                let attrs: serde_json::Value = instance.attributes.clone();
                let sensitive: serde_json::Value =
                    serde_json::to_value(&instance.sensitive_attributes)
                        .unwrap_or(serde_json::json!([]));

                let result = sqlx::query(
                    "INSERT INTO resources (id, workspace_id, module_path, resource_type, resource_name,
                        resource_mode, provider_source, index_key, address, status,
                        attributes_json, sensitive_attrs, schema_version, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                     ON CONFLICT(workspace_id, address) DO NOTHING",
                )
                .bind(&id)
                .bind(workspace_id)
                .bind("")
                .bind(&tf_resource.resource_type)
                .bind(&tf_resource.name)
                .bind(&tf_resource.mode)
                .bind(&tf_resource.provider)
                .bind(&instance.index_key)
                .bind(&address)
                .bind("created")
                .bind(attrs)
                .bind(sensitive)
                .bind(instance.schema_version.unwrap_or(0))
                .bind(now)
                .bind(now)
                .execute(&self.pool)
                .await;

                match result {
                    Ok(r) if r.rows_affected() > 0 => imported += 1,
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
            let _ = sqlx::query(
                "INSERT INTO resource_outputs (id, workspace_id, module_path, output_name, output_value, sensitive)
                 VALUES ($1, $2, '', $3, $4, $5)
                 ON CONFLICT(workspace_id, module_path, output_name) DO UPDATE SET
                    output_value = EXCLUDED.output_value",
            )
            .bind(&id)
            .bind(workspace_id)
            .bind(name)
            .bind(&value_str)
            .bind(output.sensitive.unwrap_or(false))
            .execute(&self.pool)
            .await;
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
        sqlx::query(
            "INSERT INTO providers (id, workspace_id, source, version)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(workspace_id, source) DO UPDATE SET version = EXCLUDED.version",
        )
        .bind(&id)
        .bind(workspace_id)
        .bind(source)
        .bind(version)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    async fn list_providers(&self, workspace_id: &str) -> Result<Vec<(String, String, String)>> {
        let rows = sqlx::query(
            "SELECT id, source, version FROM providers WHERE workspace_id = $1 ORDER BY source",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| (r.get("id"), r.get("source"), r.get("version")))
            .collect())
    }
}

// ─── Helper functions ───────────────────────────────────────────────────────

fn resource_from_row(row: &sqlx::postgres::PgRow) -> ResourceState {
    // JSONB → serde_json::Value → String for model compatibility
    let attributes_json = read_jsonb_as_string(row, "attributes_json", "{}");

    // JSONB → serde_json::Value → Vec<String>
    let sensitive_val: serde_json::Value = row
        .try_get("sensitive_attrs")
        .unwrap_or(serde_json::json!([]));
    let sensitive_attrs: Vec<String> = serde_json::from_value(sensitive_val).unwrap_or_default();

    ResourceState {
        id: row.try_get("id").unwrap_or_default(),
        workspace_id: row.try_get("workspace_id").unwrap_or_default(),
        module_path: row.try_get("module_path").unwrap_or_default(),
        resource_type: row.try_get("resource_type").unwrap_or_default(),
        resource_name: row.try_get("resource_name").unwrap_or_default(),
        resource_mode: row.try_get("resource_mode").unwrap_or_default(),
        provider_source: row.try_get("provider_source").unwrap_or_default(),
        index_key: row.try_get("index_key").unwrap_or_default(),
        address: row.try_get("address").unwrap_or_default(),
        status: row.try_get("status").unwrap_or_default(),
        attributes_json,
        sensitive_attrs,
        schema_version: row.try_get("schema_version").unwrap_or_default(),
        created_at: read_ts(row, "created_at"),
        updated_at: read_ts(row, "updated_at"),
    }
}

fn run_from_row(row: &sqlx::postgres::PgRow) -> RunRecord {
    RunRecord {
        id: row.try_get("id").unwrap_or_default(),
        workspace_id: row.try_get("workspace_id").unwrap_or_default(),
        started_at: read_ts(row, "started_at"),
        completed_at: read_optional_ts(row, "completed_at"),
        status: row.try_get("status").unwrap_or_default(),
        operation: row.try_get("operation").unwrap_or_default(),
        resources_planned: row.try_get("resources_planned").unwrap_or_default(),
        resources_succeeded: row.try_get("resources_succeeded").unwrap_or_default(),
        resources_failed: row.try_get("resources_failed").unwrap_or_default(),
        error_message: row.try_get("error_message").unwrap_or_default(),
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
    sensitive_attributes: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    dependencies: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct TfOutput {
    value: serde_json::Value,
    #[serde(rename = "type")]
    _output_type: Option<serde_json::Value>,
    sensitive: Option<bool>,
}
