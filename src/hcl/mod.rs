pub mod json_parser;
pub mod parser;

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::types::{Expression, Value, WorkspaceConfig};

/// Parse all .tf and .tf.json files in a directory into a unified WorkspaceConfig.
pub fn parse_directory(dir: &Path) -> Result<WorkspaceConfig> {
    let mut workspace = WorkspaceConfig::default();

    let all_entries: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    // Collect .tf files (native HCL)
    let mut tf_files: Vec<std::path::PathBuf> = all_entries
        .iter()
        .filter(|p| p.extension().map(|e| e == "tf").unwrap_or(false))
        .cloned()
        .collect();
    tf_files.sort();

    // Collect .tf.json files
    let mut tf_json_files: Vec<std::path::PathBuf> = all_entries
        .iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".tf.json"))
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    tf_json_files.sort();

    if tf_files.is_empty() && tf_json_files.is_empty() {
        anyhow::bail!(
            "No .tf or .tf.json files found in directory: {}",
            dir.display()
        );
    }

    // Parse .tf files first, then .tf.json files (matches Terraform behavior)
    for file in &tf_files {
        tracing::debug!("Parsing HCL file: {}", file.display());
        let content = std::fs::read_to_string(file)?;
        let partial = parser::parse_hcl(&content, file)?;
        merge_workspace(&mut workspace, partial);
    }

    for file in &tf_json_files {
        tracing::debug!("Parsing tf.json file: {}", file.display());
        let content = std::fs::read_to_string(file)?;
        let partial = json_parser::parse_tf_json(&content, file)?;
        merge_workspace(&mut workspace, partial);
    }

    // Load .tfvars files and apply them to variable defaults.
    // Precedence (highest to lowest):
    //   1. TF_VAR_xxx environment variables
    //   2. *.auto.tfvars / *.auto.tfvars.json (alphabetical)
    //   3. terraform.tfvars.json (if present)
    //   4. terraform.tfvars (if present)
    //   5. Variable defaults from .tf files
    let tfvars = load_tfvars(dir)?;
    apply_tfvars(&mut workspace, &tfvars);

    // Apply TF_VAR_xxx environment variables (highest precedence)
    apply_env_vars(&mut workspace);

    Ok(workspace)
}

/// Load variable values from .tfvars files in the directory.
fn load_tfvars(dir: &Path) -> Result<HashMap<String, Expression>> {
    let mut values = HashMap::new();

    // Load terraform.tfvars, then terraform.tfvars.json, if present
    for name in ["terraform.tfvars", "terraform.tfvars.json"] {
        let default_tfvars = dir.join(name);
        if default_tfvars.exists() {
            tracing::info!("Loading {}", default_tfvars.display());
            let parsed = parse_any_tfvars_file(&default_tfvars)?;
            values.extend(parsed);
        }
    }

    // Load *.auto.tfvars and *.auto.tfvars.json files together, in
    // alphabetical order of filename (matches Terraform behavior)
    let entries = std::fs::read_dir(dir)?;
    let mut auto_files: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".auto.tfvars") || n.ends_with(".auto.tfvars.json"))
                .unwrap_or(false)
        })
        .collect();
    auto_files.sort();

    for file in &auto_files {
        tracing::info!("Loading {}", file.display());
        let parsed = parse_any_tfvars_file(file)?;
        values.extend(parsed);
    }

    Ok(values)
}

/// Parse a .tfvars or .tfvars.json file, dispatching on the extension.
fn parse_any_tfvars_file(path: &Path) -> Result<HashMap<String, Expression>> {
    if path.extension().map(|e| e == "json").unwrap_or(false) {
        parse_tfvars_json_file(path)
    } else {
        parse_tfvars_file(path)
    }
}

/// Parse a single .tfvars.json file into a map of variable name → Expression.
/// The root must be a JSON object; values are literals (no interpolation).
fn parse_tfvars_json_file(path: &Path) -> Result<HashMap<String, Expression>> {
    let content =
        std::fs::read_to_string(path).context(format!("Failed to read {}", path.display()))?;
    let root: serde_json::Value =
        serde_json::from_str(&content).context(format!("Failed to parse {}", path.display()))?;
    let serde_json::Value::Object(obj) = root else {
        anyhow::bail!(
            "Failed to parse {}: root must be a JSON object",
            path.display()
        );
    };

    Ok(obj
        .iter()
        .map(|(name, v)| (name.clone(), Expression::Literal(json_to_value(v))))
        .collect())
}

/// Convert a JSON value into a literal Value.
fn json_to_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => Value::Int(i),
            None => Value::Float(n.as_f64().unwrap_or_default()),
        },
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Map(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

/// Parse a single .tfvars file into a map of variable name → Expression.
/// .tfvars files are HCL-formatted key-value assignments.
fn parse_tfvars_file(path: &Path) -> Result<HashMap<String, Expression>> {
    let content =
        std::fs::read_to_string(path).context(format!("Failed to read {}", path.display()))?;
    let body: hcl::Body =
        hcl::from_str(&content).context(format!("Failed to parse {}", path.display()))?;

    let mut values = HashMap::new();
    for attr in body.attributes() {
        let name = attr.key().to_string();
        let expr = parser::hcl_expr_to_expression(attr.expr());
        values.insert(name, expr);
    }

    Ok(values)
}

/// Apply tfvars values to workspace variables by overriding their defaults.
fn apply_tfvars(workspace: &mut WorkspaceConfig, tfvars: &HashMap<String, Expression>) {
    for var in &mut workspace.variables {
        if let Some(value) = tfvars.get(&var.name) {
            var.default = Some(value.clone());
        }
    }
}

/// Apply TF_VAR_xxx environment variables to workspace variables.
fn apply_env_vars(workspace: &mut WorkspaceConfig) {
    for var in &mut workspace.variables {
        let env_key = format!("TF_VAR_{}", var.name);
        if let Ok(env_val) = std::env::var(&env_key) {
            var.default = Some(Expression::Literal(Value::String(env_val)));
        }
    }
}

/// Merge a partial workspace config into the main one.
fn merge_workspace(main: &mut WorkspaceConfig, partial: WorkspaceConfig) {
    main.providers.extend(partial.providers);
    main.resources.extend(partial.resources);
    main.data_sources.extend(partial.data_sources);
    main.modules.extend(partial.modules);
    main.variables.extend(partial.variables);
    main.outputs.extend(partial.outputs);
    main.locals.extend(partial.locals);

    if main.terraform_settings.is_none() {
        main.terraform_settings = partial.terraform_settings;
    } else if let Some(partial_tf) = partial.terraform_settings {
        if let Some(ref mut main_tf) = main.terraform_settings {
            main_tf
                .required_providers
                .extend(partial_tf.required_providers);
            if main_tf.required_version.is_none() {
                main_tf.required_version = partial_tf.required_version;
            }
            if main_tf.backend.is_none() {
                main_tf.backend = partial_tf.backend;
            }
        }
    }
}
