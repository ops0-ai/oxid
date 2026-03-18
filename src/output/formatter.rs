use colored::Colorize;

use crate::executor::engine::{PlanSummary, PlannedChange, ResourceAction};
use crate::state::models::ResourceState;

/// Print a success message.
pub fn print_success(msg: &str) {
    println!("{} {}", "✓".green().bold(), msg.green());
}

/// Print an error message.
pub fn print_error(msg: &str) {
    println!("{} {}", "✗".red().bold(), msg.red());
}

/// Print a resource-level execution plan in a Terraform-like format.
pub fn print_resource_plan(plan: &PlanSummary, _targets: &[String]) {
    println!();

    if plan.changes.is_empty() && plan.outputs.is_empty() {
        println!("{}", "No changes. Infrastructure is up-to-date.".green());
        return;
    }

    // Check if there are any actionable changes (engine already filtered by target)
    let actionable: Vec<&PlannedChange> = plan
        .changes
        .iter()
        .filter(|c| c.action != ResourceAction::NoOp)
        .collect();

    if !actionable.is_empty() {
        // Legend
        println!("Oxid used the selected providers to generate the following execution plan.");
        println!("Resource actions are indicated with the following symbols:");

        let has_creates = actionable
            .iter()
            .any(|c| c.action == ResourceAction::Create);
        let has_updates = actionable
            .iter()
            .any(|c| c.action == ResourceAction::Update);
        let has_deletes = actionable
            .iter()
            .any(|c| c.action == ResourceAction::Delete);
        let has_replaces = actionable
            .iter()
            .any(|c| c.action == ResourceAction::Replace);
        let has_reads = actionable.iter().any(|c| c.action == ResourceAction::Read);

        if has_creates {
            println!("  {} create", "+".green().bold());
        }
        if has_updates {
            println!("  {} update in-place", "~".yellow().bold());
        }
        if has_replaces {
            println!(
                "  {} destroy and then create replacement",
                "-/+".magenta().bold()
            );
        }
        if has_deletes {
            println!("  {} destroy", "-".red().bold());
        }
        if has_reads {
            println!(" {} read (data resources)", "<=".cyan().bold());
        }

        println!();
        println!("Oxid will perform the following actions:");
        println!();

        // Print each resource
        for change in &actionable {
            print_resource_change(change);
        }
    }

    // Print summary
    println!("{}", plan);
    println!();

    // Print output changes
    if !plan.outputs.is_empty() {
        println!("Changes to Outputs:");
        let name_width = plan
            .outputs
            .iter()
            .map(|o| o.name.len())
            .max()
            .unwrap_or(10);

        for output in &plan.outputs {
            let (icon, color_fn): (&str, fn(&str) -> colored::ColoredString) = match output.action {
                ResourceAction::Create => ("+", |s: &str| s.green()),
                ResourceAction::Update => ("~", |s: &str| s.yellow()),
                ResourceAction::Delete => ("-", |s: &str| s.red()),
                _ => ("+", |s: &str| s.green()),
            };
            let value_display = "(known after apply)".to_string();
            let line = format!(
                "  {} {:<width$} = {}",
                icon,
                output.name,
                value_display,
                width = name_width,
            );
            println!("{}", color_fn(&line));
        }
        println!();
    }
}

/// Print a single resource change with its attributes.
fn print_resource_change(change: &PlannedChange) {
    let (icon, color_fn): (&str, fn(&str) -> colored::ColoredString) = match change.action {
        ResourceAction::Create => ("+", |s: &str| s.green()),
        ResourceAction::Update => ("~", |s: &str| s.yellow()),
        ResourceAction::Delete => ("-", |s: &str| s.red()),
        ResourceAction::Replace => ("-/+", |s: &str| s.magenta()),
        ResourceAction::Read => ("<=", |s: &str| s.cyan()),
        ResourceAction::NoOp => return,
    };

    let action_desc = match change.action {
        ResourceAction::Create => "will be created",
        ResourceAction::Update => "will be updated in-place",
        ResourceAction::Delete => "will be destroyed",
        ResourceAction::Replace => "must be replaced",
        ResourceAction::Read => "will be read during apply",
        ResourceAction::NoOp => return,
    };

    // Header: # aws_vpc.main will be created
    println!(
        "  {} {} {}",
        "#".dimmed(),
        change.address.bold(),
        action_desc.dimmed()
    );

    // Resource block: + resource "aws_vpc" "main" {
    let is_data = change.address.starts_with("data.");
    let (block_type, res_type, res_name) = if is_data {
        // data.aws_ami.amazon_linux → data "aws_ami" "amazon_linux"
        let stripped = change
            .address
            .strip_prefix("data.")
            .unwrap_or(&change.address);
        let parts: Vec<&str> = stripped.splitn(2, '.').collect();
        if parts.len() == 2 {
            ("data", parts[0], parts[1])
        } else {
            ("data", change.resource_type.as_str(), stripped)
        }
    } else {
        let parts: Vec<&str> = change.address.splitn(2, '.').collect();
        if parts.len() == 2 {
            ("resource", parts[0], parts[1])
        } else {
            (
                "resource",
                change.resource_type.as_str(),
                change.address.as_str(),
            )
        }
    };
    let header = format!(
        "  {} {} \"{}\" \"{}\" {{",
        icon, block_type, res_type, res_name
    );
    println!("{}", color_fn(&header));

    // Collect user-specified keys for identification
    let user_keys: std::collections::HashSet<String> = change
        .user_config
        .as_ref()
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.keys()
                .filter(|k| {
                    let v = &obj[k.as_str()];
                    !v.is_null()
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    // Print attributes from planned state
    if let Some(ref planned) = change.planned_state {
        if let Some(obj) = planned.as_object() {
            let prior_obj = change.prior_state.as_ref().and_then(|v| v.as_object());

            // Sort keys: user-specified first, then alphabetical
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort_by(|a, b| {
                let a_user = user_keys.contains(a.as_str());
                let b_user = user_keys.contains(b.as_str());
                match (a_user, b_user) {
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    _ => a.cmp(b),
                }
            });

            // Find max key length for alignment
            let max_key_len = keys.iter().map(|k| k.len()).max().unwrap_or(0).min(50);

            for key in &keys {
                let value = &obj[key.as_str()];

                // Skip null values that aren't user-specified (reduce noise)
                if value.is_null() && !user_keys.contains(key.as_str()) {
                    continue;
                }

                // Skip very large nested objects/arrays unless user-specified
                if !user_keys.contains(key.as_str()) {
                    match value {
                        serde_json::Value::Object(m) if m.len() > 8 => continue,
                        serde_json::Value::Array(a) if a.len() > 10 => continue,
                        _ => {}
                    }
                }

                let is_user_set = user_keys.contains(key.as_str());
                let prior_value = prior_obj.and_then(|p| p.get(key.as_str()));

                let display_val = format_plan_value(value, is_user_set, prior_value);

                // Show change marker for updates
                let attr_icon = match change.action {
                    ResourceAction::Update => {
                        if prior_value.map(|p| p != value).unwrap_or(true) && is_user_set {
                            "~"
                        } else {
                            " "
                        }
                    }
                    ResourceAction::Replace => {
                        if change.requires_replace.contains(&key.to_string()) {
                            "#" // forces replacement
                        } else {
                            " "
                        }
                    }
                    _ => "+",
                };

                let line = format!(
                    "      {} {:<width$} = {}",
                    attr_icon,
                    key,
                    display_val,
                    width = max_key_len
                );
                println!("{}", color_fn(&line));
            }
        }
    } else if change.action == ResourceAction::Create || change.action == ResourceAction::Replace {
        // No planned state yet — show user config
        if let Some(ref config) = change.user_config {
            if let Some(obj) = config.as_object() {
                let max_key_len = obj.keys().map(|k| k.len()).max().unwrap_or(0).min(50);
                for (key, value) in obj {
                    if value.is_null() {
                        continue;
                    }
                    let display_val = format_value_short(value);
                    let line = format!(
                        "      + {:<width$} = {}",
                        key,
                        display_val,
                        width = max_key_len
                    );
                    println!("{}", color_fn(&line));
                }
            }
        }
    }

    let closing = format!(
        "  {} }}",
        if change.action == ResourceAction::Delete {
            "-"
        } else {
            " "
        }
    );
    println!("{}", color_fn(&closing));
    println!();
}

/// Format a value for the plan display.
fn format_plan_value(
    value: &serde_json::Value,
    is_user_set: bool,
    _prior_value: Option<&serde_json::Value>,
) -> String {
    if is_user_set {
        format_value_short(value)
    } else if value.is_null() {
        "(known after apply)".dimmed().to_string()
    } else {
        format_value_short(value)
    }
}

/// Format a JSON value for short inline display.
fn format_value_short(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => format!("\"{}\"", s),
        serde_json::Value::Null => "(known after apply)".dimmed().to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                "[]".to_string()
            } else if arr.len() <= 4
                && arr
                    .iter()
                    .all(|v| matches!(v, serde_json::Value::String(_)))
            {
                let items: Vec<String> = arr.iter().map(format_value_short).collect();
                format!("[{}]", items.join(", "))
            } else {
                format!("[...{} items]", arr.len())
            }
        }
        serde_json::Value::Object(obj) => {
            if obj.is_empty() {
                "{}".to_string()
            } else {
                // Multi-line block style like Terraform
                let mut lines = Vec::new();
                let max_key_len = obj.keys().map(|k| k.len()).max().unwrap_or(0);
                lines.push("{".to_string());
                for (k, v) in obj {
                    lines.push(format!(
                        "          + {:<width$} = {}",
                        format!("\"{}\"", k),
                        format_value_short(v),
                        width = max_key_len + 2,
                    ));
                }
                lines.push("        }".to_string());
                lines.join("\n")
            }
        }
    }
}

/// Format an output value for display after apply, matching Terraform's style.
pub fn format_output_value(value: &serde_json::Value, indent: usize) -> String {
    let pad = "  ".repeat(indent);
    match value {
        serde_json::Value::String(s) => format!("\"{}\"", s),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return "[]".to_string();
            }
            let mut lines = vec!["[".to_string()];
            for item in arr {
                lines.push(format!(
                    "{}  {},",
                    pad,
                    format_output_value(item, indent + 1)
                ));
            }
            lines.push(format!("{}]", pad));
            lines.join("\n")
        }
        serde_json::Value::Object(obj) => {
            if obj.is_empty() {
                return "{}".to_string();
            }
            let mut lines = vec!["{".to_string()];
            let key_width = obj.keys().map(|k| k.len()).max().unwrap_or(0);
            for (k, v) in obj {
                lines.push(format!(
                    "{}  {:<width$} = {}",
                    pad,
                    format!("\"{}\"", k),
                    format_output_value(v, indent + 1),
                    width = key_width + 2,
                ));
            }
            lines.push(format!("{}}}", pad));
            lines.join("\n")
        }
    }
}

/// Convert a ResourceAction to Terraform-compatible action strings.
fn action_to_tf_actions(action: &ResourceAction) -> Vec<&'static str> {
    match action {
        ResourceAction::Create => vec!["create"],
        ResourceAction::Update => vec!["update"],
        ResourceAction::Delete => vec!["delete"],
        ResourceAction::Replace => vec!["delete", "create"],
        ResourceAction::Read => vec!["read"],
        ResourceAction::NoOp => vec!["no-op"],
    }
}

/// Build `after_unknown` — marks fields in planned_state that are unknown (null in planned
/// but not explicitly set by user config).
fn build_after_unknown(
    planned: Option<&serde_json::Value>,
    user_config: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(serde_json::Value::Object(planned_map)) = planned else {
        return serde_json::json!({});
    };
    let user_map = user_config.and_then(|v| v.as_object());

    let mut unknown = serde_json::Map::new();
    for (key, value) in planned_map {
        // If the planned value contains an "unknown" marker or is null but not in user config,
        // mark it as unknown
        let in_user_config = user_map.map(|m| m.contains_key(key)).unwrap_or(false);
        if value.is_null() && !in_user_config {
            unknown.insert(key.clone(), serde_json::Value::Bool(true));
        } else if let serde_json::Value::Object(_) = value {
            // Recurse for nested objects
            let nested = build_after_unknown(Some(value), user_config.and_then(|u| u.get(key)));
            if nested.as_object().map(|m| !m.is_empty()).unwrap_or(false) {
                unknown.insert(key.clone(), nested);
            }
        }
    }
    serde_json::Value::Object(unknown)
}

/// Parse address into (type, name) — e.g. "aws_vpc.main" → ("aws_vpc", "main")
fn parse_address(address: &str) -> (&str, &str) {
    // Handle data sources: "data.aws_ami.name" → type="aws_ami", name="name"
    let addr = address.strip_prefix("data.").unwrap_or(address);
    if let Some(dot) = addr.find('.') {
        (&addr[..dot], &addr[dot + 1..])
    } else {
        (addr, "")
    }
}

/// Normalize provider source to full registry path.
/// e.g. "hashicorp/aws" → "registry.terraform.io/hashicorp/aws"
fn normalize_provider_name(source: &str) -> String {
    if source.contains('/') && !source.contains("registry.terraform.io") {
        format!("registry.terraform.io/{}", source)
    } else {
        source.to_string()
    }
}

/// Print the plan as Terraform-compatible JSON (matches `terraform show -json tfplan`).
pub fn print_plan_json(plan: &PlanSummary) {
    let version = env!("CARGO_PKG_VERSION");

    // Build resource_changes array (Terraform format)
    let resource_changes: Vec<serde_json::Value> = plan
        .changes
        .iter()
        .filter(|c| c.action != ResourceAction::Read || c.address.starts_with("data."))
        .map(|c| {
            let (resource_type, name) = parse_address(&c.address);
            let mode = if c.address.starts_with("data.") {
                "data"
            } else {
                "managed"
            };
            let actions = action_to_tf_actions(&c.action);
            let provider_name = normalize_provider_name(&c.provider_source);

            let before_sensitive = if c.prior_state.is_none() {
                serde_json::json!(false)
            } else {
                build_sensitive_values(c.prior_state.as_ref())
            };
            let after_sensitive = if c.planned_state.is_none() {
                serde_json::json!(false)
            } else {
                build_sensitive_values(c.planned_state.as_ref())
            };

            let mut entry = serde_json::json!({
                "address": c.address,
                "mode": mode,
                "type": resource_type,
                "name": name,
                "provider_name": provider_name,
                "change": {
                    "actions": actions,
                    "before": c.prior_state,
                    "after": c.planned_state,
                    "after_unknown": build_after_unknown(
                        c.planned_state.as_ref(),
                        c.user_config.as_ref(),
                    ),
                    "before_sensitive": before_sensitive,
                    "after_sensitive": after_sensitive,
                }
            });
            if let Some(ref idx) = c.index {
                entry["index"] = idx.clone();
            }
            entry
        })
        .collect();

    // Build planned_values.root_module.resources
    let planned_resources: Vec<serde_json::Value> = plan
        .changes
        .iter()
        .filter(|c| {
            !c.address.starts_with("data.")
                && matches!(
                    c.action,
                    ResourceAction::Create
                        | ResourceAction::Update
                        | ResourceAction::Replace
                        | ResourceAction::NoOp
                )
        })
        .map(|c| {
            let (resource_type, name) = parse_address(&c.address);
            let provider_name = normalize_provider_name(&c.provider_source);
            let mut entry = serde_json::json!({
                "address": c.address,
                "mode": "managed",
                "type": resource_type,
                "name": name,
                "provider_name": provider_name,
                "schema_version": 0,
                "values": c.planned_state,
                "sensitive_values": build_sensitive_values(c.planned_state.as_ref()),
            });
            if let Some(ref idx) = c.index {
                entry["index"] = idx.clone();
            }
            entry
        })
        .collect();

    // Build planned_values.outputs (Terraform: just {"sensitive": false} per output)
    let mut planned_outputs = serde_json::Map::new();
    for output in &plan.outputs {
        planned_outputs.insert(
            output.name.clone(),
            serde_json::json!({
                "sensitive": false,
            }),
        );
    }

    // Build output_changes (Terraform format: map of name → change object)
    let mut output_changes = serde_json::Map::new();
    for output in &plan.outputs {
        let actions = action_to_tf_actions(&output.action);
        output_changes.insert(
            output.name.clone(),
            serde_json::json!({
                "actions": actions,
                "before": null,
                "after": null,
                "after_unknown": !output.value_known,
                "after_sensitive": false,
                "before_sensitive": false,
            }),
        );
    }

    let json = serde_json::json!({
        "format_version": "1.2",
        "terraform_version": format!("oxid-{}", version),
        "applyable": plan.creates > 0 || plan.updates > 0 || plan.deletes > 0 || plan.replaces > 0,
        "complete": true,
        "errored": false,
        "variables": plan.variables,
        "planned_values": {
            "outputs": planned_outputs,
            "root_module": {
                "resources": planned_resources,
            }
        },
        "resource_changes": resource_changes,
        "output_changes": output_changes,
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&json).unwrap_or_default()
    );
}

/// Build sensitive_values structure matching Terraform format.
/// Objects get `{}` for each nested object, arrays get `[]` or `[false, ...]` etc.
fn build_sensitive_values(state: Option<&serde_json::Value>) -> serde_json::Value {
    match state {
        Some(serde_json::Value::Object(obj)) => {
            let mut result = serde_json::Map::new();
            for (key, value) in obj {
                match value {
                    serde_json::Value::Object(_) => {
                        result.insert(key.clone(), build_sensitive_values(Some(value)));
                    }
                    serde_json::Value::Array(arr) => {
                        let inner: Vec<serde_json::Value> = arr
                            .iter()
                            .map(|v| match v {
                                serde_json::Value::Object(_) => build_sensitive_values(Some(v)),
                                _ => serde_json::json!(false),
                            })
                            .collect();
                        result.insert(key.clone(), serde_json::Value::Array(inner));
                    }
                    _ => {} // scalar values not included in sensitive_values
                }
            }
            serde_json::Value::Object(result)
        }
        _ => serde_json::json!({}),
    }
}

/// Print a list of resources from state.
pub fn print_resource_list(resources: &[ResourceState]) {
    if resources.is_empty() {
        println!("{}", "No resources in state.".dimmed());
        return;
    }

    println!();
    println!("{}", "Resources".bold().cyan());
    println!("{}", "─".repeat(80));
    println!(
        "  {:<35} {:<20} {:<12} {}",
        "ADDRESS".bold(),
        "TYPE".bold(),
        "STATUS".bold(),
        "PROVIDER".bold()
    );
    println!("{}", "─".repeat(80));

    for resource in resources {
        let status_colored = match resource.status.as_str() {
            "created" => resource.status.green().to_string(),
            "failed" => resource.status.red().to_string(),
            "tainted" => resource.status.yellow().to_string(),
            "deleted" => resource.status.dimmed().to_string(),
            "planned" => resource.status.blue().to_string(),
            _ => resource.status.clone(),
        };

        let provider_short = resource
            .provider_source
            .split('/')
            .next_back()
            .unwrap_or(&resource.provider_source);

        println!(
            "  {:<35} {:<20} {:<12} {}",
            resource.address,
            resource.resource_type,
            status_colored,
            provider_short.dimmed()
        );
    }

    println!();
    println!("  {} resource(s) total.", resources.len());
    println!();
}

/// Print detailed resource state.
pub fn print_resource_detail(resource: &ResourceState) {
    println!();
    println!("{} {}", "Resource:".bold().cyan(), resource.address.bold());
    println!("{}", "─".repeat(60));
    println!("  {:<18} {}", "Type:".bold(), resource.resource_type);
    println!("  {:<18} {}", "Name:".bold(), resource.resource_name);
    println!("  {:<18} {}", "Provider:".bold(), resource.provider_source);
    println!("  {:<18} {}", "Mode:".bold(), resource.resource_mode);

    let status_colored = match resource.status.as_str() {
        "created" => resource.status.green().to_string(),
        "failed" => resource.status.red().to_string(),
        "tainted" => resource.status.yellow().to_string(),
        _ => resource.status.clone(),
    };
    println!("  {:<18} {}", "Status:".bold(), status_colored);

    if !resource.module_path.is_empty() {
        println!("  {:<18} {}", "Module:".bold(), resource.module_path);
    }

    if let Some(ref idx) = resource.index_key {
        println!("  {:<18} {}", "Index:".bold(), idx);
    }

    println!(
        "  {:<18} {}",
        "Schema Version:".bold(),
        resource.schema_version
    );
    println!("  {:<18} {}", "Created:".bold(), resource.created_at);
    println!("  {:<18} {}", "Updated:".bold(), resource.updated_at);

    // Print attributes
    if resource.attributes_json != "{}" && !resource.attributes_json.is_empty() {
        println!();
        println!("  {}:", "Attributes".bold());

        if let Ok(attrs) = serde_json::from_str::<serde_json::Value>(&resource.attributes_json) {
            if let Some(obj) = attrs.as_object() {
                let sensitive: std::collections::HashSet<&str> = resource
                    .sensitive_attrs
                    .iter()
                    .map(|s| s.as_str())
                    .collect();

                for (key, value) in obj {
                    let display_value = if sensitive.contains(key.as_str()) {
                        "(sensitive)".dimmed().to_string()
                    } else {
                        format_value_short(value)
                    };
                    println!("    {:<20} = {}", key, display_value);
                }
            }
        }
    }

    println!("{}", "─".repeat(60));
    println!();
}
