use std::collections::HashMap;

use anyhow::{bail, Result};
use petgraph::graph::{DiGraph, NodeIndex};

use crate::config::types::{Expression, ResourceConfig, ResourceIndex, WorkspaceConfig};
use crate::executor::engine::{eval_expression, EvalContext};

/// A node in the resource-level dependency graph.
#[derive(Debug, Clone)]
pub enum DagNode {
    Resource {
        address: String,
        base_address: String,
        resource_type: String,
        name: String,
        provider_source: String,
        config: ResourceConfig,
        index: Option<ResourceIndex>,
    },
    DataSource {
        address: String,
        base_address: String,
        resource_type: String,
        name: String,
        provider_source: String,
        config: ResourceConfig,
        index: Option<ResourceIndex>,
    },
    Output {
        name: String,
        module_path: String,
    },
}

impl DagNode {
    pub fn address(&self) -> &str {
        match self {
            DagNode::Resource { address, .. } => address,
            DagNode::DataSource { address, .. } => address,
            DagNode::Output { name, .. } => name,
        }
    }

    pub fn base_address(&self) -> &str {
        match self {
            DagNode::Resource { base_address, .. } => base_address,
            DagNode::DataSource { base_address, .. } => base_address,
            DagNode::Output { name, .. } => name,
        }
    }

    pub fn index(&self) -> Option<&ResourceIndex> {
        match self {
            DagNode::Resource { index, .. } => index.as_ref(),
            DagNode::DataSource { index, .. } => index.as_ref(),
            DagNode::Output { .. } => None,
        }
    }
}

/// The type of dependency between nodes.
#[derive(Debug, Clone)]
pub enum DependencyEdge {
    /// Explicitly declared via `depends_on`.
    Explicit,
    /// Inferred from expression references (e.g. `aws_vpc.main.id`).
    Implicit,
    /// Data source dependency (data sources run during planning).
    DataDependency,
    /// Provider dependency (provider must be configured first).
    ProviderDep,
}

/// A resource-level dependency graph.
pub type ResourceGraph = DiGraph<DagNode, DependencyEdge>;

/// Build a resource-level dependency graph from a WorkspaceConfig.
///
/// This replaces the module-level DAG with individual resource nodes,
/// enabling resource-level parallelism during plan and apply.
/// Resources with `count` or `for_each` are expanded into individual nodes.
pub fn build_resource_dag(
    workspace: &WorkspaceConfig,
    provider_map: &HashMap<String, String>,
    var_defaults: &HashMap<String, serde_json::Value>,
) -> Result<(ResourceGraph, HashMap<String, NodeIndex>)> {
    let mut graph = DiGraph::new();
    let mut node_map: HashMap<String, NodeIndex> = HashMap::new();
    // Maps base_address -> Vec<NodeIndex> for expanded resources
    let mut base_to_indices: HashMap<String, Vec<NodeIndex>> = HashMap::new();

    // Add all resources as nodes (expanding count/for_each)
    for resource in &workspace.resources {
        let base_address = format!("{}.{}", resource.resource_type, resource.name);
        let provider_source = resolve_provider_source(resource, provider_map);

        if let Some(count) = evaluate_count(resource, var_defaults)? {
            for i in 0..count {
                let address = format!("{}[{}]", base_address, i);
                let node = DagNode::Resource {
                    address: address.clone(),
                    base_address: base_address.clone(),
                    resource_type: resource.resource_type.clone(),
                    name: resource.name.clone(),
                    provider_source: provider_source.clone(),
                    config: resource.clone(),
                    index: Some(ResourceIndex::Count(i)),
                };
                let idx = graph.add_node(node);
                node_map.insert(address, idx);
                base_to_indices
                    .entry(base_address.clone())
                    .or_default()
                    .push(idx);
            }
        } else if let Some(keys) = evaluate_for_each(resource, var_defaults)? {
            for (key, value) in &keys {
                let address = format!("{}[\"{}\"]", base_address, key);
                let value_json = serde_json::to_string(value).unwrap_or_default();
                let node = DagNode::Resource {
                    address: address.clone(),
                    base_address: base_address.clone(),
                    resource_type: resource.resource_type.clone(),
                    name: resource.name.clone(),
                    provider_source: provider_source.clone(),
                    config: resource.clone(),
                    index: Some(ResourceIndex::ForEach(key.clone(), value_json)),
                };
                let idx = graph.add_node(node);
                node_map.insert(address, idx);
                base_to_indices
                    .entry(base_address.clone())
                    .or_default()
                    .push(idx);
            }
        } else {
            let node = DagNode::Resource {
                address: base_address.clone(),
                base_address: base_address.clone(),
                resource_type: resource.resource_type.clone(),
                name: resource.name.clone(),
                provider_source: provider_source.clone(),
                config: resource.clone(),
                index: None,
            };
            let idx = graph.add_node(node);
            node_map.insert(base_address.clone(), idx);
            base_to_indices
                .entry(base_address.clone())
                .or_default()
                .push(idx);
        }
    }

    // Add all data sources as nodes (expanding count/for_each)
    for data_source in &workspace.data_sources {
        let base_address = format!("data.{}.{}", data_source.resource_type, data_source.name);
        let provider_source = resolve_provider_source(data_source, provider_map);

        // Data sources rarely use count, but support it
        let node = DagNode::DataSource {
            address: base_address.clone(),
            base_address: base_address.clone(),
            resource_type: data_source.resource_type.clone(),
            name: data_source.name.clone(),
            provider_source,
            config: data_source.clone(),
            index: None,
        };
        let idx = graph.add_node(node);
        node_map.insert(base_address.clone(), idx);
        base_to_indices
            .entry(base_address.clone())
            .or_default()
            .push(idx);
    }

    // Add output nodes
    for output in &workspace.outputs {
        let node = DagNode::Output {
            name: output.name.clone(),
            module_path: String::new(),
        };
        let idx = graph.add_node(node);
        node_map.insert(format!("output.{}", output.name), idx);
    }

    // Add explicit and implicit dependencies
    for resource in workspace
        .resources
        .iter()
        .chain(workspace.data_sources.iter())
    {
        let is_data = workspace
            .data_sources
            .iter()
            .any(|d| d.resource_type == resource.resource_type && d.name == resource.name);
        let base_address = if is_data {
            format!("data.{}.{}", resource.resource_type, resource.name)
        } else {
            format!("{}.{}", resource.resource_type, resource.name)
        };

        // Get all node indices for this resource (may be multiple if count/for_each expanded)
        let to_indices: Vec<NodeIndex> = base_to_indices
            .get(&base_address)
            .cloned()
            .unwrap_or_default();

        if to_indices.is_empty() {
            continue;
        }

        // Explicit depends_on
        for dep in &resource.depends_on {
            let from_indices = resolve_dep_indices(dep, &node_map, &base_to_indices);
            for &from_idx in &from_indices {
                for &to_idx in &to_indices {
                    if from_idx != to_idx {
                        graph.add_edge(from_idx, to_idx, DependencyEdge::Explicit);
                    }
                }
            }
        }

        // Implicit dependencies from expressions
        let refs = extract_references_from_attributes(&resource.attributes);
        for ref_address in &refs {
            let from_indices = resolve_dep_indices(ref_address, &node_map, &base_to_indices);
            for &from_idx in &from_indices {
                for &to_idx in &to_indices {
                    if from_idx != to_idx {
                        graph.add_edge(from_idx, to_idx, DependencyEdge::Implicit);
                    }
                }
            }
        }
    }

    // Add output dependencies
    for output in &workspace.outputs {
        let output_address = format!("output.{}", output.name);
        if let Some(&to_idx) = node_map.get(&output_address) {
            for dep in &output.depends_on {
                let from_indices = resolve_dep_indices(dep, &node_map, &base_to_indices);
                for &from_idx in &from_indices {
                    graph.add_edge(from_idx, to_idx, DependencyEdge::Explicit);
                }
            }

            let refs = extract_references_from_expression(&output.value);
            for ref_address in &refs {
                let from_indices = resolve_dep_indices(ref_address, &node_map, &base_to_indices);
                for &from_idx in &from_indices {
                    graph.add_edge(from_idx, to_idx, DependencyEdge::Implicit);
                }
            }
        }
    }

    // Verify no cycles
    if petgraph::algo::is_cyclic_directed(&graph) {
        bail!("Circular dependency detected in resource graph");
    }

    Ok((graph, node_map))
}

/// Resolve a dependency address to node indices. Tries exact match first, then base_address.
fn resolve_dep_indices(
    dep: &str,
    node_map: &HashMap<String, NodeIndex>,
    base_to_indices: &HashMap<String, Vec<NodeIndex>>,
) -> Vec<NodeIndex> {
    // Exact match (e.g. "aws_vpc.main" or "aws_instance.main[0]")
    if let Some(&idx) = node_map.get(dep) {
        return vec![idx];
    }
    // Base address match (e.g. "aws_instance.main" resolves to all expanded instances)
    if let Some(indices) = base_to_indices.get(dep) {
        return indices.clone();
    }
    vec![]
}

/// Evaluate the count expression and return the count, or None if no count is set.
fn evaluate_count(
    resource: &ResourceConfig,
    var_defaults: &HashMap<String, serde_json::Value>,
) -> Result<Option<usize>> {
    let Some(ref count_expr) = resource.count else {
        return Ok(None);
    };
    let ctx = EvalContext::plan_only(var_defaults.clone());
    let val = eval_expression(count_expr, &ctx);
    match val {
        serde_json::Value::Number(n) => {
            let count = n.as_u64().ok_or_else(|| {
                anyhow::anyhow!(
                    "count must be a non-negative integer for {}.{}, got {}",
                    resource.resource_type,
                    resource.name,
                    n
                )
            })?;
            Ok(Some(count as usize))
        }
        serde_json::Value::Null => {
            bail!(
                "Cannot determine count for {}.{}: count expression resolved to null (missing variable?)",
                resource.resource_type, resource.name
            );
        }
        _ => bail!(
            "count for {}.{} must evaluate to a number, got {:?}",
            resource.resource_type,
            resource.name,
            val
        ),
    }
}

/// Evaluate the for_each expression and return key-value pairs, or None if not set.
fn evaluate_for_each(
    resource: &ResourceConfig,
    var_defaults: &HashMap<String, serde_json::Value>,
) -> Result<Option<Vec<(String, serde_json::Value)>>> {
    let Some(ref for_each_expr) = resource.for_each else {
        return Ok(None);
    };
    let ctx = EvalContext::plan_only(var_defaults.clone());
    let val = eval_expression(for_each_expr, &ctx);
    match val {
        serde_json::Value::Object(map) => Ok(Some(map.into_iter().collect())),
        serde_json::Value::Array(arr) => Ok(Some(
            arr.into_iter()
                .map(|v| {
                    let key = match &v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (key, v)
                })
                .collect(),
        )),
        serde_json::Value::Null | serde_json::Value::String(_) => {
            // for_each references another resource (e.g. `for_each = aws_subnet.public`)
            // which can't be resolved at DAG-build time. Treat as a single unindexed node;
            // the dependency is still tracked via expression references.
            tracing::debug!(
                "for_each for {}.{} resolved to {:?} (likely a resource reference), treating as single node",
                resource.resource_type, resource.name, val
            );
            Ok(None)
        }
        _ => bail!(
            "for_each for {}.{} must evaluate to a map or set, got {:?}",
            resource.resource_type,
            resource.name,
            val
        ),
    }
}

/// Extract resource references from a map of attributes.
fn extract_references_from_attributes(attrs: &HashMap<String, Expression>) -> Vec<String> {
    let mut refs = Vec::new();
    for expr in attrs.values() {
        refs.extend(extract_references_from_expression(expr));
    }
    refs
}

/// Extract resource addresses referenced in an expression.
/// e.g. Reference(["aws_vpc", "main", "id"]) → "aws_vpc.main"
fn extract_references_from_expression(expr: &Expression) -> Vec<String> {
    let mut refs = Vec::new();
    collect_references(expr, &mut refs);
    refs
}

fn collect_references(expr: &Expression, refs: &mut Vec<String>) {
    match expr {
        Expression::Reference(parts) => {
            if parts.len() >= 2 {
                let first = &parts[0];
                // Skip "var.", "local.", "each.", "count.", "path.", "terraform."
                match first.as_str() {
                    "var" | "local" | "each" | "count" | "path" | "terraform" | "self" => {}
                    "data" if parts.len() >= 3 => {
                        refs.push(format!("data.{}.{}", parts[1], parts[2]));
                    }
                    "module" if parts.len() >= 2 => {
                        // Module references are tracked but don't resolve to
                        // individual resource addresses (modules are opaque).
                    }
                    _ => {
                        // resource_type.name pattern
                        refs.push(format!("{}.{}", parts[0], parts[1]));
                    }
                }
            }
        }
        Expression::Literal(val) => {
            // Scan literal values for embedded ${...} references (from nested blocks)
            collect_references_from_value(val, refs);
        }
        Expression::FunctionCall { args, .. } => {
            for arg in args {
                collect_references(arg, refs);
            }
        }
        Expression::Conditional {
            condition,
            true_val,
            false_val,
        } => {
            collect_references(condition, refs);
            collect_references(true_val, refs);
            collect_references(false_val, refs);
        }
        Expression::ForExpr {
            collection,
            key_expr,
            value_expr,
            condition,
            ..
        } => {
            collect_references(collection, refs);
            if let Some(k) = key_expr {
                collect_references(k, refs);
            }
            collect_references(value_expr, refs);
            if let Some(c) = condition {
                collect_references(c, refs);
            }
        }
        Expression::Template(parts) => {
            for part in parts {
                match part {
                    crate::config::types::TemplatePart::Interpolation(e) => {
                        collect_references(e, refs);
                    }
                    crate::config::types::TemplatePart::Directive(e) => {
                        collect_references(e, refs);
                    }
                    crate::config::types::TemplatePart::Literal(_) => {}
                }
            }
        }
        Expression::Index { collection, key } => {
            collect_references(collection, refs);
            collect_references(key, refs);
        }
        Expression::GetAttr { object, .. } => {
            collect_references(object, refs);
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_references(left, refs);
            collect_references(right, refs);
        }
        Expression::UnaryOp { operand, .. } => {
            collect_references(operand, refs);
        }
        Expression::Splat { source, each } => {
            collect_references(source, refs);
            collect_references(each, refs);
        }
    }
}

/// Extract resource references from `${...}` interpolation strings embedded in literal values.
/// This handles nested blocks (like `route { gateway_id = aws_internet_gateway.main.id }`)
/// which get parsed as Literal values containing `${type.name.attr}` strings.
fn collect_references_from_value(val: &crate::config::types::Value, refs: &mut Vec<String>) {
    use crate::config::types::Value;
    match val {
        Value::String(s) => {
            // Scan for ${...} patterns
            let mut remaining = s.as_str();
            while let Some(start) = remaining.find("${") {
                if let Some(end) = remaining[start + 2..].find('}') {
                    let ref_str = &remaining[start + 2..start + 2 + end];
                    // Extract resource references from the interpolation.
                    // Handle function calls by scanning for resource-like refs.
                    extract_refs_from_interpolation(ref_str, refs);
                    remaining = &remaining[start + 2 + end + 1..];
                } else {
                    break;
                }
            }
        }
        Value::List(items) => {
            for item in items {
                collect_references_from_value(item, refs);
            }
        }
        Value::Map(entries) => {
            for (_, v) in entries {
                collect_references_from_value(v, refs);
            }
        }
        _ => {}
    }
}

/// Extract resource references from an interpolation string.
/// Handles both simple refs like `aws_vpc.main.id` and function calls
/// like `concat(aws_subnet.private[*].id, aws_subnet.public[*].id)`.
fn extract_refs_from_interpolation(s: &str, refs: &mut Vec<String>) {
    // Split on word boundaries to find resource-like references (type.name patterns).
    // We look for sequences of word chars separated by dots that look like resource addresses.
    let skip = [
        "var",
        "local",
        "each",
        "count",
        "path",
        "terraform",
        "self",
        "null",
        "true",
        "false",
    ];

    // Tokenize: split on anything that isn't a word char, dot, or bracket content
    // Simple approach: split on commas, parens, spaces, then process each token
    for token in s.split([',', '(', ')', ' ']) {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        // Strip leading/trailing brackets for splat-like tokens
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() >= 2 {
            // Strip any [.*] from the resource name part
            let type_name = parts[0].split('[').next().unwrap_or(parts[0]);
            let name = parts[1].split('[').next().unwrap_or(parts[1]);
            if type_name.is_empty() || name.is_empty() {
                continue;
            }
            match type_name {
                t if skip.contains(&t) => {}
                "data" if parts.len() >= 3 => {
                    let data_name = parts[2].split('[').next().unwrap_or(parts[2]);
                    refs.push(format!("data.{}.{}", name, data_name));
                }
                _ => {
                    // Looks like a resource reference
                    if type_name.contains('_') || parts.len() >= 3 {
                        refs.push(format!("{}.{}", type_name, name));
                    }
                }
            }
        }
    }
}

/// Resolve the provider source for a resource.
/// Uses `provider_ref` if set, otherwise derives from resource type prefix.
fn resolve_provider_source(
    resource: &ResourceConfig,
    provider_map: &HashMap<String, String>,
) -> String {
    if let Some(ref provider_ref) = resource.provider_ref {
        // Strip alias: "aws.west" → "aws"
        let base = provider_ref.split('.').next().unwrap_or(provider_ref);
        provider_map
            .get(base)
            .cloned()
            .unwrap_or_else(|| format!("hashicorp/{}", base))
    } else {
        // Derive from resource type: "aws_vpc" → "aws"
        let prefix = resource
            .resource_type
            .split('_')
            .next()
            .unwrap_or(&resource.resource_type);
        provider_map
            .get(prefix)
            .cloned()
            .unwrap_or_else(|| format!("hashicorp/{}", prefix))
    }
}

/// Get a topological ordering of the graph (dependencies before dependents).
pub fn topological_order(graph: &ResourceGraph) -> Result<Vec<NodeIndex>> {
    petgraph::algo::toposort(graph, None).map_err(|cycle| {
        anyhow::anyhow!(
            "Cycle detected involving {:?}",
            graph[cycle.node_id()].address()
        )
    })
}

/// Get the reverse topological ordering (for destroy operations).
pub fn reverse_topological_order(graph: &ResourceGraph) -> Result<Vec<NodeIndex>> {
    let mut order = topological_order(graph)?;
    order.reverse();
    Ok(order)
}

/// Generate DOT representation of the resource graph.
pub fn to_dot(graph: &ResourceGraph) -> String {
    let mut dot = String::from("digraph resources {\n");
    dot.push_str("  rankdir=TB;\n");
    dot.push_str("  node [shape=box, style=filled];\n\n");

    for idx in graph.node_indices() {
        let node = &graph[idx];
        let (label, color) = match node {
            DagNode::Resource {
                address,
                resource_type,
                ..
            } => (format!("{}\\n{}", address, resource_type), "#a8d8a8"),
            DagNode::DataSource {
                address,
                resource_type,
                ..
            } => (format!("data.{}\\n{}", address, resource_type), "#a8c8d8"),
            DagNode::Output { name, .. } => (format!("output.{}", name), "#d8d8a8"),
        };
        dot.push_str(&format!(
            "  n{} [label=\"{}\", fillcolor=\"{}\"];\n",
            idx.index(),
            label,
            color
        ));
    }

    dot.push('\n');

    for edge in graph.edge_indices() {
        if let Some((from, to)) = graph.edge_endpoints(edge) {
            let style = match &graph[edge] {
                DependencyEdge::Explicit => "solid",
                DependencyEdge::Implicit => "dashed",
                DependencyEdge::DataDependency => "dotted",
                DependencyEdge::ProviderDep => "bold",
            };
            dot.push_str(&format!(
                "  n{} -> n{} [style={}];\n",
                from.index(),
                to.index(),
                style
            ));
        }
    }

    dot.push_str("}\n");
    dot
}
