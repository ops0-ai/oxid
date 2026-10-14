use std::collections::HashSet;

use oxid::dag::resource_graph::{build_resource_dag, to_dot, to_dot_with_modules, ResourceGraph};
use oxid::executor::engine::{build_provider_map, build_variable_defaults};
use oxid::hcl::parse_directory;

// ─── Helper ──────────────────────────────────────────────────────────────────

/// Parse `main.tf` and return the graph's edges as (from, to) addresses.
fn edges(tf: &str) -> (ResourceGraph, HashSet<(String, String)>) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("main.tf"), tf).unwrap();
    let ws = parse_directory(dir.path()).unwrap();
    let (graph, _) =
        build_resource_dag(&ws, &build_provider_map(&ws), &build_variable_defaults(&ws)).unwrap();

    let edges = graph
        .edge_indices()
        .map(|e| {
            let (from, to) = graph.edge_endpoints(e).unwrap();
            (
                graph[from].address().to_string(),
                graph[to].address().to_string(),
            )
        })
        .collect();
    (graph, edges)
}

fn has_edge(edges: &HashSet<(String, String)>, from: &str, to: &str) -> bool {
    edges.contains(&(from.to_string(), to.to_string()))
}

// ─── References through locals ───────────────────────────────────────────────

#[test]
fn test_local_reference_creates_edge_to_data_source() {
    let (_, edges) = edges(
        r#"
data "null_data_source" "network" {}

locals {
  network_id = data.null_data_source.network.id
}

resource "null_resource" "zone" {
  triggers = {
    network = local.network_id
  }
}

resource "null_resource" "direct" {
  name = local.network_id
}
"#,
    );
    assert!(
        has_edge(
            &edges,
            "data.null_data_source.network",
            "null_resource.direct"
        ),
        "{:?}",
        edges
    );
    assert!(
        has_edge(
            &edges,
            "data.null_data_source.network",
            "null_resource.zone"
        ),
        "nested block / map attribute referencing a local: {:?}",
        edges
    );
}

#[test]
fn test_chained_locals_resolve_transitively() {
    let (_, edges) = edges(
        r#"
resource "null_resource" "vcn" {}

locals {
  vcn_id  = null_resource.vcn.id
  by_name = { main = local.vcn_id }
  ids     = [for k, v in local.by_name : v]
}

resource "null_resource" "subnet" {
  vcn = local.ids[0]
}
"#,
    );
    assert!(
        has_edge(&edges, "null_resource.vcn", "null_resource.subnet"),
        "{:?}",
        edges
    );
}

#[test]
fn test_for_each_reference_creates_edge() {
    // for_each = local.x where the local resolves to a resource: the
    // for_each expression is a dependency, not only the attributes
    let (_, edges) = edges(
        r#"
resource "null_resource" "endpoints" {}

locals {
  endpoints = null_resource.endpoints.triggers
}

resource "null_resource" "rules" {
  for_each = local.endpoints
}
"#,
    );
    assert!(
        has_edge(&edges, "null_resource.endpoints", "null_resource.rules"),
        "{:?}",
        edges
    );
}

#[test]
fn test_output_referencing_local_depends_on_resource() {
    let (_, edges) = edges(
        r#"
resource "null_resource" "lb" {}

locals {
  lb_id = null_resource.lb.id
}

output "lb" {
  value = local.lb_id
}
"#,
    );
    assert!(has_edge(&edges, "null_resource.lb", "lb"), "{:?}", edges);
}

#[test]
fn test_cyclic_locals_do_not_hang_or_add_edges() {
    let (_, edges) = edges(
        r#"
locals {
  a = local.b
  b = local.a
}

resource "null_resource" "x" {
  name = local.a
}
"#,
    );
    assert!(edges.is_empty(), "{:?}", edges);
}

#[test]
fn test_var_references_still_ignored() {
    let (_, edges) = edges(
        r#"
variable "name" {
  default = "x"
}

locals {
  name = var.name
}

resource "null_resource" "x" {
  name = local.name
}
"#,
    );
    assert!(edges.is_empty(), "{:?}", edges);
}

// ─── count / for_each that can't be evaluated at graph time ──────────────────

fn node_addresses(graph: &ResourceGraph) -> Vec<String> {
    graph
        .node_indices()
        .map(|i| graph[i].address().to_string())
        .collect()
}

#[test]
fn test_count_from_local_keeps_resource_in_graph() {
    let (graph, edges) = edges(
        r#"
resource "null_resource" "db" {}

locals {
  dbs = [null_resource.db.id]
}

resource "null_resource" "prereqs" {
  count = length(local.dbs)
}
"#,
    );
    assert!(
        node_addresses(&graph).contains(&"null_resource.prereqs".to_string()),
        "{:?}",
        node_addresses(&graph)
    );
    assert!(
        has_edge(&edges, "null_resource.db", "null_resource.prereqs"),
        "{:?}",
        edges
    );
}

#[test]
fn test_for_each_from_local_function_keeps_resource_in_graph() {
    let (graph, _) = edges(
        r#"
resource "null_resource" "db" {}

locals {
  ids = [null_resource.db.id]
}

resource "null_resource" "users" {
  for_each = toset(local.ids)
}
"#,
    );
    assert!(
        node_addresses(&graph).contains(&"null_resource.users".to_string()),
        "{:?}",
        node_addresses(&graph)
    );
}

#[test]
fn test_count_from_variable_still_expands() {
    let (graph, _) = edges(
        r#"
variable "n" {
  default = 2
}

resource "null_resource" "x" {
  count = var.n
}
"#,
    );
    let mut addrs = node_addresses(&graph);
    addrs.sort();
    assert_eq!(addrs, vec!["null_resource.x[0]", "null_resource.x[1]"]);
}

// ─── DOT output ──────────────────────────────────────────────────────────────

#[test]
fn test_dot_data_source_label_has_single_prefix() {
    let (graph, _) = edges(r#"data "null_data_source" "network" {}"#);
    let dot = to_dot(&graph);
    assert!(
        dot.contains("label=\"data.null_data_source.network\\n"),
        "{}",
        dot
    );
    assert!(!dot.contains("data.data."), "{}", dot);
}

// ─── Module nodes in DOT output ───────────────────────────────────────────────

/// Render `main.tf` with module nodes and return DOT edges as label pairs.
fn module_dot_edges(tf: &str) -> (String, HashSet<(String, String)>) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("main.tf"), tf).unwrap();
    let ws = parse_directory(dir.path()).unwrap();
    let (graph, _) =
        build_resource_dag(&ws, &build_provider_map(&ws), &build_variable_defaults(&ws)).unwrap();
    let dot = to_dot_with_modules(&graph, &ws);

    // id -> first line of label
    let mut labels = std::collections::HashMap::new();
    for line in dot.lines() {
        let line = line.trim();
        if let Some((id, rest)) = line.split_once(" [label=\"") {
            let label = rest.split(['\\', '"']).next().unwrap();
            labels.insert(id.to_string(), label.to_string());
        }
    }
    let edges = dot
        .lines()
        .filter_map(|l| l.trim().split_once(" -> "))
        .map(|(from, rest)| {
            let to = rest.split_whitespace().next().unwrap();
            (labels[from].clone(), labels[to].clone())
        })
        .collect();
    (dot, edges)
}

const MODULE_CONFIG: &str = r#"
data "null_data_source" "subnet" {}

locals {
  subnet_id = data.null_data_source.subnet.id
  db_ids    = [for db in module.db : db.id]
}

module "db" {
  source    = "git::https://example.com/modules.git//database?ref=v1.2.0"
  count     = 2
  subnet_id = local.subnet_id
}

module "monitoring" {
  source   = "./modules/monitoring"
  db_ids   = local.db_ids
}

resource "null_resource" "prereqs" {
  count    = length(local.db_ids)
  triggers = {
    ids = join(",", local.db_ids)
  }
}

output "db" {
  value = module.db[0].id
}
"#;

#[test]
fn test_module_node_rendered_with_short_source() {
    let (dot, _) = module_dot_edges(MODULE_CONFIG);
    assert!(dot.contains("label=\"module.db\\ndatabase\""), "{}", dot);
    assert!(
        dot.contains("label=\"module.monitoring\\n./modules/monitoring\""),
        "{}",
        dot
    );
    assert!(dot.contains("shape=component"), "{}", dot);
}

#[test]
fn test_module_inputs_and_consumers_are_linked() {
    let (_, edges) = module_dot_edges(MODULE_CONFIG);
    let has = |from: &str, to: &str| edges.contains(&(from.to_string(), to.to_string()));
    // input through a local
    assert!(
        has("data.null_data_source.subnet", "module.db"),
        "{:?}",
        edges
    );
    // module -> module through a local
    assert!(has("module.db", "module.monitoring"), "{:?}", edges);
    // resource whose count and attributes use module outputs through a local
    assert!(has("module.db", "null_resource.prereqs"), "{:?}", edges);
    // output using a module output directly
    assert!(has("module.db", "output.db"), "{:?}", edges);
}

#[test]
fn test_module_references_do_not_change_execution_graph() {
    // to_dot (and plan/apply) must not gain module nodes
    let (graph, _) = edges(MODULE_CONFIG);
    let dot = to_dot(&graph);
    assert!(!dot.contains("module."), "{}", dot);
}

#[test]
fn test_no_modules_matches_to_dot() {
    let tf = r#"
resource "null_resource" "a" {}
resource "null_resource" "b" {
  name = null_resource.a.id
}
"#;
    let (graph, _) = edges(tf);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("main.tf"), tf).unwrap();
    let ws = parse_directory(dir.path()).unwrap();
    assert_eq!(to_dot_with_modules(&graph, &ws), to_dot(&graph));
}
