use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use colored::Colorize;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use tokio::sync::{mpsc, Semaphore};
use tracing::debug;

use super::resource_graph::{DagNode, ResourceGraph};

/// Status of a node during execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Pending,
    Running,
    Succeeded,
    Failed(String),
    Skipped(String),
}

/// Result of executing a single node.
#[derive(Debug)]
pub struct NodeResult {
    pub node_index: NodeIndex,
    pub address: String,
    pub status: NodeStatus,
    pub outputs: Option<serde_json::Value>,
}

/// Operation mode for the walker — controls progress messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkMode {
    Apply,
    Destroy,
}

/// Message sent back from worker tasks to the walker.
enum WalkerMessage {
    NodeCompleted(NodeResult),
}

/// Callback signature for node execution.
pub type NodeExecutor = Box<
    dyn Fn(
            NodeIndex,
            DagNode,
        ) -> futures::future::BoxFuture<'static, Result<Option<serde_json::Value>>>
        + Send
        + Sync,
>;

/// Info about a running node for the heartbeat timer.
struct RunningNode {
    address: String,
    verb_progress: &'static str, // "Creating", "Destroying", "Reading"
    verb_past: &'static str,     // "Creation", "Destruction", "Read"
}

/// Event-driven DAG walker that executes nodes as their dependencies are satisfied.
pub struct DagWalker {
    max_parallelism: usize,
}

impl DagWalker {
    pub fn new(max_parallelism: usize) -> Self {
        Self { max_parallelism }
    }

    /// Walk the DAG, executing nodes via the provided executor function.
    /// If `changed_addresses` is provided, only those resources will show progress output.
    pub async fn walk(
        &self,
        graph: &ResourceGraph,
        executor: Arc<NodeExecutor>,
        mode: WalkMode,
    ) -> Result<Vec<NodeResult>> {
        self.walk_with_filter(graph, executor, mode, None).await
    }

    /// Walk the DAG with an optional filter for progress display.
    pub async fn walk_with_filter(
        &self,
        graph: &ResourceGraph,
        executor: Arc<NodeExecutor>,
        mode: WalkMode,
        changed_addresses: Option<Arc<std::collections::HashMap<String, String>>>,
    ) -> Result<Vec<NodeResult>> {
        let node_count = graph.node_count();
        if node_count == 0 {
            return Ok(Vec::new());
        }

        // Count only resource/data nodes for progress display (skip outputs)
        let resource_count = graph
            .node_indices()
            .filter(|&idx| !matches!(graph[idx], DagNode::Output { .. }))
            .count();

        // Wall clock for the entire operation — shows parallelism in timestamps
        let wall_clock = Arc::new(Instant::now());

        let semaphore = Arc::new(Semaphore::new(self.max_parallelism));
        let statuses: Arc<DashMap<NodeIndex, NodeStatus>> = Arc::new(DashMap::new());
        let (tx, mut rx) = mpsc::channel::<WalkerMessage>(node_count);

        // Track start times and running node info for heartbeat
        let start_times: Arc<DashMap<NodeIndex, Instant>> = Arc::new(DashMap::new());
        let running_info: Arc<DashMap<NodeIndex, RunningNode>> = Arc::new(DashMap::new());
        let all_done = Arc::new(AtomicBool::new(false));

        // Spawn heartbeat timer — prints "Still creating... [10s elapsed]" every 10s
        let heartbeat_running = Arc::clone(&running_info);
        let heartbeat_times = Arc::clone(&start_times);
        let heartbeat_done = Arc::clone(&all_done);
        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                if heartbeat_done.load(Ordering::Relaxed) {
                    break;
                }
                // Print elapsed for all currently running nodes
                for entry in heartbeat_running.iter() {
                    let idx = *entry.key();
                    let info = entry.value();
                    if let Some(start) = heartbeat_times.get(&idx) {
                        let elapsed = start.elapsed().as_secs();
                        if elapsed >= 10 {
                            println!(
                                "{}: Still {}... [{} elapsed]",
                                info.address,
                                info.verb_progress.to_lowercase().cyan(),
                                format_duration(elapsed).bold(),
                            );
                        }
                    }
                }
            }
        });

        // Precompute dependency info
        let mut in_degree: HashMap<NodeIndex, usize> = HashMap::new();
        let mut dependents: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();
        let mut dependencies: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();

        for idx in graph.node_indices() {
            in_degree.insert(idx, 0);
            dependents.insert(idx, Vec::new());
            dependencies.insert(idx, Vec::new());
            statuses.insert(idx, NodeStatus::Pending);
        }

        for edge in graph.edge_indices() {
            if let Some((from, to)) = graph.edge_endpoints(edge) {
                *in_degree.entry(to).or_insert(0) += 1;
                dependents.entry(from).or_default().push(to);
                dependencies.entry(to).or_default().push(from);
            }
        }

        // Find initially ready nodes (no dependencies)
        let ready: Vec<NodeIndex> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&idx, _)| idx)
            .collect();

        let mut completed_count = 0;
        let mut resource_completed = 0;
        let mut results: Vec<NodeResult> = Vec::new();

        // Spawn initial ready nodes
        for &idx in &ready {
            spawn_node(
                idx,
                graph,
                &executor,
                &semaphore,
                &statuses,
                &tx,
                mode,
                &start_times,
                &running_info,
                &wall_clock,
                &changed_addresses,
            );
        }

        // Process completions until all nodes are done
        while completed_count < node_count {
            let msg = rx.recv().await;
            match msg {
                Some(WalkerMessage::NodeCompleted(result)) => {
                    let node_idx = result.node_index;
                    let succeeded = result.status == NodeStatus::Succeeded;
                    let is_output = matches!(graph[node_idx], DagNode::Output { .. });

                    // Calculate elapsed time for this node
                    let elapsed_secs = start_times
                        .get(&node_idx)
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(0);

                    // Remove from running tracking
                    let node_info = running_info.remove(&node_idx);
                    start_times.remove(&node_idx);

                    statuses.insert(node_idx, result.status.clone());
                    completed_count += 1;
                    if !is_output {
                        resource_completed += 1;
                    }

                    // Extract resource ID from outputs if available
                    let resource_id = result
                        .outputs
                        .as_ref()
                        .and_then(|o| o.get("id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    // User-facing progress (skip outputs and unchanged resources)
                    if !is_output && node_info.is_some() {
                        match &result.status {
                            NodeStatus::Succeeded => {
                                let verb_past = node_info
                                    .as_ref()
                                    .map(|(_, info)| info.verb_past)
                                    .unwrap_or("Operation");
                                let id_suffix = resource_id
                                    .as_deref()
                                    .map(|id| format!(" [id={}]", id))
                                    .unwrap_or_default();
                                println!(
                                    "{}: {} after {} [{}/{}]{}",
                                    result.address,
                                    format!("{} complete", verb_past).green().bold(),
                                    format_duration(elapsed_secs).bold(),
                                    resource_completed,
                                    resource_count,
                                    id_suffix,
                                );
                            }
                            NodeStatus::Failed(err) => {
                                println!(
                                    "{}: {} after {} — {}",
                                    result.address.bold(),
                                    "FAILED".red().bold(),
                                    format_duration(elapsed_secs),
                                    err.red(),
                                );
                            }
                            _ => {}
                        }
                    }

                    debug!(
                        address = %result.address,
                        status = ?result.status,
                        elapsed_secs = elapsed_secs,
                        progress = format!("{}/{}", completed_count, node_count),
                        "Node completed"
                    );

                    if succeeded {
                        if let Some(deps) = dependents.get(&node_idx) {
                            for &dependent_idx in deps {
                                let all_deps_met = dependencies
                                    .get(&dependent_idx)
                                    .map(|dep_list| {
                                        dep_list.iter().all(|dep_idx| {
                                            statuses
                                                .get(dep_idx)
                                                .map(|s| *s == NodeStatus::Succeeded)
                                                .unwrap_or(false)
                                        })
                                    })
                                    .unwrap_or(true);

                                if all_deps_met {
                                    spawn_node(
                                        dependent_idx,
                                        graph,
                                        &executor,
                                        &semaphore,
                                        &statuses,
                                        &tx,
                                        mode,
                                        &start_times,
                                        &running_info,
                                        &wall_clock,
                                        &changed_addresses,
                                    );
                                }
                            }
                        }
                    } else {
                        let skipped = collect_transitive_dependents(node_idx, &dependents);
                        for &skip_idx in &skipped {
                            let skip_address = graph[skip_idx].address().to_string();
                            let skip_is_output = matches!(graph[skip_idx], DagNode::Output { .. });
                            let reason = format!("Dependency '{}' failed", result.address);

                            if !skip_is_output {
                                resource_completed += 1;
                                println!(
                                    "{}: {} — {}",
                                    skip_address.bold(),
                                    "Skipped".yellow(),
                                    reason.dimmed(),
                                );
                            }

                            statuses.insert(skip_idx, NodeStatus::Skipped(reason.clone()));
                            completed_count += 1;

                            results.push(NodeResult {
                                node_index: skip_idx,
                                address: skip_address,
                                status: NodeStatus::Skipped(reason),
                                outputs: None,
                            });
                        }
                    }

                    results.push(result);
                }
                None => break,
            }
        }

        // Stop the heartbeat timer
        all_done.store(true, Ordering::Relaxed);
        heartbeat_handle.abort();

        Ok(results)
    }
}

/// Spawn execution of a single node.
#[allow(clippy::too_many_arguments)]
fn spawn_node(
    idx: NodeIndex,
    graph: &ResourceGraph,
    executor: &Arc<NodeExecutor>,
    semaphore: &Arc<Semaphore>,
    statuses: &Arc<DashMap<NodeIndex, NodeStatus>>,
    tx: &mpsc::Sender<WalkerMessage>,
    mode: WalkMode,
    start_times: &Arc<DashMap<NodeIndex, Instant>>,
    running_info: &Arc<DashMap<NodeIndex, RunningNode>>,
    _wall_clock: &Arc<Instant>,
    changed_addresses: &Option<Arc<std::collections::HashMap<String, String>>>,
) {
    let node = graph[idx].clone();
    let address = node.address().to_string();
    let is_output = matches!(node, DagNode::Output { .. });
    let is_data = matches!(node, DagNode::DataSource { .. });
    let executor = Arc::clone(executor);
    let semaphore = Arc::clone(semaphore);
    let statuses = Arc::clone(statuses);
    let tx = tx.clone();

    statuses.insert(idx, NodeStatus::Running);
    start_times.insert(idx, Instant::now());

    // Only show progress for resources that have actual changes
    let show_progress = if let Some(ref changed) = changed_addresses {
        changed.contains_key(&address) || is_data
    } else {
        true // No filter = show everything (plan mode, destroy mode)
    };

    // Show progress for resources only (not outputs, not unchanged)
    if !is_output && show_progress {
        // Use the planned action to determine the verb
        let action = changed_addresses
            .as_ref()
            .and_then(|m| m.get(&address))
            .map(|s| s.as_str());

        let (verb_progress, verb_past) = match mode {
            WalkMode::Destroy => ("Destroying", "Destruction"),
            WalkMode::Apply if is_data => ("Reading", "Read"),
            WalkMode::Apply => match action {
                Some("~") => ("Updating", "Update"),
                Some("-") => ("Destroying", "Destruction"),
                Some("-/+") => ("Replacing", "Replacement"),
                _ => ("Creating", "Creation"),
            },
        };

        println!("{}: {}...", address, verb_progress.cyan());

        running_info.insert(
            idx,
            RunningNode {
                address: address.clone(),
                verb_progress,
                verb_past,
            },
        );
    }

    tokio::spawn(async move {
        let _permit = semaphore.acquire().await.unwrap();

        let result = executor(idx, node).await;

        let node_result = match result {
            Ok(outputs) => NodeResult {
                node_index: idx,
                address,
                status: NodeStatus::Succeeded,
                outputs,
            },
            Err(e) => NodeResult {
                node_index: idx,
                address,
                status: NodeStatus::Failed(e.to_string()),
                outputs: None,
            },
        };

        let _ = tx.send(WalkerMessage::NodeCompleted(node_result)).await;
    });
}

/// Format seconds into a human-readable duration string.
fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else {
        let mins = secs / 60;
        let remaining = secs % 60;
        if remaining == 0 {
            format!("{}m", mins)
        } else {
            format!("{}m{}s", mins, remaining)
        }
    }
}

/// Collect all transitive dependents of a node (for cascade skip on failure).
fn collect_transitive_dependents(
    start: NodeIndex,
    dependents: &HashMap<NodeIndex, Vec<NodeIndex>>,
) -> Vec<NodeIndex> {
    let mut visited = HashSet::new();
    let mut stack = vec![start];

    while let Some(node) = stack.pop() {
        if let Some(deps) = dependents.get(&node) {
            for &dep in deps {
                if visited.insert(dep) {
                    stack.push(dep);
                }
            }
        }
    }

    visited.into_iter().collect()
}
