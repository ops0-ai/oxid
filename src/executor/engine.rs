use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use colored::Colorize;
use dashmap::DashMap;
use petgraph::graph::NodeIndex;
use tracing::{debug, info};

use crate::config::types::WorkspaceConfig;
use crate::dag::resource_graph::{self, DagNode};
use crate::dag::walker::{DagWalker, NodeExecutor, NodeResult, NodeStatus};
use crate::provider::manager::ProviderManager;
use crate::state::backend::StateBackend;

/// The action to take for a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceAction {
    Create,
    Update,
    Delete,
    Replace,
    Read,
    NoOp,
}

impl std::fmt::Display for ResourceAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceAction::Create => write!(f, "+"),
            ResourceAction::Update => write!(f, "~"),
            ResourceAction::Delete => write!(f, "-"),
            ResourceAction::Replace => write!(f, "-/+"),
            ResourceAction::Read => write!(f, "<="),
            ResourceAction::NoOp => write!(f, "(no changes)"),
        }
    }
}

/// A planned change for a single resource.
#[derive(Debug)]
pub struct PlannedChange {
    pub address: String,
    pub action: ResourceAction,
    pub resource_type: String,
    pub provider_source: String,
    pub planned_state: Option<serde_json::Value>,
    pub prior_state: Option<serde_json::Value>,
    pub user_config: Option<serde_json::Value>,
    pub requires_replace: Vec<String>,
    pub planned_private: Vec<u8>,
}

/// A planned output change.
#[derive(Debug)]
pub struct PlannedOutput {
    pub name: String,
    pub action: ResourceAction,
    pub value_known: bool,
}

/// Summary of a plan operation.
#[derive(Debug)]
pub struct PlanSummary {
    pub changes: Vec<PlannedChange>,
    pub outputs: Vec<PlannedOutput>,
    pub creates: usize,
    pub updates: usize,
    pub deletes: usize,
    pub replaces: usize,
    pub no_ops: usize,
}

impl std::fmt::Display for PlanSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.creates > 0 {
            parts.push(format!("{} to add", self.creates));
        }
        if self.replaces > 0 {
            parts.push(format!("{} to replace", self.replaces));
        }
        if self.updates > 0 {
            parts.push(format!("{} to change", self.updates));
        }
        if self.deletes > 0 {
            parts.push(format!("{} to destroy", self.deletes));
        }
        if parts.is_empty() {
            write!(f, "No changes.")
        } else {
            write!(f, "Plan: {}.", parts.join(", "))
        }
    }
}

/// Summary of an apply operation.
#[derive(Debug)]
pub struct ApplySummary {
    pub results: Vec<NodeResult>,
    pub added: usize,
    pub changed: usize,
    pub destroyed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub elapsed_secs: u64,
    pub is_destroy: bool,
}

impl std::fmt::Display for ApplySummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let action = if self.is_destroy { "Destroy" } else { "Apply" };
        let time = format_elapsed(self.elapsed_secs);
        if self.is_destroy {
            write!(
                f,
                "{} complete! Resources: {} destroyed",
                action, self.destroyed,
            )?;
        } else {
            write!(
                f,
                "{} complete! Resources: {} added, {} changed, {} destroyed",
                action, self.added, self.changed, self.destroyed,
            )?;
        }
        if self.failed > 0 {
            write!(f, ", {} failed", self.failed)?;
        }
        write!(f, ". Total time: {}.", time)
    }
}

fn format_elapsed(secs: u64) -> String {
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

/// The resource execution engine orchestrating plan and apply operations.
///
/// This is the core of oxid v2 — it directly communicates with providers
/// via gRPC to plan and apply individual resource changes, using the
/// event-driven DAG walker for maximum parallelism.
pub struct ResourceEngine {
    provider_manager: Arc<ProviderManager>,
    parallelism: usize,
}

impl ResourceEngine {
    pub fn new(provider_manager: Arc<ProviderManager>, parallelism: usize) -> Self {
        Self {
            provider_manager,
            parallelism,
        }
    }

    /// Get a reference to the provider manager.
    pub fn provider_manager(&self) -> &ProviderManager {
        &self.provider_manager
    }

    /// Plan all resources in the workspace.
    /// Returns a summary of what would change.
    pub async fn plan(
        &self,
        workspace: &WorkspaceConfig,
        backend: &dyn StateBackend,
        workspace_id: &str,
    ) -> Result<PlanSummary> {
        let provider_map = build_provider_map(workspace);
        let var_defaults = build_variable_defaults(workspace);
        let (graph, _node_map) =
            resource_graph::build_resource_dag(workspace, &provider_map, &var_defaults)?;

        // Ensure all providers are started and configured
        self.initialize_providers(workspace).await?;

        let pm = Arc::clone(&self.provider_manager);
        let ws_id = workspace_id.to_string();

        // Pre-load existing resource states so cross-resource references resolve during plan
        let resource_states = Arc::new(DashMap::new());
        {
            let existing = backend
                .list_resources(
                    &ws_id,
                    &crate::state::models::ResourceFilter {
                        resource_type: None,
                        module_path: None,
                        status: None,
                        address_pattern: None,
                    },
                )
                .await?;
            for res in existing {
                if let Ok(attrs) = serde_json::from_str::<serde_json::Value>(&res.attributes_json) {
                    resource_states.insert(res.address.clone(), attrs);
                }
            }
        }

        let mut changes = Vec::new();
        let mut outputs = Vec::new();

        // Count resources for progress
        let total_resources = graph
            .node_indices()
            .filter(|&idx| !matches!(graph[idx], DagNode::Output { .. }))
            .count();
        let mut planned_count = 0;

        // Walk the graph to plan each resource
        for idx in graph.node_indices() {
            let node = &graph[idx];
            match node {
                DagNode::Resource {
                    address,
                    resource_type,
                    provider_source,
                    config,
                    index,
                    ..
                } => {
                    planned_count += 1;
                    println!(
                        "{}: {} [{}/{}]",
                        address,
                        "Refreshing state...".dimmed(),
                        planned_count,
                        total_resources,
                    );

                    // Build eval context with count.index / each.key + existing resource states
                    let mut eval_ctx = EvalContext::with_states(
                        var_defaults.clone(),
                        Arc::clone(&resource_states),
                    );
                    match index {
                        Some(crate::config::types::ResourceIndex::Count(i)) => {
                            eval_ctx.count_index = Some(*i)
                        }
                        Some(crate::config::types::ResourceIndex::ForEach(k)) => {
                            eval_ctx.each_key = Some(k.clone());
                            eval_ctx.each_value = Some(serde_json::Value::String(k.clone()));
                        }
                        None => {}
                    }

                    // Build the proposed config as JSON
                    let user_config = attributes_to_json(&config.attributes, &eval_ctx);

                    // Build full config with all schema attributes for msgpack encoding
                    let config_json = if let Ok(Some(schema)) =
                        pm.get_resource_schema(provider_source, resource_type).await
                    {
                        build_full_resource_config(&user_config, &schema)
                    } else {
                        user_config.clone()
                    };

                    // Check if resource exists in state
                    let prior_state = backend
                        .get_resource(&ws_id, address)
                        .await?
                        .map(|r| serde_json::from_str::<serde_json::Value>(&r.attributes_json))
                        .transpose()?;

                    let plan_result = match pm
                        .plan_resource(
                            provider_source,
                            resource_type,
                            prior_state.as_ref(),
                            Some(&config_json),
                            &config_json,
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(e) => {
                            info!("PlanResourceChange failed for {}: {}", address, e);
                            continue;
                        }
                    };

                    let action = determine_action(
                        prior_state.as_ref(),
                        plan_result.planned_state.as_ref(),
                        &plan_result.requires_replace,
                    );

                    changes.push(PlannedChange {
                        address: address.clone(),
                        action,
                        resource_type: resource_type.clone(),
                        provider_source: provider_source.clone(),
                        planned_state: plan_result.planned_state,
                        prior_state,
                        user_config: Some(user_config),
                        requires_replace: plan_result.requires_replace,
                        planned_private: plan_result.planned_private,
                    });
                }
                DagNode::DataSource {
                    address,
                    resource_type,
                    provider_source,
                    config,
                    index,
                    ..
                } => {
                    planned_count += 1;
                    println!(
                        "{}: {} [{}/{}]",
                        address,
                        "Reading...".cyan(),
                        planned_count,
                        total_resources,
                    );
                    let mut ds_eval_ctx = EvalContext::with_states(
                        var_defaults.clone(),
                        Arc::clone(&resource_states),
                    );
                    match index {
                        Some(crate::config::types::ResourceIndex::Count(i)) => {
                            ds_eval_ctx.count_index = Some(*i);
                        }
                        Some(crate::config::types::ResourceIndex::ForEach(k)) => {
                            ds_eval_ctx.each_key = Some(k.clone());
                            ds_eval_ctx.each_value = Some(serde_json::Value::String(k.clone()));
                        }
                        None => {}
                    }
                    let user_config = attributes_to_json(&config.attributes, &ds_eval_ctx);

                    // Build full config with all schema attributes
                    let config_json = if let Ok(Some(schema)) = pm
                        .get_data_source_schema(provider_source, resource_type)
                        .await
                    {
                        build_full_resource_config(&user_config, &schema)
                    } else {
                        user_config.clone()
                    };

                    let read_start = std::time::Instant::now();
                    let data_state = match pm
                        .read_data_source(provider_source, resource_type, &config_json)
                        .await
                    {
                        Ok(state) => {
                            let elapsed = read_start.elapsed().as_secs();
                            let id_str = state
                                .get("id")
                                .and_then(|v| v.as_str())
                                .map(|id| format!(" [id={}]", id))
                                .unwrap_or_default();
                            println!(
                                "{}: {} after {}s{}",
                                address,
                                "Read complete".green(),
                                elapsed,
                                id_str,
                            );
                            state
                        }
                        Err(e) => {
                            println!("{}: {} — {}", address, "Read FAILED".red().bold(), e,);
                            continue;
                        }
                    };

                    changes.push(PlannedChange {
                        address: address.clone(),
                        action: ResourceAction::Read,
                        resource_type: resource_type.clone(),
                        provider_source: provider_source.clone(),
                        planned_state: Some(data_state),
                        prior_state: None,
                        user_config: Some(user_config),
                        requires_replace: vec![],
                        planned_private: vec![],
                    });
                }
                DagNode::Output { ref name, .. } => {
                    outputs.push(PlannedOutput {
                        name: name.clone(),
                        action: ResourceAction::Create,
                        value_known: false,
                    });
                }
            }
        }

        let creates = changes
            .iter()
            .filter(|c| c.action == ResourceAction::Create)
            .count();
        let updates = changes
            .iter()
            .filter(|c| c.action == ResourceAction::Update)
            .count();
        let deletes = changes
            .iter()
            .filter(|c| c.action == ResourceAction::Delete)
            .count();
        let replaces = changes
            .iter()
            .filter(|c| c.action == ResourceAction::Replace)
            .count();
        let no_ops = changes
            .iter()
            .filter(|c| c.action == ResourceAction::NoOp)
            .count();

        Ok(PlanSummary {
            changes,
            outputs,
            creates,
            updates,
            deletes,
            replaces,
            no_ops,
        })
    }

    /// Apply all planned changes using the event-driven DAG walker.
    pub async fn apply(
        &self,
        workspace: &WorkspaceConfig,
        backend: Arc<dyn StateBackend>,
        workspace_id: &str,
        plan: &PlanSummary,
    ) -> Result<ApplySummary> {
        let provider_map = build_provider_map(workspace);
        let var_defaults = build_variable_defaults(workspace);
        let (graph, _node_map) =
            resource_graph::build_resource_dag(workspace, &provider_map, &var_defaults)?;

        let pm = Arc::clone(&self.provider_manager);
        let ws_id = workspace_id.to_string();
        let backend_clone = Arc::clone(&backend);
        // Shared map of completed resource states for cross-resource reference resolution.
        // As each resource completes, its new state is inserted here so dependents can
        // resolve references like `aws_s3_bucket.public_scripts.id`.
        let resource_states: Arc<DashMap<String, serde_json::Value>> = Arc::new(DashMap::new());

        // Build a map of planned changes for the executor to reference
        let _planned_changes: Arc<HashMap<String, &PlannedChange>> = Arc::new(
            plan.changes
                .iter()
                .map(|c| (c.address.clone(), c))
                .collect(),
        );

        // Create the node executor closure
        let executor: NodeExecutor = Box::new(move |_idx: NodeIndex, node: DagNode| {
            let pm = Arc::clone(&pm);
            let ws_id = ws_id.clone();
            let backend = Arc::clone(&backend_clone);
            let resource_states = Arc::clone(&resource_states);
            let var_defaults = var_defaults.clone();

            Box::pin(async move {
                match node {
                    DagNode::Resource {
                        ref address,
                        ref resource_type,
                        ref provider_source,
                        ref config,
                        ref index,
                        ..
                    } => {
                        let mut eval_ctx = EvalContext::with_states(
                            var_defaults.clone(),
                            Arc::clone(&resource_states),
                        );
                        match index {
                            Some(crate::config::types::ResourceIndex::Count(i)) => {
                                eval_ctx.count_index = Some(*i);
                            }
                            Some(crate::config::types::ResourceIndex::ForEach(k)) => {
                                eval_ctx.each_key = Some(k.clone());
                                eval_ctx.each_value = Some(serde_json::Value::String(k.clone()));
                            }
                            None => {}
                        }
                        let user_config = attributes_to_json(&config.attributes, &eval_ctx);

                        // Build full config with all schema attributes for msgpack encoding
                        let config_json = if let Ok(Some(schema)) =
                            pm.get_resource_schema(provider_source, resource_type).await
                        {
                            build_full_resource_config(&user_config, &schema)
                        } else {
                            user_config
                        };

                        // Get prior state from database
                        let prior_state = backend
                            .get_resource(&ws_id, address)
                            .await?
                            .map(|r| serde_json::from_str::<serde_json::Value>(&r.attributes_json))
                            .transpose()?;

                        // Plan
                        let plan_result = pm
                            .plan_resource(
                                provider_source,
                                resource_type,
                                prior_state.as_ref(),
                                Some(&config_json),
                                &config_json,
                            )
                            .await?;

                        // If requires_replace is non-empty AND there's a prior state,
                        // we need to destroy the old resource first, then create new.
                        let apply_result =
                            if !plan_result.requires_replace.is_empty() && prior_state.is_some() {
                                info!(
                                    address = %address,
                                    replace_fields = ?plan_result.requires_replace,
                                    "Resource requires replacement — destroying old, creating new"
                                );

                                // Step 1: Destroy the old resource
                                // Plan a destroy (prior → null)
                                let destroy_plan = pm
                                    .plan_resource(
                                        provider_source,
                                        resource_type,
                                        prior_state.as_ref(),
                                        None, // proposed_new = null means destroy
                                        &config_json,
                                    )
                                    .await?;

                                // Apply the destroy
                                let _destroy_result = pm
                                    .apply_resource(
                                        provider_source,
                                        resource_type,
                                        prior_state.as_ref(),
                                        None, // planned_state = null means destroy
                                        &config_json,
                                        &destroy_plan.planned_private,
                                    )
                                    .await?;

                                info!(address = %address, "Old resource destroyed");

                                // Remove from state database
                                backend.delete_resource(&ws_id, address).await.ok();

                                // Step 2: Create the new resource
                                // Plan a create (null → new)
                                let create_plan = pm
                                    .plan_resource(
                                        provider_source,
                                        resource_type,
                                        None, // no prior state
                                        Some(&config_json),
                                        &config_json,
                                    )
                                    .await?;

                                // Apply the create
                                pm.apply_resource(
                                    provider_source,
                                    resource_type,
                                    None, // no prior state
                                    create_plan.planned_state.as_ref(),
                                    &config_json,
                                    &create_plan.planned_private,
                                )
                                .await?
                            } else {
                                // Normal apply (create or in-place update)
                                pm.apply_resource(
                                    provider_source,
                                    resource_type,
                                    prior_state.as_ref(),
                                    plan_result.planned_state.as_ref(),
                                    &config_json,
                                    &plan_result.planned_private,
                                )
                                .await?
                            };

                        // Store the new state in both the database and the shared map
                        if let Some(ref new_state) = apply_result.new_state {
                            // Insert into shared resource states for dependent resources
                            resource_states.insert(address.clone(), new_state.clone());

                            let mut resource_state = crate::state::models::ResourceState::new(
                                &ws_id,
                                resource_type,
                                &config.name,
                                address,
                            );
                            resource_state.provider_source = provider_source.to_string();
                            resource_state.status = "created".to_string();
                            resource_state.attributes_json = serde_json::to_string(new_state)?;
                            resource_state.index_key = match index {
                                Some(crate::config::types::ResourceIndex::Count(i)) => {
                                    Some(i.to_string())
                                }
                                Some(crate::config::types::ResourceIndex::ForEach(k)) => {
                                    Some(k.clone())
                                }
                                None => None,
                            };

                            backend.upsert_resource(&resource_state).await?;

                            info!(address = %address, "Resource applied successfully");
                        }

                        Ok(apply_result.new_state)
                    }
                    DagNode::DataSource {
                        ref address,
                        ref resource_type,
                        ref provider_source,
                        ref config,
                        ref index,
                        ..
                    } => {
                        let mut eval_ctx = EvalContext::with_states(
                            var_defaults.clone(),
                            Arc::clone(&resource_states),
                        );
                        match index {
                            Some(crate::config::types::ResourceIndex::Count(i)) => {
                                eval_ctx.count_index = Some(*i);
                            }
                            Some(crate::config::types::ResourceIndex::ForEach(k)) => {
                                eval_ctx.each_key = Some(k.clone());
                                eval_ctx.each_value = Some(serde_json::Value::String(k.clone()));
                            }
                            None => {}
                        }
                        let user_config = attributes_to_json(&config.attributes, &eval_ctx);

                        // Build full config with all schema attributes
                        let config_json = if let Ok(Some(schema)) = pm
                            .get_data_source_schema(provider_source, resource_type)
                            .await
                        {
                            build_full_resource_config(&user_config, &schema)
                        } else {
                            user_config
                        };

                        let state = pm
                            .read_data_source(provider_source, resource_type, &config_json)
                            .await?;
                        // Store data source state for dependent resources
                        resource_states.insert(address.clone(), state.clone());
                        Ok(Some(state))
                    }
                    DagNode::Output { .. } => {
                        // Outputs are evaluated after all resources
                        Ok(None)
                    }
                }
            })
        });

        let walker = DagWalker::new(self.parallelism);
        let start = std::time::Instant::now();
        let results = walker
            .walk(
                &graph,
                Arc::new(executor),
                crate::dag::walker::WalkMode::Apply,
            )
            .await?;
        let elapsed_secs = start.elapsed().as_secs();

        let failed = results
            .iter()
            .filter(|r| matches!(r.status, NodeStatus::Failed(_)))
            .count();
        let skipped = results
            .iter()
            .filter(|r| matches!(r.status, NodeStatus::Skipped(_)))
            .count();

        // Count by action type from the plan
        let added = plan.creates + plan.replaces;
        let changed = plan.updates;
        let destroyed = plan.deletes;

        Ok(ApplySummary {
            results,
            added,
            changed,
            destroyed,
            failed,
            skipped,
            elapsed_secs,
            is_destroy: false,
        })
    }

    /// Destroy resources in reverse dependency order.
    pub async fn destroy(
        &self,
        workspace: &WorkspaceConfig,
        backend: Arc<dyn StateBackend>,
        workspace_id: &str,
    ) -> Result<ApplySummary> {
        let provider_map = build_provider_map(workspace);
        let var_defaults = build_variable_defaults(workspace);
        let (graph, _node_map) =
            resource_graph::build_resource_dag(workspace, &provider_map, &var_defaults)?;

        // For destroy, we reverse the graph edges so dependents are destroyed first
        let mut reverse_graph = petgraph::graph::DiGraph::new();
        let mut idx_map: HashMap<NodeIndex, NodeIndex> = HashMap::new();

        for idx in graph.node_indices() {
            let new_idx = reverse_graph.add_node(graph[idx].clone());
            idx_map.insert(idx, new_idx);
        }

        for edge in graph.edge_indices() {
            if let Some((from, to)) = graph.edge_endpoints(edge) {
                // Reverse the edge direction
                reverse_graph.add_edge(
                    idx_map[&to],
                    idx_map[&from],
                    crate::dag::resource_graph::DependencyEdge::Explicit,
                );
            }
        }

        let pm = Arc::clone(&self.provider_manager);
        let ws_id = workspace_id.to_string();
        let backend_clone = Arc::clone(&backend);

        self.initialize_providers(workspace).await?;

        let executor: NodeExecutor = Box::new(move |_idx: NodeIndex, node: DagNode| {
            let pm = Arc::clone(&pm);
            let ws_id = ws_id.clone();
            let backend = Arc::clone(&backend_clone);
            let var_defaults = var_defaults.clone();

            Box::pin(async move {
                match node {
                    DagNode::Resource {
                        ref address,
                        ref resource_type,
                        ref provider_source,
                        ref config,
                        ref index,
                        ..
                    } => {
                        let mut eval_ctx = EvalContext::plan_only(var_defaults.clone());
                        match index {
                            Some(crate::config::types::ResourceIndex::Count(i)) => {
                                eval_ctx.count_index = Some(*i);
                            }
                            Some(crate::config::types::ResourceIndex::ForEach(k)) => {
                                eval_ctx.each_key = Some(k.clone());
                                eval_ctx.each_value = Some(serde_json::Value::String(k.clone()));
                            }
                            None => {}
                        }
                        // Get current state
                        let current_state = backend
                            .get_resource(&ws_id, address)
                            .await?
                            .map(|r| serde_json::from_str::<serde_json::Value>(&r.attributes_json))
                            .transpose()?;

                        if current_state.is_none() {
                            debug!(address = %address, "Resource not in state, skipping destroy");
                            return Ok(None);
                        }

                        let user_config = attributes_to_json(&config.attributes, &eval_ctx);

                        // Build full config with all schema attributes for msgpack encoding
                        let config_json = if let Ok(Some(schema)) =
                            pm.get_resource_schema(provider_source, resource_type).await
                        {
                            build_full_resource_config(&user_config, &schema)
                        } else {
                            user_config
                        };

                        // Plan destroy (proposed_new_state = null)
                        let plan_result = pm
                            .plan_resource(
                                provider_source,
                                resource_type,
                                current_state.as_ref(),
                                None, // null planned state = destroy
                                &config_json,
                            )
                            .await?;

                        // Apply destroy
                        let _apply_result = pm
                            .apply_resource(
                                provider_source,
                                resource_type,
                                current_state.as_ref(),
                                None, // null planned state = destroy
                                &config_json,
                                &plan_result.planned_private,
                            )
                            .await?;

                        // Remove from state
                        backend.delete_resource(&ws_id, address).await?;
                        info!(address = %address, "Resource destroyed");

                        // Return the prior state's ID so the walker can display it
                        let resource_id = current_state
                            .as_ref()
                            .and_then(|s| s.get("id"))
                            .and_then(|v| v.as_str())
                            .map(|id| serde_json::json!({"id": id}));
                        Ok(resource_id)
                    }
                    _ => Ok(None),
                }
            })
        });

        let walker = DagWalker::new(self.parallelism);
        let start = std::time::Instant::now();
        let results = walker
            .walk(
                &reverse_graph,
                Arc::new(executor),
                crate::dag::walker::WalkMode::Destroy,
            )
            .await?;
        let elapsed_secs = start.elapsed().as_secs();

        let destroyed = results
            .iter()
            .filter(|r| r.status == NodeStatus::Succeeded)
            .count();
        let failed = results
            .iter()
            .filter(|r| matches!(r.status, NodeStatus::Failed(_)))
            .count();
        let skipped = results
            .iter()
            .filter(|r| matches!(r.status, NodeStatus::Skipped(_)))
            .count();

        Ok(ApplySummary {
            results,
            added: 0,
            changed: 0,
            destroyed,
            failed,
            skipped,
            elapsed_secs,
            is_destroy: true,
        })
    }

    /// Initialize all providers referenced in the workspace.
    async fn initialize_providers(&self, workspace: &WorkspaceConfig) -> Result<()> {
        // Build variable defaults map for resolving var.xxx references
        let var_defaults = build_variable_defaults(workspace);

        for provider in &workspace.providers {
            let version = provider.version_constraint.as_deref().unwrap_or(">= 0.0.0");

            info!(
                provider = %provider.source,
                version = %version,
                "Initializing provider"
            );

            self.provider_manager
                .get_connection(&provider.source, version)
                .await
                .context(format!("Failed to initialize provider {}", provider.source))?;

            // Get schema so we know all provider config attributes (required for cty msgpack)
            let schema = self
                .provider_manager
                .get_schema(&provider.source, version)
                .await
                .context(format!(
                    "Failed to get schema for provider {}",
                    provider.source
                ))?;

            // Build full provider config with all attributes (unset ones as null)
            let user_config = resolve_attributes(&provider.config, &var_defaults);
            let full_config = build_full_provider_config(&user_config, &schema);
            info!(
                "Configuring provider with {} attributes",
                full_config.as_object().map(|m| m.len()).unwrap_or(0)
            );

            self.provider_manager
                .configure_provider(&provider.source, &full_config)
                .await
                .context(format!("Failed to configure provider {}", provider.source))?;
        }

        Ok(())
    }

    /// Stop all running providers.
    pub async fn shutdown(&self) -> Result<()> {
        self.provider_manager.stop_all().await
    }
}

// ─── Helper Functions ────────────────────────────────────────────────────────

/// Build a map from provider local name to source string.
pub fn build_provider_map(workspace: &WorkspaceConfig) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for provider in &workspace.providers {
        map.insert(provider.name.clone(), provider.source.clone());
    }

    // Also add from terraform_settings.required_providers
    if let Some(ref tf) = workspace.terraform_settings {
        for (name, req) in &tf.required_providers {
            map.insert(name.clone(), req.source.clone());
        }
    }

    map
}

/// Evaluation context for resolving expressions.
/// Contains variable defaults and completed resource states for cross-resource references.
pub struct EvalContext {
    pub var_defaults: HashMap<String, serde_json::Value>,
    /// Completed resource states keyed by address (e.g. "aws_s3_bucket.public_scripts").
    /// Populated during apply as resources complete. Empty during plan.
    pub resource_states: Arc<DashMap<String, serde_json::Value>>,
    /// Current count index for resources with `count` (e.g. count.index = 3).
    pub count_index: Option<usize>,
    /// Current for_each key (e.g. each.key = "us-east-1a").
    pub each_key: Option<String>,
    /// Current for_each value.
    pub each_value: Option<serde_json::Value>,
}

impl EvalContext {
    pub fn plan_only(var_defaults: HashMap<String, serde_json::Value>) -> Self {
        Self {
            var_defaults,
            resource_states: Arc::new(DashMap::new()),
            count_index: None,
            each_key: None,
            each_value: None,
        }
    }

    pub fn with_states(
        var_defaults: HashMap<String, serde_json::Value>,
        resource_states: Arc<DashMap<String, serde_json::Value>>,
    ) -> Self {
        Self {
            var_defaults,
            resource_states,
            count_index: None,
            each_key: None,
            each_value: None,
        }
    }
}

/// Convert attribute expressions to a JSON object, resolving variable and resource references.
pub fn attributes_to_json(
    attrs: &HashMap<String, crate::config::types::Expression>,
    ctx: &EvalContext,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, expr) in attrs {
        map.insert(key.clone(), eval_expression(expr, ctx));
    }
    serde_json::Value::Object(map)
}

/// Evaluate an expression to a JSON value, resolving variable and resource references.
pub fn eval_expression(
    expr: &crate::config::types::Expression,
    ctx: &EvalContext,
) -> serde_json::Value {
    use crate::config::types::{Expression, TemplatePart};
    match expr {
        Expression::Literal(val) => resolve_value_json(val, ctx),
        Expression::Reference(parts) => resolve_reference(parts, ctx),
        Expression::Template(parts) => {
            let mut result = String::new();
            for part in parts {
                match part {
                    TemplatePart::Literal(s) => result.push_str(s),
                    TemplatePart::Interpolation(expr) => {
                        let val = eval_expression(expr, ctx);
                        match val {
                            serde_json::Value::String(s) => result.push_str(&s),
                            serde_json::Value::Number(n) => result.push_str(&n.to_string()),
                            serde_json::Value::Bool(b) => result.push_str(&b.to_string()),
                            serde_json::Value::Null => {} // skip nulls in templates
                            _ => result.push_str(&val.to_string()),
                        }
                    }
                    TemplatePart::Directive(expr) => {
                        let val = eval_expression(expr, ctx);
                        if let serde_json::Value::String(s) = val {
                            result.push_str(&s);
                        }
                    }
                }
            }
            serde_json::Value::String(result)
        }
        Expression::FunctionCall { name, args } => {
            let evaluated_args: Vec<serde_json::Value> =
                args.iter().map(|a| eval_expression(a, ctx)).collect();
            match name.as_str() {
                "tolist" | "toset" => evaluated_args
                    .into_iter()
                    .next()
                    .unwrap_or(serde_json::Value::Null),
                "tostring" => match evaluated_args.into_iter().next() {
                    Some(serde_json::Value::String(s)) => serde_json::Value::String(s),
                    Some(v) => serde_json::Value::String(v.to_string()),
                    None => serde_json::Value::Null,
                },
                "tonumber" => match evaluated_args.first() {
                    Some(serde_json::Value::String(s)) => s
                        .parse::<f64>()
                        .map(|n| serde_json::json!(n))
                        .unwrap_or(serde_json::Value::Null),
                    Some(v @ serde_json::Value::Number(_)) => v.clone(),
                    _ => serde_json::Value::Null,
                },
                "tobool" => match evaluated_args.first() {
                    Some(serde_json::Value::String(s)) => match s.as_str() {
                        "true" => serde_json::Value::Bool(true),
                        "false" => serde_json::Value::Bool(false),
                        _ => serde_json::Value::Null,
                    },
                    Some(v @ serde_json::Value::Bool(_)) => v.clone(),
                    _ => serde_json::Value::Null,
                },
                "tomap" => evaluated_args
                    .into_iter()
                    .next()
                    .unwrap_or(serde_json::Value::Null),
                "jsonencode" => {
                    if let Some(val) = evaluated_args.into_iter().next() {
                        match serde_json::to_string(&val) {
                            Ok(s) => serde_json::Value::String(s),
                            Err(_) => serde_json::Value::Null,
                        }
                    } else {
                        serde_json::Value::Null
                    }
                }
                "jsondecode" => {
                    if let Some(serde_json::Value::String(s)) = evaluated_args.first() {
                        serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
                    } else {
                        serde_json::Value::Null
                    }
                }
                "length" => {
                    if let Some(serde_json::Value::Array(arr)) = evaluated_args.first() {
                        serde_json::json!(arr.len())
                    } else if let Some(serde_json::Value::String(s)) = evaluated_args.first() {
                        serde_json::json!(s.len())
                    } else if let Some(serde_json::Value::Object(m)) = evaluated_args.first() {
                        serde_json::json!(m.len())
                    } else {
                        serde_json::json!(0)
                    }
                }
                "concat" => {
                    let mut result = Vec::new();
                    for arg in &evaluated_args {
                        if let serde_json::Value::Array(arr) = arg {
                            result.extend(arr.iter().cloned());
                        }
                    }
                    serde_json::Value::Array(result)
                }
                "merge" => {
                    let mut result = serde_json::Map::new();
                    for arg in &evaluated_args {
                        if let serde_json::Value::Object(m) = arg {
                            result.extend(m.iter().map(|(k, v)| (k.clone(), v.clone())));
                        }
                    }
                    serde_json::Value::Object(result)
                }
                "keys" => {
                    if let Some(serde_json::Value::Object(m)) = evaluated_args.first() {
                        serde_json::Value::Array(
                            m.keys()
                                .map(|k| serde_json::Value::String(k.clone()))
                                .collect(),
                        )
                    } else {
                        serde_json::Value::Array(vec![])
                    }
                }
                "values" => {
                    if let Some(serde_json::Value::Object(m)) = evaluated_args.first() {
                        serde_json::Value::Array(m.values().cloned().collect())
                    } else {
                        serde_json::Value::Array(vec![])
                    }
                }
                "lookup" => {
                    let map = evaluated_args.first();
                    let key = evaluated_args.get(1);
                    let default = evaluated_args.get(2);
                    if let (
                        Some(serde_json::Value::Object(m)),
                        Some(serde_json::Value::String(k)),
                    ) = (map, key)
                    {
                        m.get(k)
                            .cloned()
                            .or_else(|| default.cloned())
                            .unwrap_or(serde_json::Value::Null)
                    } else {
                        default.cloned().unwrap_or(serde_json::Value::Null)
                    }
                }
                "element" => {
                    let list = evaluated_args.first();
                    let idx = evaluated_args.get(1);
                    if let (
                        Some(serde_json::Value::Array(arr)),
                        Some(serde_json::Value::Number(n)),
                    ) = (list, idx)
                    {
                        let i = n.as_u64().unwrap_or(0) as usize;
                        arr.get(i % arr.len().max(1))
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    } else {
                        serde_json::Value::Null
                    }
                }
                "join" => {
                    if let (
                        Some(serde_json::Value::String(sep)),
                        Some(serde_json::Value::Array(arr)),
                    ) = (evaluated_args.first(), evaluated_args.get(1))
                    {
                        let parts: Vec<String> = arr
                            .iter()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .collect();
                        serde_json::Value::String(parts.join(sep))
                    } else {
                        serde_json::Value::String(String::new())
                    }
                }
                "split" => {
                    if let (
                        Some(serde_json::Value::String(sep)),
                        Some(serde_json::Value::String(s)),
                    ) = (evaluated_args.first(), evaluated_args.get(1))
                    {
                        serde_json::Value::Array(
                            s.split(sep.as_str())
                                .map(|p| serde_json::Value::String(p.to_string()))
                                .collect(),
                        )
                    } else {
                        serde_json::Value::Array(vec![])
                    }
                }
                "format" => {
                    if let Some(serde_json::Value::String(fmt)) = evaluated_args.first() {
                        // Simple %s/%d/%v replacement
                        let mut result = fmt.clone();
                        for arg in &evaluated_args[1..] {
                            let replacement = match arg {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                other => other.to_string(),
                            };
                            if let Some(pos) = result
                                .find("%s")
                                .or_else(|| result.find("%d"))
                                .or_else(|| result.find("%v"))
                            {
                                result.replace_range(pos..pos + 2, &replacement);
                            }
                        }
                        serde_json::Value::String(result)
                    } else {
                        serde_json::Value::String(String::new())
                    }
                }
                "coalesce" => evaluated_args
                    .into_iter()
                    .find(|v| !v.is_null() && *v != serde_json::Value::String(String::new()))
                    .unwrap_or(serde_json::Value::Null),
                "lower" => match evaluated_args.into_iter().next() {
                    Some(serde_json::Value::String(s)) => {
                        serde_json::Value::String(s.to_lowercase())
                    }
                    _ => serde_json::Value::Null,
                },
                "upper" => match evaluated_args.into_iter().next() {
                    Some(serde_json::Value::String(s)) => {
                        serde_json::Value::String(s.to_uppercase())
                    }
                    _ => serde_json::Value::Null,
                },
                "trim" | "trimspace" => match evaluated_args.into_iter().next() {
                    Some(serde_json::Value::String(s)) => {
                        serde_json::Value::String(s.trim().to_string())
                    }
                    _ => serde_json::Value::Null,
                },
                "replace" => {
                    if let (
                        Some(serde_json::Value::String(s)),
                        Some(serde_json::Value::String(old)),
                        Some(serde_json::Value::String(new)),
                    ) = (
                        evaluated_args.first(),
                        evaluated_args.get(1),
                        evaluated_args.get(2),
                    ) {
                        serde_json::Value::String(s.replace(old.as_str(), new.as_str()))
                    } else {
                        serde_json::Value::Null
                    }
                }
                "try" => evaluated_args
                    .into_iter()
                    .find(|v| !v.is_null())
                    .unwrap_or(serde_json::Value::Null),
                "compact" => {
                    if let Some(serde_json::Value::Array(arr)) = evaluated_args.into_iter().next() {
                        serde_json::Value::Array(
                            arr.into_iter()
                                .filter(|v| {
                                    !matches!(v, serde_json::Value::String(s) if s.is_empty())
                                        && !v.is_null()
                                })
                                .collect(),
                        )
                    } else {
                        serde_json::Value::Array(vec![])
                    }
                }
                "flatten" => {
                    if let Some(serde_json::Value::Array(arr)) = evaluated_args.into_iter().next() {
                        let mut result = Vec::new();
                        for item in arr {
                            if let serde_json::Value::Array(inner) = item {
                                result.extend(inner);
                            } else {
                                result.push(item);
                            }
                        }
                        serde_json::Value::Array(result)
                    } else {
                        serde_json::Value::Array(vec![])
                    }
                }
                "distinct" => {
                    if let Some(serde_json::Value::Array(arr)) = evaluated_args.into_iter().next() {
                        let mut seen = Vec::new();
                        let mut result = Vec::new();
                        for item in arr {
                            let s = item.to_string();
                            if !seen.contains(&s) {
                                seen.push(s);
                                result.push(item);
                            }
                        }
                        serde_json::Value::Array(result)
                    } else {
                        serde_json::Value::Array(vec![])
                    }
                }
                other => {
                    tracing::warn!("Unsupported function: {}()", other);
                    serde_json::Value::Null
                }
            }
        }
        Expression::Conditional {
            condition,
            true_val,
            false_val,
        } => {
            let cond = eval_expression(condition, ctx);
            let is_true = match &cond {
                serde_json::Value::Bool(b) => *b,
                serde_json::Value::Null => false,
                _ => true,
            };
            if is_true {
                eval_expression(true_val, ctx)
            } else {
                eval_expression(false_val, ctx)
            }
        }
        _ => serde_json::Value::Null,
    }
}

/// Resolve a reference expression (var.xxx, aws_vpc.main.id, data.aws_ami.xxx.id, etc.)
fn resolve_reference(parts: &[String], ctx: &EvalContext) -> serde_json::Value {
    if parts.len() >= 2 && parts[0] == "var" {
        if let Some(val) = ctx.var_defaults.get(&parts[1]) {
            return val.clone();
        }
        return serde_json::Value::Null;
    }

    // count.index
    if parts.len() >= 2 && parts[0] == "count" && parts[1] == "index" {
        if let Some(idx) = ctx.count_index {
            return serde_json::json!(idx);
        }
        return serde_json::Value::Null;
    }

    // each.key / each.value
    if parts.len() >= 2 && parts[0] == "each" {
        match parts[1].as_str() {
            "key" => {
                return ctx
                    .each_key
                    .as_ref()
                    .map(|k| serde_json::Value::String(k.clone()))
                    .unwrap_or(serde_json::Value::Null);
            }
            "value" => {
                return ctx.each_value.clone().unwrap_or(serde_json::Value::Null);
            }
            _ => return serde_json::Value::Null,
        }
    }

    // data.TYPE.NAME.ATTR
    if parts.len() >= 4 && parts[0] == "data" {
        let address = format!("data.{}.{}", parts[1], parts[2]);
        if let Some(state) = ctx.resource_states.get(&address) {
            return traverse_json_value(state.value(), &parts[3..]);
        }
        return serde_json::Value::Null;
    }

    // resource references: TYPE.NAME.ATTR (e.g. aws_s3_bucket.public_scripts.id)
    // Also handles splat: TYPE.NAME.[*].ATTR → collects from all indexed instances
    if parts.len() >= 3 {
        let address = format!("{}.{}", parts[0], parts[1]);

        // Splat: aws_instance.main[*].id → collect attr from all indexed instances
        if parts.len() >= 4 && parts[2] == "[*]" {
            let attr_path = &parts[3..];
            let prefix = format!("{}[", address);
            let mut values: Vec<(String, serde_json::Value)> = Vec::new();
            for entry in ctx.resource_states.iter() {
                if entry.key().starts_with(&prefix) || *entry.key() == address {
                    let val = traverse_json_value(entry.value(), attr_path);
                    values.push((entry.key().clone(), val));
                }
            }
            // Sort by key to get consistent ordering (e.g. [0], [1], [2], ...)
            values.sort_by(|a, b| a.0.cmp(&b.0));
            return serde_json::Value::Array(values.into_iter().map(|(_, v)| v).collect());
        }

        if let Some(state) = ctx.resource_states.get(&address) {
            return traverse_json_value(state.value(), &parts[2..]);
        }
    }

    serde_json::Value::Null
}

/// Traverse a JSON value by attribute path.
/// e.g. ["id"] looks up state["id"], ["tags", "Name"] looks up state["tags"]["Name"]
fn traverse_json_value(value: &serde_json::Value, path: &[String]) -> serde_json::Value {
    let mut current = value;
    for key in path {
        match current {
            serde_json::Value::Object(map) => {
                if let Some(v) = map.get(key.as_str()) {
                    current = v;
                } else {
                    return serde_json::Value::Null;
                }
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    if let Some(v) = arr.get(idx) {
                        current = v;
                    } else {
                        return serde_json::Value::Null;
                    }
                } else {
                    return serde_json::Value::Null;
                }
            }
            _ => return serde_json::Value::Null,
        }
    }
    current.clone()
}

/// Resolve a literal Value to JSON, handling string interpolation in nested values.
fn resolve_value_json(val: &crate::config::types::Value, ctx: &EvalContext) -> serde_json::Value {
    use crate::config::types::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => {
            if s.contains("${") {
                resolve_interpolated_string(s, ctx)
            } else {
                serde_json::Value::String(s.clone())
            }
        }
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(|v| resolve_value_json(v, ctx)).collect())
        }
        Value::Map(entries) => {
            let map: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), resolve_value_json(v, ctx)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

/// Resolve `${...}` interpolations in a string value.
/// Handles both variable refs (${var.xxx}) and resource refs (${aws_s3_bucket.xxx.id}).
fn resolve_interpolated_string(s: &str, ctx: &EvalContext) -> serde_json::Value {
    // If the string is a single interpolation like "${aws_s3_bucket.xxx.id}",
    // return the raw value (could be non-string)
    if s.starts_with("${") && s.ends_with('}') && s.matches("${").count() == 1 {
        let ref_str = &s[2..s.len() - 1];
        let ref_parts: Vec<String> = ref_str.split('.').map(|p| p.trim().to_string()).collect();
        let resolved = resolve_reference(&ref_parts, ctx);
        if !resolved.is_null() {
            return resolved;
        }
    }

    let mut result = String::new();
    let mut remaining = s;

    while let Some(start) = remaining.find("${") {
        result.push_str(&remaining[..start]);

        if let Some(end) = remaining[start + 2..].find('}') {
            let ref_str = &remaining[start + 2..start + 2 + end];
            let ref_parts: Vec<String> = ref_str.split('.').map(|p| p.trim().to_string()).collect();
            let resolved = resolve_reference(&ref_parts, ctx);
            match resolved {
                serde_json::Value::String(s) => result.push_str(&s),
                serde_json::Value::Number(n) => result.push_str(&n.to_string()),
                serde_json::Value::Bool(b) => result.push_str(&b.to_string()),
                serde_json::Value::Null => {} // unresolved ref — skip
                _ => result.push_str(&resolved.to_string()),
            }
            remaining = &remaining[start + 2 + end + 1..];
        } else {
            result.push_str(remaining);
            remaining = "";
        }
    }
    result.push_str(remaining);

    serde_json::Value::String(result)
}

/// Build a map of variable name -> default JSON value from workspace variables.
pub fn build_variable_defaults(workspace: &WorkspaceConfig) -> HashMap<String, serde_json::Value> {
    let empty_ctx = EvalContext::plan_only(HashMap::new());
    let mut defaults = HashMap::new();
    for var in &workspace.variables {
        if let Some(ref default) = var.default {
            defaults.insert(var.name.clone(), eval_expression(default, &empty_ctx));
        }
    }
    defaults
}

/// Resolve attribute expressions to JSON, substituting variable references.
fn resolve_attributes(
    attrs: &HashMap<String, crate::config::types::Expression>,
    var_defaults: &HashMap<String, serde_json::Value>,
) -> serde_json::Value {
    let ctx = EvalContext::plan_only(var_defaults.clone());
    attributes_to_json(attrs, &ctx)
}

/// Build the full provider config object with all schema attributes.
/// cty msgpack requires ALL attributes to be present (null for unset ones).
fn build_full_provider_config(
    user_config: &serde_json::Value,
    schema: &serde_json::Value,
) -> serde_json::Value {
    let mut full = serde_json::Map::new();

    if let Some(provider_schema) = schema.get("provider") {
        if let Some(block) = provider_schema.get("block") {
            if let Some(attrs) = block.get("attributes").and_then(|a| a.as_array()) {
                for attr in attrs {
                    if let Some(name) = attr.get("name").and_then(|n| n.as_str()) {
                        let value = user_config
                            .get(name)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        full.insert(name.to_string(), value);
                    }
                }
            }
            if let Some(block_types) = block.get("block_types").and_then(|b| b.as_array()) {
                for bt in block_types {
                    if let Some(name) = bt.get("type_name").and_then(|n| n.as_str()) {
                        if !full.contains_key(name) {
                            full.insert(name.to_string(), serde_json::json!([]));
                        }
                    }
                }
            }
        }
    }

    if full.is_empty() {
        return user_config.clone();
    }

    serde_json::Value::Object(full)
}

/// Build a full resource config with all schema attributes.
/// Similar to `build_full_provider_config`, but for resource types.
/// cty msgpack requires ALL attributes to be present (null for unset/computed).
fn build_full_resource_config(
    user_config: &serde_json::Value,
    schema: &serde_json::Value,
) -> serde_json::Value {
    let mut full = serde_json::Map::new();

    if let Some(block) = schema.get("block") {
        populate_block_attributes(&mut full, block, user_config);
    }

    if full.is_empty() {
        return user_config.clone();
    }

    serde_json::Value::Object(full)
}

/// Recursively populate all attributes from a schema block.
fn populate_block_attributes(
    full: &mut serde_json::Map<String, serde_json::Value>,
    block: &serde_json::Value,
    user_config: &serde_json::Value,
) {
    // Add all attributes from schema, handling cty type coercion
    if let Some(attrs) = block.get("attributes").and_then(|a| a.as_array()) {
        for attr in attrs {
            if let Some(name) = attr.get("name").and_then(|n| n.as_str()) {
                let mut value = user_config
                    .get(name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);

                // If the cty type is list/set of objects and user provided a single object, wrap it
                if let Some(cty_type) = attr.get("type") {
                    value = coerce_value_to_cty_type(value, cty_type);
                }

                full.insert(name.to_string(), value);
            }
        }
    }

    // Add nested block types with correct defaults based on nesting mode
    // (from tfplugin5.proto): INVALID=0, SINGLE=1, LIST=2, SET=3, MAP=4, GROUP=5
    if let Some(block_types) = block.get("block_types").and_then(|b| b.as_array()) {
        for bt in block_types {
            if let Some(name) = bt.get("type_name").and_then(|n| n.as_str()) {
                let nesting = bt.get("nesting").and_then(|n| n.as_i64()).unwrap_or(2);
                let is_list_or_set = matches!(nesting, 2 | 3); // LIST=2, SET=3
                let nested_block_schema = bt.get("block");

                // Get user value from either full (if it was inserted as an attribute) or user_config
                let user_val = full.remove(name).or_else(|| user_config.get(name).cloned());

                if let Some(user_val) = user_val {
                    let val = match (is_list_or_set, &user_val) {
                        // LIST/SET: single object → wrap in array, populate sub-attrs
                        (true, serde_json::Value::Object(_)) => {
                            let populated = populate_nested_object(&user_val, nested_block_schema);
                            serde_json::Value::Array(vec![populated])
                        }
                        // LIST/SET: already an array → populate each element
                        (true, serde_json::Value::Array(arr)) => {
                            let populated: Vec<serde_json::Value> = arr
                                .iter()
                                .map(|item| populate_nested_object(item, nested_block_schema))
                                .collect();
                            serde_json::Value::Array(populated)
                        }
                        // SINGLE/GROUP: object → populate sub-attrs
                        (false, serde_json::Value::Object(_)) => {
                            populate_nested_object(&user_val, nested_block_schema)
                        }
                        _ => user_val,
                    };
                    full.insert(name.to_string(), val);
                    continue;
                }

                let default_val = match nesting {
                    1 => serde_json::Value::Null, // SINGLE → null
                    4 => serde_json::json!({}),   // MAP → empty map
                    5 => serde_json::Value::Null, // GROUP → null
                    _ => serde_json::json!([]),   // LIST(2)/SET(3) → empty array
                };
                full.insert(name.to_string(), default_val);
            }
        }
    }
}

/// Recursively populate a nested block object with all schema-defined attributes.
fn populate_nested_object(
    user_obj: &serde_json::Value,
    block_schema: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(schema) = block_schema else {
        return user_obj.clone();
    };
    if !user_obj.is_object() {
        return user_obj.clone();
    }
    let mut nested = serde_json::Map::new();
    populate_block_attributes(&mut nested, schema, user_obj);
    if nested.is_empty() {
        return user_obj.clone();
    }
    serde_json::Value::Object(nested)
}

/// Coerce a JSON value to match the expected cty type.
/// cty types are JSON-encoded, e.g.:
///   "string", "number", "bool"
///   ["list", "string"]
///   ["set", ["object", {"attr1": "string", "attr2": "number"}]]
///   ["object", {"attr1": "string"}]
///   ["map", "string"]
pub fn coerce_value_to_cty_type(
    value: serde_json::Value,
    cty_type: &serde_json::Value,
) -> serde_json::Value {
    if value.is_null() {
        return value;
    }

    match cty_type {
        // Collection types: ["list", elem], ["set", elem], ["map", elem], ["object", attrs]
        serde_json::Value::Array(arr) if arr.len() == 2 => {
            let type_name = arr[0].as_str().unwrap_or("");
            let elem_type = &arr[1];
            match type_name {
                "list" | "set" => match value {
                    // Single object → coerce and wrap in array
                    serde_json::Value::Object(_) => {
                        let coerced = coerce_value_to_cty_type(value, elem_type);
                        serde_json::Value::Array(vec![coerced])
                    }
                    // Already an array → coerce each element recursively
                    serde_json::Value::Array(items) => {
                        let coerced: Vec<serde_json::Value> = items
                            .into_iter()
                            .map(|item| coerce_value_to_cty_type(item, elem_type))
                            .collect();
                        serde_json::Value::Array(coerced)
                    }
                    _ => coerce_value_to_cty_type(value, elem_type),
                },
                "map" => {
                    if let serde_json::Value::Object(map) = value {
                        let coerced: serde_json::Map<String, serde_json::Value> = map
                            .into_iter()
                            .map(|(k, v)| (k, coerce_value_to_cty_type(v, elem_type)))
                            .collect();
                        serde_json::Value::Object(coerced)
                    } else {
                        value
                    }
                }
                "object" => {
                    // elem_type is the attribute map: {"attr1": "string", ...}
                    if let Some(attr_map) = elem_type.as_object() {
                        if let serde_json::Value::Object(mut obj) = value {
                            for (attr_name, attr_type) in attr_map {
                                if let Some(existing) = obj.remove(attr_name) {
                                    obj.insert(
                                        attr_name.clone(),
                                        coerce_value_to_cty_type(existing, attr_type),
                                    );
                                } else {
                                    obj.insert(attr_name.clone(), serde_json::Value::Null);
                                }
                            }
                            serde_json::Value::Object(obj)
                        } else {
                            value
                        }
                    } else {
                        value
                    }
                }
                _ => value,
            }
        }
        // Scalar types: "string", "number", "bool"
        serde_json::Value::String(expected) => match expected.as_str() {
            "string" => match &value {
                serde_json::Value::Number(n) => serde_json::Value::String(n.to_string()),
                serde_json::Value::Bool(b) => serde_json::Value::String(b.to_string()),
                _ => value,
            },
            "number" => match &value {
                serde_json::Value::String(s) => {
                    if let Ok(n) = s.parse::<i64>() {
                        serde_json::Value::Number(n.into())
                    } else if let Ok(n) = s.parse::<f64>() {
                        serde_json::Number::from_f64(n)
                            .map(serde_json::Value::Number)
                            .unwrap_or(value)
                    } else {
                        value
                    }
                }
                serde_json::Value::Bool(b) => {
                    serde_json::Value::Number(if *b { 1 } else { 0 }.into())
                }
                _ => value,
            },
            "bool" => match &value {
                serde_json::Value::String(s) => match s.as_str() {
                    "true" | "1" => serde_json::Value::Bool(true),
                    "false" | "0" => serde_json::Value::Bool(false),
                    _ => value,
                },
                serde_json::Value::Number(n) => {
                    serde_json::Value::Bool(n.as_f64().is_some_and(|v| v != 0.0))
                }
                _ => value,
            },
            _ => value, // Unknown scalar type (e.g., "dynamic")
        },
        _ => value,
    }
}

/// Populate a JSON object with all attributes from a cty object type definition,
/// coercing existing attribute values to match their declared types.
/// cty object type is ["object", {"attr1": "string", "attr2": "number", ...}]
/// The second element is a map of attribute names to their types.
pub fn populate_object_from_cty(
    value: serde_json::Value,
    cty_elem_type: &serde_json::Value,
) -> serde_json::Value {
    // If the element type is ["object", {attr_map}], populate missing attrs as null
    // and coerce existing attribute values to their declared types
    if let serde_json::Value::Array(arr) = cty_elem_type {
        if arr.len() == 2 && arr[0].as_str() == Some("object") {
            if let Some(attr_map) = arr[1].as_object() {
                if let serde_json::Value::Object(mut obj) = value {
                    for (attr_name, attr_type) in attr_map {
                        if let Some(existing) = obj.remove(attr_name) {
                            obj.insert(
                                attr_name.clone(),
                                coerce_value_to_cty_type(existing, attr_type),
                            );
                        } else {
                            obj.insert(attr_name.clone(), serde_json::Value::Null);
                        }
                    }
                    return serde_json::Value::Object(obj);
                }
            }
        }
    }
    value
}

/// Determine what action to take based on prior and planned state.
fn determine_action(
    prior: Option<&serde_json::Value>,
    planned: Option<&serde_json::Value>,
    requires_replace: &[String],
) -> ResourceAction {
    match (prior, planned) {
        (None, Some(_)) => ResourceAction::Create,
        (Some(_), None) => ResourceAction::Delete,
        (Some(prior), Some(planned)) => {
            if prior == planned {
                ResourceAction::NoOp
            } else if !requires_replace.is_empty() {
                ResourceAction::Replace
            } else {
                ResourceAction::Update
            }
        }
        (None, None) => ResourceAction::NoOp,
    }
}
