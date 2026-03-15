#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

/// Reset SIGPIPE to default behavior so piping (e.g. `oxid graph | dot`) exits cleanly
/// instead of panicking on broken pipe.
#[cfg(unix)]
fn reset_sigpipe() {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use tracing_subscriber::EnvFilter;

mod config;
mod dag;
mod executor;
mod hcl;
mod output;
mod planner;
mod provider;
mod state;

use config::loader;
use executor::engine::ResourceEngine;
use provider::manager::ProviderManager;
use config::types::StateBackendConfig;
use state::backend::StateBackend;
use state::models::{ResourceFilter, ResourceState};
use state::query::{execute_query, QueryFormat};
use state::sqlite::SqliteBackend;
#[cfg(feature = "postgres")]
use state::postgres::PostgresBackend;

/// oxid - Standalone infrastructure engine
#[derive(Parser)]
#[command(name = "oxid", version, about, long_about = None)]
struct Cli {
    /// Path to config directory or file (auto-detects .tf and .yaml)
    #[arg(short, long, default_value = ".")]
    config: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Working directory for .oxid state and cache
    #[arg(short, long, default_value = ".oxid")]
    working_dir: String,

    /// Maximum parallelism for resource operations
    #[arg(short, long, default_value = "10")]
    parallelism: usize,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize project — download providers, create state database
    Init,

    /// Show execution plan (resource-level create/update/delete)
    Plan {
        /// Plan only specific resource address(es)
        #[arg(short, long)]
        target: Vec<String>,

        /// Output as JSON (machine-parseable)
        #[arg(long)]
        json: bool,

        /// Refresh state from cloud providers before planning (default: true)
        #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
        refresh: bool,
    },

    /// Apply infrastructure changes with resource-level parallelism
    Apply {
        /// Apply only specific resource address(es)
        #[arg(short, long)]
        target: Vec<String>,

        /// Skip confirmation prompt
        #[arg(long)]
        auto_approve: bool,
    },

    /// Destroy infrastructure in reverse dependency order
    Destroy {
        /// Destroy only specific resource address(es)
        #[arg(short, long)]
        target: Vec<String>,

        /// Skip confirmation prompt
        #[arg(long)]
        auto_approve: bool,
    },

    /// Manage state
    State {
        #[command(subcommand)]
        command: StateCommands,
    },

    /// Import existing infrastructure
    Import {
        #[command(subcommand)]
        command: ImportCommands,
    },

    /// Run a SQL query against the state database
    Query {
        /// SQL query to execute (SELECT only)
        sql: String,

        /// Output format: table, json, csv
        #[arg(short, long, default_value = "table")]
        format: String,
    },

    /// Manage workspaces
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommands,
    },

    /// Show dependency graph as DOT
    Graph {
        /// Graph type: resource or module
        #[arg(short = 'T', long, default_value = "resource")]
        graph_type: String,
    },

    /// List providers and their versions
    Providers,

    /// Detect drift between state and real infrastructure
    Drift {
        /// Skip provider refresh (only compare config vs state addresses)
        #[arg(long)]
        no_refresh: bool,
    },

    /// Validate configuration without running anything
    Validate,
}

#[derive(Subcommand)]
enum StateCommands {
    /// List all resources in state
    List {
        /// Filter by resource type (e.g. aws_vpc)
        #[arg(long)]
        filter: Option<String>,
    },

    /// Show details for a specific resource
    Show {
        /// Resource address (e.g. aws_instance.web)
        address: String,
    },

    /// Remove a resource from state without destroying it
    Rm {
        /// Resource address to remove
        address: String,
    },

    /// Move a resource to a new address in state
    Mv {
        /// Source resource address
        source: String,
        /// Destination resource address
        destination: String,
    },
}

#[derive(Subcommand)]
enum ImportCommands {
    /// Import from a .tfstate file (local path or auto-detect remote backend)
    Tfstate {
        /// Path to .tfstate file (omit to auto-detect backend from .tf files)
        path: Option<String>,
    },

    /// Import a single resource by provider ID
    Resource {
        /// Resource address (e.g. aws_instance.web)
        address: String,
        /// Provider resource ID
        id: String,
    },
}

#[derive(Subcommand)]
enum WorkspaceCommands {
    /// List all workspaces
    List,
    /// Create a new workspace
    New {
        /// Workspace name
        name: String,
    },
    /// Select a workspace
    Select {
        /// Workspace name
        name: String,
    },
    /// Delete a workspace
    Delete {
        /// Workspace name
        name: String,
    },
}

const DEFAULT_WORKSPACE: &str = "default";

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(unix)]
    reset_sigpipe();

    let cli = Cli::parse();

    let filter = if cli.verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("warn")
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    match cli.command {
        Commands::Init => cmd_init(&cli).await,
        Commands::Plan { ref target, json, refresh } => cmd_plan(&cli, target, json, refresh).await,
        Commands::Apply {
            ref target,
            auto_approve,
        } => cmd_apply(&cli, target, auto_approve).await,
        Commands::Destroy {
            ref target,
            auto_approve,
        } => cmd_destroy(&cli, target, auto_approve).await,
        Commands::State { ref command } => cmd_state(&cli, command).await,
        Commands::Import { ref command } => cmd_import(&cli, command).await,
        Commands::Query {
            ref sql,
            ref format,
        } => cmd_query(&cli, sql, format).await,
        Commands::Workspace { ref command } => cmd_workspace(&cli, command).await,
        Commands::Graph { ref graph_type } => cmd_graph(&cli, graph_type).await,
        Commands::Providers => cmd_providers(&cli).await,
        Commands::Drift { no_refresh } => cmd_drift(&cli, !no_refresh).await,
        Commands::Validate => cmd_validate(&cli).await,
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn resolve_backend_config() -> StateBackendConfig {
    if let Ok(url) = std::env::var("OXID_DATABASE_URL") {
        let schema =
            std::env::var("OXID_DATABASE_SCHEMA").unwrap_or_else(|_| "public".to_string());
        StateBackendConfig::Postgres {
            connection_string: url,
            schema,
        }
    } else {
        StateBackendConfig::Sqlite {
            path: "oxid.db".to_string(),
        }
    }
}

async fn open_backend(working_dir: &str) -> Result<Box<dyn StateBackend>> {
    let config = resolve_backend_config();
    match config {
        StateBackendConfig::Sqlite { path } => {
            let db_path = format!("{}/{}", working_dir, path);
            eprintln!(
                "  {} Using {} backend ({})",
                "→".blue(),
                "SQLite".bold(),
                db_path
            );
            Ok(Box::new(SqliteBackend::open(&db_path)?))
        }
        #[cfg(feature = "postgres")]
        StateBackendConfig::Postgres {
            connection_string,
            schema,
        } => {
            let display_host = connection_string.split('@').last().unwrap_or("(hidden)");
            eprintln!(
                "  {} Using {} backend ({}, schema: {})",
                "→".blue(),
                "PostgreSQL".bold(),
                display_host.split('?').next().unwrap_or(display_host),
                schema
            );
            let backend = PostgresBackend::connect(&connection_string, &schema).await?;
            Ok(Box::new(backend))
        }
        #[cfg(not(feature = "postgres"))]
        StateBackendConfig::Postgres { .. } => {
            bail!("PostgreSQL backend requires the 'postgres' feature. Rebuild with: cargo build --features postgres")
        }
    }
}

fn provider_manager(working_dir: &str) -> ProviderManager {
    let cache_dir = std::path::PathBuf::from(format!("{}/providers", working_dir));
    ProviderManager::new(cache_dir)
}

// ─── Commands ────────────────────────────────────────────────────────────────

async fn cmd_init(cli: &Cli) -> Result<()> {
    let config_path = Path::new(&cli.config);
    let working_dir = &cli.working_dir;

    // Create working directory structure
    std::fs::create_dir_all(working_dir)?;
    std::fs::create_dir_all(format!("{}/providers", working_dir))?;

    // Initialize state database
    let backend = open_backend(working_dir).await?;
    backend.initialize().await?;

    // Create default workspace
    match backend.get_workspace(DEFAULT_WORKSPACE).await? {
        Some(_) => {}
        None => {
            backend.create_workspace(DEFAULT_WORKSPACE).await?;
        }
    }

    // Load config and download providers if config exists
    let mode = loader::detect_mode(config_path);
    if mode != loader::ConfigMode::Yaml || config_path.exists() {
        match loader::load_workspace(config_path) {
            Ok(workspace) => {
                let pm = provider_manager(working_dir);
                let mut downloaded = 0;
                for provider in &workspace.providers {
                    let version = provider.version_constraint.as_deref().unwrap_or(">= 0.0.0");
                    tracing::info!(
                        provider = %provider.source,
                        version = %version,
                        "Downloading provider"
                    );
                    match pm.ensure_provider(&provider.source, version).await {
                        Ok(path) => {
                            println!(
                                "  {} {} ({})",
                                "+".green(),
                                provider.source.bold(),
                                path.display()
                            );
                            downloaded += 1;
                        }
                        Err(e) => {
                            println!("  {} {} — {}", "!".yellow(), provider.source.bold(), e);
                        }
                    }
                }
                if downloaded > 0 {
                    println!();
                    println!(
                        "{} Downloaded {} provider(s).",
                        "✓".green().bold(),
                        downloaded
                    );
                }
            }
            Err(_) => {
                // No config found yet — that's fine for init
            }
        }
    }

    // Auto-import existing Terraform state if present
    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("Default workspace missing after init")?;
    let existing_count = backend.count_resources(&ws.id).await?;

    if existing_count == 0 {
        // 1. Check for local terraform.tfstate
        let local_tfstate = config_path.join("terraform.tfstate");
        let local_tfstate = if local_tfstate.exists() {
            Some(local_tfstate)
        } else if Path::new("terraform.tfstate").exists() {
            Some(Path::new("terraform.tfstate").to_path_buf())
        } else {
            None
        };

        if let Some(tfstate_path) = local_tfstate {
            println!();
            println!(
                "  {} Found existing Terraform state: {}",
                "→".blue(),
                tfstate_path.display()
            );
            match std::fs::read_to_string(&tfstate_path) {
                Ok(state_json) => {
                    let result = backend.import_tfstate(&ws.id, &state_json).await?;
                    println!(
                        "  {} Imported {} resource(s) from local tfstate",
                        "✓".green().bold(),
                        result.imported
                    );
                    if result.skipped > 0 {
                        println!(
                            "  {} Skipped {} (already exist)",
                            "!".yellow(),
                            result.skipped
                        );
                    }
                }
                Err(e) => {
                    println!(
                        "  {} Could not read {}: {}",
                        "!".yellow(),
                        tfstate_path.display(),
                        e
                    );
                }
            }
        } else {
            // 2. Try to detect remote backend from .tf files and fetch state
            if let Ok(workspace_config) = crate::hcl::parse_directory(config_path) {
                if let Some(remote_backend) = workspace_config
                    .terraform_settings
                    .as_ref()
                    .and_then(|ts| ts.backend.as_ref())
                {
                    println!();
                    match remote_backend {
                        crate::config::types::BackendConfig::S3 {
                            bucket,
                            key,
                            region,
                            ..
                        } => {
                            println!(
                                "  {} Detected Terraform S3 backend: s3://{}/{}",
                                "→".blue(),
                                bucket,
                                key
                            );
                            println!("{}", "  Fetching remote state...".dimmed());
                            match crate::state::remote::fetch_remote_state(remote_backend).await {
                                Ok(state_json) => {
                                    let result =
                                        backend.import_tfstate(&ws.id, &state_json).await?;
                                    println!(
                                        "  {} Imported {} resource(s) from S3 backend (region: {})",
                                        "✓".green().bold(),
                                        result.imported,
                                        region.as_deref().unwrap_or("default")
                                    );
                                    if result.skipped > 0 {
                                        println!(
                                            "  {} Skipped {} (already exist)",
                                            "!".yellow(),
                                            result.skipped
                                        );
                                    }
                                }
                                Err(e) => {
                                    println!(
                                        "  {} Could not fetch remote state: {}",
                                        "!".yellow(),
                                        e
                                    );
                                    println!(
                                        "  {}",
                                        "You can import manually later: oxid import tfstate"
                                            .dimmed()
                                    );
                                }
                            }
                        }
                        crate::config::types::BackendConfig::Unsupported { backend_type } => {
                            println!(
                                "  {} Detected Terraform {} backend (auto-import not supported yet)",
                                "!".yellow(),
                                backend_type
                            );
                            println!(
                                "  {}",
                                "Export manually: terraform state pull > terraform.tfstate && oxid import tfstate terraform.tfstate"
                                    .dimmed()
                            );
                        }
                    }
                }
            }
        }
    }

    output::formatter::print_success("Project initialized successfully.");
    Ok(())
}

async fn cmd_plan(cli: &Cli, targets: &[String], json: bool, refresh: bool) -> Result<()> {
    let workspace = loader::load_workspace(Path::new(&cli.config))?;

    // Validate count/for_each references before planning
    let validation_errors = dag::validation::validate_count_references(&workspace);
    if !validation_errors.is_empty() {
        dag::validation::print_validation_errors(&validation_errors);
        bail!("Validation failed.");
    }

    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    let pm = Arc::new(provider_manager(&cli.working_dir));
    let engine = ResourceEngine::new(pm, cli.parallelism);

    let plan = engine.plan(&workspace, &*backend, &ws.id, refresh).await?;
    engine.shutdown().await?;

    if json {
        output::formatter::print_plan_json(&plan);
    } else {
        output::formatter::print_resource_plan(&plan, targets);
    }
    Ok(())
}

async fn cmd_apply(cli: &Cli, targets: &[String], auto_approve: bool) -> Result<()> {
    let workspace = loader::load_workspace(Path::new(&cli.config))?;

    // Validate count/for_each references before applying
    let validation_errors = dag::validation::validate_count_references(&workspace);
    if !validation_errors.is_empty() {
        dag::validation::print_validation_errors(&validation_errors);
        bail!("Validation failed.");
    }

    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    let pm = Arc::new(provider_manager(&cli.working_dir));
    let engine = ResourceEngine::new(pm, cli.parallelism);

    // Plan first
    let plan = engine.plan(&workspace, &*backend, &ws.id, true).await?;
    output::formatter::print_resource_plan(&plan, targets);

    if plan.creates == 0 && plan.updates == 0 && plan.deletes == 0 && plan.replaces == 0 {
        println!("\n{}", "No changes. Infrastructure is up-to-date.".green());
        engine.shutdown().await?;
        return Ok(());
    }

    // Confirm
    if !auto_approve {
        println!(
            "\nDo you want to perform these actions? Only '{}' will be accepted.",
            "yes".bold()
        );
        print!("  Enter a value: ");
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "yes" {
            println!("\n{}", "Apply cancelled.".yellow());
            engine.shutdown().await?;
            return Ok(());
        }
    }

    // Record run
    let run_id = backend
        .start_run(
            &ws.id,
            "apply",
            (plan.creates + plan.updates + plan.deletes) as i32,
        )
        .await?;

    // Apply
    let backend_arc: Arc<dyn StateBackend> = Arc::from(backend);
    let summary = engine
        .apply(&workspace, Arc::clone(&backend_arc), &ws.id, &plan)
        .await?;

    // Complete run
    let status = if summary.failed == 0 {
        "succeeded"
    } else {
        "failed"
    };
    let total_succeeded = (summary.added + summary.changed + summary.destroyed) as i32;
    backend_arc
        .complete_run(&run_id, status, total_succeeded, summary.failed as i32)
        .await?;

    engine.shutdown().await?;

    // Print summary
    println!();
    println!("{}", summary);

    // Evaluate and print outputs
    if !workspace.outputs.is_empty() && summary.failed == 0 {
        // Load all resource states from the backend into a DashMap for expression evaluation
        let resource_states: Arc<dashmap::DashMap<String, serde_json::Value>> =
            Arc::new(dashmap::DashMap::new());
        let all_resources = backend_arc
            .list_resources(&ws.id, &crate::state::models::ResourceFilter::default())
            .await?;
        for r in &all_resources {
            if let Ok(attrs) = serde_json::from_str::<serde_json::Value>(&r.attributes_json) {
                resource_states.insert(r.address.clone(), attrs);
            }
        }

        let var_defaults = executor::engine::build_variable_defaults(&workspace);
        let eval_ctx =
            executor::engine::EvalContext::with_states(var_defaults, Arc::clone(&resource_states));

        println!();
        println!("{}:", "Outputs".bold());
        println!();
        let name_width = workspace
            .outputs
            .iter()
            .map(|o| o.name.len())
            .max()
            .unwrap_or(10);

        for output in &workspace.outputs {
            let value = executor::engine::eval_expression(&output.value, &eval_ctx);
            let display = if output.sensitive {
                "<sensitive>".to_string()
            } else {
                output::formatter::format_output_value(&value, 0)
            };
            println!("{:<width$} = {}", output.name, display, width = name_width);
        }
    }

    Ok(())
}

async fn cmd_destroy(cli: &Cli, _targets: &[String], auto_approve: bool) -> Result<()> {
    let workspace = loader::load_workspace(Path::new(&cli.config))?;
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    // Show what will be destroyed
    let resource_count = backend.count_resources(&ws.id).await?;
    if resource_count == 0 {
        println!("{}", "No resources in state. Nothing to destroy.".dimmed());
        return Ok(());
    }

    // List resources that will be destroyed
    let resources = backend
        .list_resources(&ws.id, &crate::state::models::ResourceFilter::default())
        .await?;

    println!("\nDestruction Plan");
    println!("{}", "─".repeat(60));
    for r in &resources {
        println!("  {} {}", "-".red().bold(), r.address.red());
    }
    println!("{}", "─".repeat(60));
    println!(
        "\n{} This will destroy {} resource(s).",
        "⚠".yellow().bold(),
        resource_count.to_string().red().bold()
    );

    if !auto_approve {
        println!(
            "\nDo you really want to destroy all resources? Only '{}' will be accepted.",
            "yes".bold()
        );
        print!("  Enter a value: ");
        use std::io::Write;
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "yes" {
            println!("\n{}", "Destroy cancelled.".yellow());
            return Ok(());
        }
    }

    let pm = Arc::new(provider_manager(&cli.working_dir));
    let engine = ResourceEngine::new(pm, cli.parallelism);

    let run_id = backend
        .start_run(&ws.id, "destroy", resource_count as i32)
        .await?;

    let backend_arc: Arc<dyn StateBackend> = Arc::from(backend);
    let summary = engine
        .destroy(&workspace, Arc::clone(&backend_arc), &ws.id)
        .await?;

    let status = if summary.failed == 0 {
        "succeeded"
    } else {
        "failed"
    };
    backend_arc
        .complete_run(
            &run_id,
            status,
            summary.destroyed as i32,
            summary.failed as i32,
        )
        .await?;

    engine.shutdown().await?;

    // Print summary
    println!();
    println!("{}", summary);

    Ok(())
}

async fn cmd_state(cli: &Cli, command: &StateCommands) -> Result<()> {
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    match command {
        StateCommands::List { filter } => {
            let resource_filter = if let Some(f) = filter {
                // Parse filter like "type=aws_vpc" or "status=created"
                let mut rf = ResourceFilter::default();
                for part in f.split(',') {
                    let kv: Vec<&str> = part.splitn(2, '=').collect();
                    if kv.len() == 2 {
                        match kv[0].trim() {
                            "type" => rf.resource_type = Some(kv[1].trim().to_string()),
                            "module" => rf.module_path = Some(kv[1].trim().to_string()),
                            "status" => rf.status = Some(kv[1].trim().to_string()),
                            _ => {}
                        }
                    }
                }
                rf
            } else {
                ResourceFilter::default()
            };

            let resources = backend.list_resources(&ws.id, &resource_filter).await?;
            output::formatter::print_resource_list(&resources);
        }

        StateCommands::Show { address } => {
            let resource = backend
                .get_resource(&ws.id, address)
                .await?
                .context(format!("Resource '{}' not found in state.", address))?;
            output::formatter::print_resource_detail(&resource);
        }

        StateCommands::Rm { address } => {
            let resource = backend.get_resource(&ws.id, address).await?;
            if resource.is_none() {
                bail!("Resource '{}' not found in state.", address);
            }
            backend.delete_resource(&ws.id, address).await?;
            output::formatter::print_success(&format!(
                "Removed {} from state (infrastructure unchanged).",
                address
            ));
        }

        StateCommands::Mv {
            source,
            destination,
        } => {
            let resource = backend
                .get_resource(&ws.id, source)
                .await?
                .context(format!("Source resource '{}' not found in state.", source))?;

            // Check destination doesn't exist
            if backend.get_resource(&ws.id, destination).await?.is_some() {
                bail!(
                    "Destination resource '{}' already exists in state.",
                    destination
                );
            }

            // Create at new address, delete old
            let mut moved = resource.clone();
            moved.address = destination.clone();
            moved.id = uuid::Uuid::new_v4().to_string();
            moved.updated_at = chrono::Utc::now().to_rfc3339();
            backend.upsert_resource(&moved).await?;
            backend.delete_resource(&ws.id, source).await?;

            output::formatter::print_success(&format!("Moved {} → {}", source, destination));
        }
    }

    Ok(())
}

async fn cmd_import(cli: &Cli, command: &ImportCommands) -> Result<()> {
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    match command {
        ImportCommands::Tfstate { path } => {
            let state_json = match path {
                Some(file_path) => std::fs::read_to_string(file_path)
                    .context(format!("Failed to read tfstate file: {}", file_path))?,
                None => {
                    println!(
                        "{}",
                        "Auto-detecting Terraform backend from .tf files...".dimmed()
                    );

                    let config_path = std::path::Path::new(&cli.config);
                    let workspace_config = crate::hcl::parse_directory(config_path)
                        .context("Failed to load .tf configuration for backend detection")?;

                    let remote_backend = workspace_config
                        .terraform_settings
                        .as_ref()
                        .and_then(|ts| ts.backend.as_ref())
                        .context(
                            "No backend configuration found in .tf files.\n\
                             Expected: terraform { backend \"s3\" { ... } }\n\
                             Alternatively, specify a local file: oxid import tfstate <path>",
                        )?;

                    match remote_backend {
                        crate::config::types::BackendConfig::S3 {
                            bucket,
                            key,
                            region,
                            ..
                        } => {
                            println!(
                                "  {} Detected S3 backend: s3://{}/{}  (region: {})",
                                "->".blue(),
                                bucket,
                                key,
                                region.as_deref().unwrap_or("default")
                            );
                        }
                        crate::config::types::BackendConfig::Unsupported { backend_type } => {
                            println!("  {} Detected backend: {}", "->".blue(), backend_type);
                        }
                    }

                    println!("{}", "Fetching remote state...".dimmed());
                    crate::state::remote::fetch_remote_state(remote_backend).await?
                }
            };

            let result = backend.import_tfstate(&ws.id, &state_json).await?;

            println!();
            println!("{}", "Import Results".bold().cyan());
            println!("{}", "─".repeat(40));
            println!(
                "  {} {}",
                "Imported:".bold(),
                result.imported.to_string().green()
            );
            println!(
                "  {} {}",
                "Skipped:".bold(),
                result.skipped.to_string().yellow()
            );
            if !result.warnings.is_empty() {
                println!("  {}:", "Warnings".bold().yellow());
                for w in &result.warnings {
                    println!("    {} {}", "!".yellow(), w);
                }
            }
            println!();
        }

        ImportCommands::Resource { address, id } => {
            // Parse address to get resource type
            let parts: Vec<&str> = address.splitn(2, '.').collect();
            if parts.len() != 2 {
                bail!(
                    "Invalid resource address '{}'. Expected format: type.name",
                    address
                );
            }
            let resource_type = parts[0];
            let resource_name = parts[1];

            let workspace = loader::load_workspace(Path::new(&cli.config))?;

            // Find the provider for this resource type
            let provider_prefix = resource_type.split('_').next().unwrap_or(resource_type);
            let provider_source = workspace
                .providers
                .iter()
                .find(|p| p.name == provider_prefix || p.source.contains(provider_prefix))
                .map(|p| p.source.clone())
                .context(format!(
                    "No provider found for resource type '{}'",
                    resource_type
                ))?;

            let pm = Arc::new(provider_manager(&cli.working_dir));
            let engine = ResourceEngine::new(pm, cli.parallelism);

            // Use the provider's ImportResourceState RPC
            // For now, create a resource state entry with the provider ID
            let mut resource = ResourceState::new(&ws.id, resource_type, resource_name, address);
            resource.provider_source = provider_source;
            resource.status = "created".to_string();
            resource.attributes_json = serde_json::json!({ "id": id }).to_string();

            backend.upsert_resource(&resource).await?;
            engine.shutdown().await?;

            output::formatter::print_success(&format!("Imported {} (id: {}).", address, id));
        }
    }

    Ok(())
}

async fn cmd_query(cli: &Cli, sql: &str, format: &str) -> Result<()> {
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let fmt = QueryFormat::parse(format);
    let result = execute_query(&*backend, sql, fmt).await?;
    println!("{}", result);
    Ok(())
}

async fn cmd_workspace(cli: &Cli, command: &WorkspaceCommands) -> Result<()> {
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    match command {
        WorkspaceCommands::List => {
            let workspaces = backend.list_workspaces().await?;
            if workspaces.is_empty() {
                println!("{}", "No workspaces.".dimmed());
                return Ok(());
            }
            println!();
            println!("{}", "Workspaces".bold().cyan());
            println!("{}", "─".repeat(40));
            for ws in &workspaces {
                let marker = if ws.name == DEFAULT_WORKSPACE {
                    "*".green().to_string()
                } else {
                    " ".to_string()
                };
                println!(" {} {}", marker, ws.name.bold());
            }
            println!();
        }

        WorkspaceCommands::New { name } => {
            if backend.get_workspace(name).await?.is_some() {
                bail!("Workspace '{}' already exists.", name);
            }
            backend.create_workspace(name).await?;
            output::formatter::print_success(&format!("Created workspace '{}'.", name));
        }

        WorkspaceCommands::Select { name } => {
            backend
                .get_workspace(name)
                .await?
                .context(format!("Workspace '{}' not found.", name))?;
            // Write selected workspace to a file
            let ws_file = format!("{}/.workspace", cli.working_dir);
            std::fs::write(&ws_file, name)?;
            output::formatter::print_success(&format!("Switched to workspace '{}'.", name));
        }

        WorkspaceCommands::Delete { name } => {
            if name == DEFAULT_WORKSPACE {
                bail!("Cannot delete the default workspace.");
            }
            backend
                .get_workspace(name)
                .await?
                .context(format!("Workspace '{}' not found.", name))?;
            backend.delete_workspace(name).await?;
            output::formatter::print_success(&format!("Deleted workspace '{}'.", name));
        }
    }

    Ok(())
}

async fn cmd_graph(cli: &Cli, graph_type: &str) -> Result<()> {
    let workspace = loader::load_workspace(Path::new(&cli.config))?;

    match graph_type {
        "resource" => {
            let provider_map = executor::engine::build_provider_map(&workspace);
            let var_defaults = executor::engine::build_variable_defaults(&workspace);
            let (graph, _) =
                dag::resource_graph::build_resource_dag(&workspace, &provider_map, &var_defaults)?;
            let dot = dag::resource_graph::to_dot(&graph);
            println!("{}", dot);
        }
        "module" => {
            // Fall back to the legacy module-level DAG for YAML configs
            let cfg = config::parser::load_config(&cli.config)?;
            let graph = dag::builder::build_dag(&cfg)?;
            let dot = dag::visualizer::to_dot(&graph);
            println!("{}", dot);
        }
        _ => bail!(
            "Unknown graph type '{}'. Use 'resource' or 'module'.",
            graph_type
        ),
    }

    Ok(())
}

async fn cmd_providers(cli: &Cli) -> Result<()> {
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    let providers = backend.list_providers(&ws.id).await?;

    if providers.is_empty() {
        // Try loading from config
        match loader::load_workspace(Path::new(&cli.config)) {
            Ok(workspace) if !workspace.providers.is_empty() => {
                println!();
                println!("{}", "Configured Providers".bold().cyan());
                println!("{}", "─".repeat(50));
                for p in &workspace.providers {
                    let version = p.version_constraint.as_deref().unwrap_or("any");
                    println!(
                        "  {} {} {}",
                        "→".blue(),
                        p.source.bold(),
                        format!("({})", version).dimmed()
                    );
                }
                println!();
                println!("{}", "Run 'oxid init' to download providers.".dimmed());
            }
            _ => {
                println!("{}", "No providers configured.".dimmed());
            }
        }
        return Ok(());
    }

    println!();
    println!("{}", "Installed Providers".bold().cyan());
    println!("{}", "─".repeat(50));
    for (_id, source, version) in &providers {
        println!(
            "  {} {} {}",
            "✓".green(),
            source.bold(),
            format!("v{}", version).dimmed()
        );
    }
    println!();

    Ok(())
}

async fn cmd_drift(cli: &Cli, refresh: bool) -> Result<()> {
    let workspace = loader::load_workspace(Path::new(&cli.config))?;
    let backend = open_backend(&cli.working_dir).await?;
    backend.initialize().await?;

    let ws = backend
        .get_workspace(DEFAULT_WORKSPACE)
        .await?
        .context("No default workspace. Run 'oxid init' first.")?;

    // Collect attribute-level drift: (address, vec of (attr_name, stored_val, cloud_val))
    let mut attribute_drifts: Vec<(String, Vec<(String, String, String)>)> = Vec::new();
    let mut deleted_resources: Vec<String> = Vec::new();

    // Refresh from cloud to detect attribute-level drift
    if refresh {
        println!("{}", "Refreshing state from providers...".dimmed());
        let pm = Arc::new(provider_manager(&cli.working_dir));
        let engine = ResourceEngine::new(pm, cli.parallelism);

        // Initialize and configure providers (connect, get schema, configure with region/creds)
        engine.initialize_providers(&workspace).await?;

        // Read each resource from the provider, compare, and update state
        let resources = backend
            .list_resources(&ws.id, &ResourceFilter::default())
            .await?;
        let mut refreshed = 0;
        for resource in &resources {
            if resource.provider_source.is_empty() {
                continue;
            }
            let stored_state: serde_json::Value =
                serde_json::from_str(&resource.attributes_json).unwrap_or_default();

            // Pad with schema so ReadResource gets all required attributes
            let padded_state = if let Ok(Some(ref schema)) = engine
                .provider_manager()
                .get_resource_schema(&resource.provider_source, &resource.resource_type)
                .await
            {
                executor::engine::build_full_resource_config(&stored_state, schema)
            } else {
                stored_state.clone()
            };

            match engine
                .provider_manager()
                .read_resource(&resource.provider_source, &resource.resource_type, &padded_state)
                .await
            {
                Ok(Some(cloud_state)) => {
                    // Compare stored state vs cloud state — read-only, don't update DB
                    let changed_attrs = diff_attributes(&stored_state, &cloud_state);
                    if !changed_attrs.is_empty() {
                        attribute_drifts.push((resource.address.clone(), changed_attrs));
                    }
                    refreshed += 1;
                }
                Ok(None) => {
                    // Resource deleted outside of oxid
                    deleted_resources.push(resource.address.clone());
                }
                Err(e) => {
                    tracing::warn!(
                        address = %resource.address,
                        error = %e,
                        "Failed to refresh resource"
                    );
                }
            }
        }

        engine.shutdown().await?;
        if refreshed > 0 {
            println!("  {} Refreshed {} resource(s).\n", "✓".green(), refreshed);
        }
    }

    // Compare config vs state for address-level drift
    let resources = backend
        .list_resources(&ws.id, &ResourceFilter::default())
        .await?;

    // Resources in config (expanded for count/for_each)
    let var_defaults = executor::engine::build_variable_defaults(&workspace);
    let mut config_addresses: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for r in &workspace.resources {
        let base = format!("{}.{}", r.resource_type, r.name);
        if let Some(ref count_expr) = r.count {
            let ctx = executor::engine::EvalContext::plan_only(var_defaults.clone());
            let val = executor::engine::eval_expression(count_expr, &ctx);
            if let Some(n) = val.as_u64() {
                for i in 0..n as usize {
                    config_addresses.insert(format!("{}[{}]", base, i));
                }
            } else {
                config_addresses.insert(base);
            }
        } else if let Some(ref for_each_expr) = r.for_each {
            let ctx = executor::engine::EvalContext::plan_only(var_defaults.clone());
            let val = executor::engine::eval_expression(for_each_expr, &ctx);
            match val {
                serde_json::Value::Object(map) => {
                    for key in map.keys() {
                        config_addresses.insert(format!("{}[\"{}\"]", base, key));
                    }
                }
                serde_json::Value::Array(arr) => {
                    for v in &arr {
                        let key = v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string());
                        config_addresses.insert(format!("{}[\"{}\"]", base, key));
                    }
                }
                _ => {
                    config_addresses.insert(base);
                }
            }
        } else {
            config_addresses.insert(base);
        }
    }

    // Resources in state
    let state_addresses: std::collections::HashSet<String> =
        resources.iter().map(|r| r.address.clone()).collect();

    let mut drifts: Vec<(String, String, String)> = Vec::new();

    // New in config, not in state
    for addr in config_addresses.difference(&state_addresses) {
        drifts.push(("+".to_string(), addr.clone(), "new resource in config".to_string()));
    }

    // In state, not in config (or deleted from cloud)
    for addr in state_addresses.difference(&config_addresses) {
        drifts.push(("-".to_string(), addr.clone(), "in state but not in config".to_string()));
    }

    // Resources deleted from cloud
    for addr in &deleted_resources {
        drifts.push(("-".to_string(), addr.clone(), "deleted outside of oxid".to_string()));
    }

    let has_drift = !drifts.is_empty() || !attribute_drifts.is_empty() || !deleted_resources.is_empty();

    if !has_drift {
        output::formatter::print_success("No drift detected. Infrastructure is in sync.");
    } else {
        let total = drifts.len() + attribute_drifts.len() + deleted_resources.len();
        println!();
        println!(
            "{}",
            format!("Drift Detected ({} issue{})", total, if total == 1 { "" } else { "s" })
                .bold()
                .yellow()
        );
        println!("{}", "─".repeat(70));

        // Address-level drift (new in config / missing from config)
        for (icon, addr, detail) in &drifts {
            let colored_icon = match icon.as_str() {
                "+" => "+".green().to_string(),
                "-" => "-".red().to_string(),
                _ => icon.to_string(),
            };
            println!("  {} {} {}", colored_icon, addr.bold(), detail.dimmed());
        }

        // Resources deleted from cloud
        for addr in &deleted_resources {
            println!("  {} {} {}", "-".red(), addr.bold(), "deleted outside of oxid".red());
        }

        // Attribute-level drift with values
        for (addr, changes) in &attribute_drifts {
            println!("  {} {}", "~".yellow(), addr.bold());
            for (attr, stored_val, cloud_val) in changes {
                println!(
                    "      {} {}: {} {} {}",
                    "~".yellow(),
                    attr.cyan(),
                    stored_val.red(),
                    "→".dimmed(),
                    cloud_val.green(),
                );
            }
        }

        println!("{}", "─".repeat(70));
        println!();
    }

    Ok(())
}

/// Compare two JSON objects and return a list of (attr_name, stored_display, cloud_display) for diffs.
/// Ignores computed/read-only attributes and treats null/[]/{}/"" as equivalent empty values.
fn diff_attributes(
    stored: &serde_json::Value,
    cloud: &serde_json::Value,
) -> Vec<(String, String, String)> {
    let mut changed = Vec::new();

    let stored_obj = match stored.as_object() {
        Some(o) => o,
        None => return changed,
    };
    let cloud_obj = match cloud.as_object() {
        Some(o) => o,
        None => return changed,
    };

    // Check attributes that exist in stored state
    for (key, stored_val) in stored_obj {
        // Skip computed/read-only metadata fields that providers populate
        if key == "id" || key == "arn" || key.ends_with("_at")
            || key.ends_with("_id") || key.ends_with("_arn") || key.ends_with("_arns")
            || key == "owner_id" || key == "arn_suffix"
            || key == "dns_name" || key == "zone_id"
            || key.starts_with("private_") || key.starts_with("public_")
            || key == "association_id" || key == "network_interface"
            || key == "hosted_zone_id" || key == "domain_name"
            || key == "main_route_table_id" || key == "default_route_table_id"
            || key == "default_network_acl_id" || key == "default_security_group_id"
            || key == "ipv6_association_id" || key == "ipv6_cidr_block_network_border_group"
            || key == "instance_state" || key == "primary_network_interface_id"
            || key.ends_with("_status") || key.ends_with("_state")
        {
            continue;
        }
        if let Some(cloud_val) = cloud_obj.get(key) {
            if !json_values_equal(stored_val, cloud_val) {
                // For nested objects (like tags), drill in and show only changed sub-keys
                if let (Some(stored_map), Some(cloud_map)) =
                    (stored_val.as_object(), cloud_val.as_object())
                {
                    for (sub_key, sub_stored) in stored_map {
                        let sub_cloud = cloud_map
                            .get(sub_key)
                            .unwrap_or(&serde_json::Value::Null);
                        if !json_values_equal(sub_stored, sub_cloud) {
                            changed.push((
                                format!("{}.{}", key, sub_key),
                                format_drift_value(sub_stored),
                                format_drift_value(sub_cloud),
                            ));
                        }
                    }
                    // Keys added in cloud but not in stored
                    for (sub_key, sub_cloud) in cloud_map {
                        if !stored_map.contains_key(sub_key) {
                            changed.push((
                                format!("{}.{}", key, sub_key),
                                "(absent)".to_string(),
                                format_drift_value(sub_cloud),
                            ));
                        }
                    }
                } else {
                    changed.push((
                        key.clone(),
                        format_drift_value(stored_val),
                        format_drift_value(cloud_val),
                    ));
                }
            }
        }
    }

    changed
}

/// Format a JSON value for compact drift display.
fn format_drift_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "(null)".to_string(),
        serde_json::Value::String(s) => {
            if s.is_empty() {
                "(empty)".to_string()
            } else if s.len() > 80 {
                format!("\"{}...\"", &s[..77])
            } else {
                format!("\"{}\"", s)
            }
        }
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(a) => {
            if a.is_empty() {
                "[]".to_string()
            } else {
                let compact = serde_json::to_string(v).unwrap_or_default();
                if compact.len() > 80 {
                    format!("[{} items]", a.len())
                } else {
                    compact
                }
            }
        }
        serde_json::Value::Object(o) => {
            if o.is_empty() {
                "{}".to_string()
            } else {
                let compact = serde_json::to_string(v).unwrap_or_default();
                if compact.len() > 120 {
                    format!("{{{} keys}}", o.len())
                } else {
                    compact
                }
            }
        }
    }
}

/// Check if a JSON value is "empty" (null, empty string, empty array, empty object).
fn is_empty_value(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => true,
        serde_json::Value::String(s) => s.is_empty(),
        serde_json::Value::Array(a) => a.is_empty(),
        serde_json::Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

/// Compare two JSON values for drift.
/// Treats null, "", [], {} as equivalent empty values (AWS provider inconsistency).
fn json_values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value;

    // Both empty → equal (handles null vs [] vs {} vs "")
    if is_empty_value(a) && is_empty_value(b) {
        return true;
    }

    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return false;
            }
            a.iter().zip(b.iter()).all(|(x, y)| json_values_equal(x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            // Compare all keys from both sides
            let all_keys: std::collections::HashSet<&String> =
                a.keys().chain(b.keys()).collect();
            for key in all_keys {
                let val_a = a.get(key).unwrap_or(&Value::Null);
                let val_b = b.get(key).unwrap_or(&Value::Null);
                if !json_values_equal(val_a, val_b) {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}

async fn cmd_validate(cli: &Cli) -> Result<()> {
    let config_path = Path::new(&cli.config);
    let mode = loader::detect_mode(config_path);

    println!("  {} Config format: {:?}", "→".blue(), mode);

    let workspace = loader::load_workspace(config_path)?;

    println!(
        "  {} {} provider(s), {} resource(s), {} data source(s), {} module(s), {} variable(s), {} output(s)",
        "→".blue(),
        workspace.providers.len(),
        workspace.resources.len(),
        workspace.data_sources.len(),
        workspace.modules.len(),
        workspace.variables.len(),
        workspace.outputs.len(),
    );

    // Validate provider sources
    for provider in &workspace.providers {
        if provider.source.is_empty() {
            bail!("Provider '{}' has empty source.", provider.name);
        }
    }

    // Validate resource types
    for resource in &workspace.resources {
        if resource.resource_type.is_empty() {
            bail!("Resource '{}' has empty type.", resource.name);
        }
    }

    // Validate depends_on references
    let all_addresses: std::collections::HashSet<String> = workspace
        .resources
        .iter()
        .map(|r| format!("{}.{}", r.resource_type, r.name))
        .collect();

    for resource in &workspace.resources {
        for dep in &resource.depends_on {
            if !all_addresses.contains(dep) {
                tracing::warn!(
                    resource = format!("{}.{}", resource.resource_type, resource.name),
                    depends_on = %dep,
                    "depends_on references unknown resource"
                );
            }
        }
    }

    // Validate count/for_each references
    let validation_errors = dag::validation::validate_count_references(&workspace);
    if !validation_errors.is_empty() {
        dag::validation::print_validation_errors(&validation_errors);
        bail!("Validation failed.");
    }

    output::formatter::print_success("Configuration is valid.");
    Ok(())
}
