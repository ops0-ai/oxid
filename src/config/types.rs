use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

// ─── Top-Level Config ───────────────────────────────────────────────────────

/// Root configuration — the unified IR that both HCL and YAML parsers produce.
#[derive(Debug, Clone)]
pub struct OxidConfig {
    pub project: ProjectConfig,
    pub workspace: WorkspaceConfig,
}

/// Project-level metadata and settings.
#[derive(Debug, Clone)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,
    pub settings: Settings,
}

/// Global settings controlling execution behavior.
#[derive(Debug, Clone)]
pub struct Settings {
    pub parallelism: usize,
    pub state_backend: StateBackendConfig,
    pub working_dir: String,
    pub lock_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            parallelism: 10,
            state_backend: StateBackendConfig::Sqlite {
                path: ".oxid/state.db".to_string(),
            },
            working_dir: ".oxid".to_string(),
            lock_timeout: Duration::from_secs(300),
        }
    }
}

/// State backend selection.
#[derive(Debug, Clone)]
pub enum StateBackendConfig {
    Sqlite {
        path: String,
    },
    Postgres {
        connection_string: String,
        schema: String,
    },
}

impl Default for StateBackendConfig {
    fn default() -> Self {
        StateBackendConfig::Sqlite {
            path: "oxid.db".to_string(),
        }
    }
}

// ─── Workspace (the collection of all infrastructure in scope) ──────────────

/// A workspace holds all providers, resources, modules, variables, and outputs.
/// Both HCL (.tf) and YAML configs converge into this representation.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceConfig {
    pub providers: Vec<ProviderConfig>,
    pub resources: Vec<ResourceConfig>,
    pub data_sources: Vec<ResourceConfig>,
    pub modules: Vec<ModuleRef>,
    pub variables: Vec<VariableConfig>,
    pub outputs: Vec<OutputConfig>,
    pub locals: HashMap<String, Expression>,
    pub terraform_settings: Option<TerraformSettings>,
}

/// terraform {} block settings (required_providers, backend, etc.)
#[derive(Debug, Clone, Default)]
pub struct TerraformSettings {
    pub required_providers: HashMap<String, RequiredProvider>,
    pub required_version: Option<String>,
    pub backend: Option<BackendConfig>,
}

/// Terraform backend configuration parsed from `terraform { backend "..." { ... } }`.
#[derive(Debug, Clone)]
pub enum BackendConfig {
    S3 {
        bucket: String,
        key: String,
        region: Option<String>,
        dynamodb_table: Option<String>,
        role_arn: Option<String>,
        endpoint: Option<String>,
        profile: Option<String>,
    },
    Unsupported {
        backend_type: String,
    },
}

#[derive(Debug, Clone)]
pub struct RequiredProvider {
    pub source: String,
    pub version: Option<String>,
}

// ─── Provider ───────────────────────────────────────────────────────────────

/// A provider configuration (e.g. provider "aws" { region = "us-east-1" }).
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub name: String,
    pub source: String,
    pub version_constraint: Option<String>,
    pub alias: Option<String>,
    pub config: HashMap<String, Expression>,
}

// ─── Resource ───────────────────────────────────────────────────────────────

/// A resource definition parsed from either HCL or YAML.
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    pub resource_type: String,
    pub name: String,
    pub provider_ref: Option<String>,
    pub count: Option<Expression>,
    pub for_each: Option<Expression>,
    pub depends_on: Vec<String>,
    pub lifecycle: LifecycleConfig,
    pub attributes: HashMap<String, Expression>,
    pub provisioners: Vec<ProvisionerConfig>,
    pub source_location: Option<SourceLocation>,
}

#[derive(Debug, Clone, Default)]
pub struct LifecycleConfig {
    pub create_before_destroy: bool,
    pub prevent_destroy: bool,
    pub ignore_changes: Vec<String>,
    pub replace_triggered_by: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProvisionerConfig {
    pub provisioner_type: String,
    pub config: HashMap<String, Expression>,
    pub when: ProvisionerWhen,
}

#[derive(Debug, Clone, Default)]
pub enum ProvisionerWhen {
    #[default]
    Create,
    Destroy,
}

// ─── Module Reference ───────────────────────────────────────────────────────

/// A module block from HCL or a module definition from YAML.
#[derive(Debug, Clone)]
pub struct ModuleRef {
    pub name: String,
    pub source: String,
    pub version: Option<String>,
    pub depends_on: Vec<String>,
    pub variables: HashMap<String, Expression>,
    pub providers: HashMap<String, String>,
    pub outputs: Vec<String>,
}

// ─── Variable & Output ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct VariableConfig {
    pub name: String,
    pub var_type: Option<String>,
    pub default: Option<Expression>,
    pub description: Option<String>,
    pub sensitive: bool,
    pub validation: Vec<ValidationRule>,
}

#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub condition: Expression,
    pub error_message: String,
}

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub name: String,
    pub value: Expression,
    pub description: Option<String>,
    pub sensitive: bool,
    pub depends_on: Vec<String>,
}

// ─── Expression (the core value type) ───────────────────────────────────────

/// Expression represents any value or computation in HCL or YAML configs.
/// This is the core type that bridges both config formats.
#[derive(Debug, Clone)]
pub enum Expression {
    /// A literal value (string, number, bool, null, list, map).
    Literal(Value),

    /// A reference path like var.region, module.vpc.vpc_id, aws_vpc.main.id.
    Reference(Vec<String>),

    /// A function call like join(",", var.list).
    FunctionCall { name: String, args: Vec<Expression> },

    /// Ternary: condition ? true_val : false_val.
    Conditional {
        condition: Box<Expression>,
        true_val: Box<Expression>,
        false_val: Box<Expression>,
    },

    /// for expression: [for x in list : transform].
    ForExpr {
        collection: Box<Expression>,
        key_var: Option<String>,
        val_var: String,
        key_expr: Option<Box<Expression>>,
        value_expr: Box<Expression>,
        condition: Option<Box<Expression>>,
        grouping: bool,
    },

    /// String template with interpolations: "Hello ${var.name}".
    Template(Vec<TemplatePart>),

    /// Index access: expr[key].
    Index {
        collection: Box<Expression>,
        key: Box<Expression>,
    },

    /// Attribute access: expr.name.
    GetAttr {
        object: Box<Expression>,
        name: String,
    },

    /// Binary operation: a + b, a == b, a && b, etc.
    BinaryOp {
        op: BinOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    /// Unary operation: !a, -a.
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expression>,
    },

    /// Splat expression: aws_instance.web[*].id.
    Splat {
        source: Box<Expression>,
        each: Box<Expression>,
    },
}

/// The concrete value types.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::json!(*i),
            Value::Float(f) => serde_json::json!(*f),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::List(items) => {
                serde_json::Value::Array(items.iter().map(|v| v.to_json()).collect())
            }
            Value::Map(entries) => {
                let map: serde_json::Map<String, serde_json::Value> = entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect();
                serde_json::Value::Object(map)
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "{}", s),
            Value::List(_) => write!(f, "{}", serde_json::to_string(&self.to_json()).unwrap()),
            Value::Map(_) => write!(f, "{}", serde_json::to_string(&self.to_json()).unwrap()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TemplatePart {
    Literal(String),
    Interpolation(Box<Expression>),
    Directive(Box<Expression>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

// ─── Source Location ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SourceLocation {
    pub file: String,
    pub line: usize,
    pub column: usize,
    pub config_type: ConfigType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Hcl,
    Yaml,
}

// ─── Legacy YAML types (kept for backward compatibility during migration) ───

/// Root configuration structure parsed from oxid YAML files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlConfig {
    pub project: YamlProject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlProject {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub settings: YamlSettings,
    #[serde(default)]
    pub variables: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub modules: HashMap<String, YamlModuleConfig>,
    #[serde(default)]
    pub hooks: Option<Hooks>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlSettings {
    #[serde(default = "default_terraform_binary")]
    pub terraform_binary: String,
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
    #[serde(default = "default_state_backend")]
    pub state_backend: String,
    #[serde(default = "default_working_dir")]
    pub working_dir: String,
}

impl Default for YamlSettings {
    fn default() -> Self {
        Self {
            terraform_binary: default_terraform_binary(),
            parallelism: default_parallelism(),
            state_backend: default_state_backend(),
            working_dir: default_working_dir(),
        }
    }
}

fn default_terraform_binary() -> String {
    "terraform".to_string()
}

fn default_parallelism() -> usize {
    10
}

fn default_state_backend() -> String {
    "local".to_string()
}

fn default_working_dir() -> String {
    ".oxid".to_string()
}

/// A single module definition in YAML config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlModuleConfig {
    pub source: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub variables: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

/// Execution hooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hooks {
    #[serde(default)]
    pub pre_plan: Vec<String>,
    #[serde(default)]
    pub post_apply: Vec<String>,
    #[serde(default)]
    pub on_failure: Vec<String>,
}

// ─── Resource address helpers ───────────────────────────────────────────────

/// A fully qualified resource address like "module.vpc.aws_vpc.main" or "aws_instance.web".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceAddress {
    pub module_path: Vec<String>,
    pub resource_type: String,
    pub resource_name: String,
    pub index: Option<ResourceIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceIndex {
    Count(usize),
    /// ForEach(key, value_json) — key is the map key, value_json is the serialized value.
    ForEach(String, String),
}

impl ResourceAddress {
    pub fn new(resource_type: &str, resource_name: &str) -> Self {
        Self {
            module_path: vec![],
            resource_type: resource_type.to_string(),
            resource_name: resource_name.to_string(),
            index: None,
        }
    }

    pub fn with_module(mut self, module: &str) -> Self {
        self.module_path.push(module.to_string());
        self
    }

    pub fn format_address(&self) -> String {
        let mut parts = Vec::new();
        for m in &self.module_path {
            parts.push(format!("module.{}", m));
        }
        parts.push(format!("{}.{}", self.resource_type, self.resource_name));
        let base = parts.join(".");
        match &self.index {
            Some(ResourceIndex::Count(i)) => format!("{}[{}]", base, i),
            Some(ResourceIndex::ForEach(k, _)) => format!("{}[\"{}\"]", base, k),
            None => base,
        }
    }

    /// Parse a resource address string like "module.vpc.aws_vpc.main" or "aws_vpc.main[0]".
    pub fn parse(s: &str) -> Option<Self> {
        let mut modules = Vec::new();
        let mut remaining = s;

        // Extract module path prefix
        while remaining.starts_with("module.") {
            remaining = &remaining[7..]; // skip "module."
            let dot_pos = remaining.find('.')?;
            modules.push(remaining[..dot_pos].to_string());
            remaining = &remaining[dot_pos + 1..];
        }

        // Parse index suffix if present
        let (main_part, index) = if let Some(bracket_pos) = remaining.find('[') {
            let idx_str = &remaining[bracket_pos + 1..remaining.len() - 1];
            let index = if idx_str.starts_with('"') {
                ResourceIndex::ForEach(idx_str.trim_matches('"').to_string(), String::new())
            } else {
                ResourceIndex::Count(idx_str.parse().ok()?)
            };
            (&remaining[..bracket_pos], Some(index))
        } else {
            (remaining, None)
        };

        let dot_pos = main_part.find('.')?;
        let resource_type = main_part[..dot_pos].to_string();
        let resource_name = main_part[dot_pos + 1..].to_string();

        Some(Self {
            module_path: modules,
            resource_type,
            resource_name,
            index,
        })
    }
}

impl fmt::Display for ResourceAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_address())
    }
}
