use std::io::Write;
use std::path::Path;

use oxid::config::loader::{detect_mode, load_workspace, ConfigMode};
use oxid::config::types::*;
use oxid::hcl::json_parser::parse_tf_json;
use oxid::hcl::parse_directory;

// ─── Helper ──────────────────────────────────────────────────────────────────

fn parse_fixture(name: &str) -> WorkspaceConfig {
    let path = format!("tests/fixtures/tf-json/{}/cdk.tf.json", name);
    let content = std::fs::read_to_string(&path).expect("fixture should exist");
    parse_tf_json(&content, Path::new(&path)).expect("parsing should succeed")
}

// ─── oxid-ia7: Credential-free fixture tests ─────────────────────────────────

#[test]
fn test_parse_tf_json_foreach() {
    let ws = parse_fixture("foreach");

    // 4 resources: null_resource.triggered, random_integer.region_seeds,
    //              random_pet.env_names, random_string.service_tokens
    assert_eq!(ws.resources.len(), 4, "expected 4 resources");
    let resource_names: Vec<&str> = ws.resources.iter().map(|r| r.name.as_str()).collect();
    assert!(resource_names.contains(&"triggered"));
    assert!(resource_names.contains(&"region_seeds"));
    assert!(resource_names.contains(&"env_names"));
    assert!(resource_names.contains(&"service_tokens"));

    // 2 providers: null, random
    assert_eq!(ws.providers.len(), 2, "expected 2 providers");
    let provider_names: Vec<&str> = ws.providers.iter().map(|p| p.name.as_str()).collect();
    assert!(provider_names.contains(&"null"));
    assert!(provider_names.contains(&"random"));

    // 2 variables: environments (list(string)), services (map(...))
    assert_eq!(ws.variables.len(), 2, "expected 2 variables");
    let env_var = ws
        .variables
        .iter()
        .find(|v| v.name == "environments")
        .unwrap();
    assert_eq!(env_var.var_type.as_deref(), Some("list(string)"));
    let services_var = ws.variables.iter().find(|v| v.name == "services").unwrap();
    assert!(services_var
        .var_type
        .as_ref()
        .unwrap()
        .starts_with("map(object("));

    // terraform settings with required_providers
    let tf = ws.terraform_settings.as_ref().expect("terraform settings");
    assert!(tf.required_providers.contains_key("null"));
    assert!(tf.required_providers.contains_key("random"));

    // Resources have for_each
    let triggered = ws.resources.iter().find(|r| r.name == "triggered").unwrap();
    assert!(
        triggered.for_each.is_some(),
        "triggered should have for_each"
    );

    let region_seeds = ws
        .resources
        .iter()
        .find(|r| r.name == "region_seeds")
        .unwrap();
    assert!(
        region_seeds.for_each.is_some(),
        "region_seeds should have for_each"
    );
}

#[test]
fn test_parse_tf_json_multi_provider() {
    let ws = parse_fixture("multi-provider");

    // 5 providers
    assert_eq!(ws.providers.len(), 5, "expected 5 providers");
    let provider_names: Vec<&str> = ws.providers.iter().map(|p| p.name.as_str()).collect();
    assert!(provider_names.contains(&"local"));
    assert!(provider_names.contains(&"null"));
    assert!(provider_names.contains(&"random"));
    assert!(provider_names.contains(&"time"));
    assert!(provider_names.contains(&"tls"));

    // locals: common_tags and environments
    assert_eq!(ws.locals.len(), 2, "expected 2 locals");
    assert!(ws.locals.contains_key("common_tags"));
    assert!(ws.locals.contains_key("environments"));

    // 8 outputs
    assert_eq!(ws.outputs.len(), 8, "expected 8 outputs");
    let output_names: Vec<&str> = ws.outputs.iter().map(|o| o.name.as_str()).collect();
    assert!(output_names.contains(&"project_full_name"));
    assert!(output_names.contains(&"api_token_value"));

    // sensitive outputs
    let api_token = ws
        .outputs
        .iter()
        .find(|o| o.name == "api_token_value")
        .unwrap();
    assert!(api_token.sensitive, "api_token_value should be sensitive");

    // output descriptions
    let ca_cert = ws
        .outputs
        .iter()
        .find(|o| o.name == "ca_cert_output")
        .unwrap();
    assert_eq!(
        ca_cert.description.as_deref(),
        Some("CA certificate in PEM format")
    );

    // resources across multiple provider types
    let resource_types: Vec<&str> = ws
        .resources
        .iter()
        .map(|r| r.resource_type.as_str())
        .collect();
    assert!(resource_types.contains(&"local_file"));
    assert!(resource_types.contains(&"null_resource"));
    assert!(resource_types.contains(&"random_pet"));
    assert!(resource_types.contains(&"tls_private_key"));
    assert!(resource_types.contains(&"time_static"));

    // depends_on present on some resources
    let config_json = ws
        .resources
        .iter()
        .find(|r| r.name == "config_json")
        .unwrap();
    assert!(
        !config_json.depends_on.is_empty(),
        "config_json should have depends_on"
    );

    // terraform settings with 5 required providers
    let tf = ws.terraform_settings.as_ref().unwrap();
    assert_eq!(tf.required_providers.len(), 5);
}

#[test]
fn test_parse_tf_json_modules() {
    let ws = parse_fixture("modules");

    // 3 modules
    assert_eq!(ws.modules.len(), 3, "expected 3 modules");
    let module_names: Vec<&str> = ws.modules.iter().map(|m| m.name.as_str()).collect();
    assert!(module_names.contains(&"app_config"));
    assert!(module_names.contains(&"config_templates"));
    assert!(module_names.contains(&"network_config"));

    // Module sources are literal strings
    let config_templates = ws
        .modules
        .iter()
        .find(|m| m.name == "config_templates")
        .unwrap();
    assert_eq!(config_templates.source, "hashicorp/dir/template");
    assert_eq!(config_templates.version.as_deref(), Some("1.0.2"));

    let network_config = ws
        .modules
        .iter()
        .find(|m| m.name == "network_config")
        .unwrap();
    assert_eq!(network_config.source, "./modules/network");

    // Module with depends_on
    let app_config = ws.modules.iter().find(|m| m.name == "app_config").unwrap();
    assert!(
        !app_config.depends_on.is_empty(),
        "app_config should have depends_on"
    );
    assert!(app_config
        .depends_on
        .contains(&"module.network_config".to_string()));

    // Resources present
    assert_eq!(ws.resources.len(), 4, "expected 4 resources");

    // Outputs present
    assert_eq!(ws.outputs.len(), 3, "expected 3 outputs");

    // Variables
    assert_eq!(ws.variables.len(), 2);
}

// ─── oxid-wbl: Comment key stripping and provider array form ─────────────────

#[test]
fn test_comment_keys_ignored() {
    // All CDKTF fixtures have "//" comment keys — verify they don't leak
    for fixture in &["foreach", "multi-provider", "modules"] {
        let ws = parse_fixture(fixture);

        // No resource should have "//" in its attributes
        for resource in &ws.resources {
            assert!(
                !resource.attributes.contains_key("//"),
                "Resource {} in {} has a '//' attribute",
                resource.name,
                fixture
            );
        }

        // No module should have "//" in its variables
        for module in &ws.modules {
            assert!(
                !module.variables.contains_key("//"),
                "Module {} in {} has a '//' variable",
                module.name,
                fixture
            );
        }

        // No local should be named "//"
        assert!(
            !ws.locals.contains_key("//"),
            "Locals in {} has a '//' key",
            fixture
        );
    }
}

#[test]
fn test_provider_array_form() {
    // CDKTF always wraps providers in arrays, even for a single provider
    let ws = parse_fixture("foreach");

    // Despite being arrays in JSON, providers should parse correctly
    assert_eq!(ws.providers.len(), 2);
    let provider_names: Vec<&str> = ws.providers.iter().map(|p| p.name.as_str()).collect();
    assert!(provider_names.contains(&"null"));
    assert!(provider_names.contains(&"random"));
}

#[test]
fn test_provider_array_form_multi() {
    // Multi-provider fixture has 5 providers all in array form
    let ws = parse_fixture("multi-provider");
    assert_eq!(ws.providers.len(), 5);
}

// ─── File discovery: parse_directory with .tf.json ───────────────────────────

#[test]
fn test_parse_directory_discovers_tf_json() {
    // parse_directory should discover .tf.json files in a directory
    let ws = parse_directory(Path::new("tests/fixtures/tf-json/foreach")).unwrap();
    assert_eq!(ws.resources.len(), 4);
    assert_eq!(ws.providers.len(), 2);
}

// ─── Inline JSON tests ──────────────────────────────────────────────────────

#[test]
fn test_parse_empty_json() {
    let content = "{}";
    let ws = parse_tf_json(content, Path::new("test.tf.json")).unwrap();
    assert!(ws.resources.is_empty());
    assert!(ws.providers.is_empty());
    assert!(ws.variables.is_empty());
    assert!(ws.outputs.is_empty());
    assert!(ws.modules.is_empty());
    assert!(ws.locals.is_empty());
    assert!(ws.terraform_settings.is_none());
}

#[test]
fn test_parse_invalid_json() {
    let content = r#"{"resource": {"aws_s3_bucket": {"my_bucket": {"bucket": "test",}}}}"#;
    let result = parse_tf_json(content, Path::new("bad.tf.json"));
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("bad.tf.json"),
        "Error should contain filename, got: {}",
        err_msg
    );
}

#[test]
fn test_parse_simple_resource() {
    let content = r#"{
        "resource": {
            "random_pet": {
                "example": {
                    "length": 3,
                    "prefix": "oxid"
                }
            }
        }
    }"#;
    let ws = parse_tf_json(content, Path::new("test.tf.json")).unwrap();
    assert_eq!(ws.resources.len(), 1);
    let resource = &ws.resources[0];
    assert_eq!(resource.resource_type, "random_pet");
    assert_eq!(resource.name, "example");
    assert!(resource.attributes.contains_key("length"));
    assert!(resource.attributes.contains_key("prefix"));
}

#[test]
fn test_parse_data_source() {
    let content = r#"{
        "data": {
            "aws_ami": {
                "latest": {
                    "most_recent": true,
                    "owners": ["amazon"]
                }
            }
        }
    }"#;
    let ws = parse_tf_json(content, Path::new("test.tf.json")).unwrap();
    assert_eq!(ws.data_sources.len(), 1);
    let ds = &ws.data_sources[0];
    assert_eq!(ds.resource_type, "aws_ami");
    assert_eq!(ds.name, "latest");
}

// ─── oxid-pwn: AWS CDKTF fixture tests ──────────────────────────────────────

#[test]
fn test_parse_tf_json_iam_grants() {
    let ws = parse_fixture("iam-grants");
    assert_eq!(ws.resources.len(), 11, "resources");
    assert_eq!(ws.data_sources.len(), 12, "data_sources");
    assert_eq!(ws.providers.len(), 1, "providers");
    assert_eq!(ws.outputs.len(), 7, "outputs");
    assert_eq!(ws.variables.len(), 1, "variables");

    // Verify provider name
    assert_eq!(ws.providers[0].name, "aws");

    // Verify data sources include IAM policy documents
    let ds_types: Vec<&str> = ws
        .data_sources
        .iter()
        .map(|d| d.resource_type.as_str())
        .collect();
    assert!(ds_types.contains(&"aws_iam_policy_document"));
    assert!(ds_types.contains(&"aws_caller_identity"));
}

#[test]
fn test_parse_tf_json_encryption() {
    let ws = parse_fixture("encryption");
    assert_eq!(ws.resources.len(), 7, "resources");
    assert_eq!(ws.data_sources.len(), 8, "data_sources");
    assert_eq!(ws.providers.len(), 1, "providers");
    assert_eq!(ws.outputs.len(), 5, "outputs");

    // Verify resource types
    let resource_types: Vec<&str> = ws
        .resources
        .iter()
        .map(|r| r.resource_type.as_str())
        .collect();
    assert!(resource_types.contains(&"aws_kms_key"));
}

#[test]
fn test_parse_tf_json_compute_events() {
    let ws = parse_fixture("compute-events");
    assert_eq!(ws.resources.len(), 13, "resources");
    assert_eq!(ws.data_sources.len(), 9, "data_sources");
    assert_eq!(ws.providers.len(), 2, "providers");
    assert_eq!(ws.outputs.len(), 5, "outputs");
}

#[test]
fn test_parse_tf_json_storage_autoscaling() {
    let ws = parse_fixture("storage-autoscaling");
    assert_eq!(ws.resources.len(), 11, "resources");
    assert_eq!(ws.data_sources.len(), 6, "data_sources");
    assert_eq!(ws.providers.len(), 1, "providers");
    assert_eq!(ws.outputs.len(), 5, "outputs");
}

#[test]
fn test_parse_tf_json_stepfunctions() {
    let ws = parse_fixture("stepfunctions");
    assert_eq!(ws.resources.len(), 13, "resources");
    assert_eq!(ws.data_sources.len(), 12, "data_sources");
    assert_eq!(ws.providers.len(), 2, "providers");
    assert_eq!(ws.outputs.len(), 1, "outputs");
}

// ─── oxid-drb: Mixed .tf + .tf.json directory tests ─────────────────────────

#[test]
fn test_parse_mixed_tf_and_tf_json() {
    let dir = tempfile::tempdir().unwrap();

    // Write a .tf file with a variable
    let tf_path = dir.path().join("main.tf");
    let mut tf_file = std::fs::File::create(&tf_path).unwrap();
    writeln!(
        tf_file,
        r#"
variable "region" {{
  default = "us-east-1"
}}

resource "null_resource" "from_hcl" {{}}
"#
    )
    .unwrap();

    // Write a .tf.json file with a resource
    let json_path = dir.path().join("extra.tf.json");
    let mut json_file = std::fs::File::create(&json_path).unwrap();
    writeln!(
        json_file,
        r#"{{
  "resource": {{
    "null_resource": {{
      "from_json": {{}}
    }}
  }},
  "output": {{
    "mixed": {{
      "value": "${{null_resource.from_hcl.id}}"
    }}
  }}
}}"#
    )
    .unwrap();

    let ws = parse_directory(dir.path()).unwrap();

    // Both resources should be present
    assert_eq!(
        ws.resources.len(),
        2,
        "expected resources from both formats"
    );
    let names: Vec<&str> = ws.resources.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"from_hcl"));
    assert!(names.contains(&"from_json"));

    // Variable from .tf and output from .tf.json
    assert_eq!(ws.variables.len(), 1);
    assert_eq!(ws.outputs.len(), 1);
    assert_eq!(ws.outputs[0].name, "mixed");
}

#[test]
fn test_parse_json_only_directory() {
    // A directory with only .tf.json files should not error
    let ws = parse_directory(Path::new("tests/fixtures/tf-json/foreach")).unwrap();
    assert!(!ws.resources.is_empty());
}

#[test]
fn test_parse_empty_directory_errors() {
    let dir = tempfile::tempdir().unwrap();
    let result = parse_directory(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("No .tf or .tf.json files found"));
}

// ─── oxid-p4r: Error handling tests ─────────────────────────────────────────

#[test]
fn test_error_invalid_json_syntax() {
    // Trailing comma — strict JSON rejects this
    let content = r#"{"resource": {},"#;
    let result = parse_tf_json(content, Path::new("syntax-error.tf.json"));
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("syntax-error.tf.json"),
        "Error should mention filename: {}",
        err
    );
}

#[test]
fn test_error_non_object_root() {
    let content = r#"[1, 2, 3]"#;
    let result = parse_tf_json(content, Path::new("array-root.tf.json"));
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("array-root.tf.json"),
        "Error should mention filename: {}",
        err
    );
}

#[test]
fn test_error_invalid_block_structure() {
    // resource value should be an object, not a string
    let content = r#"{"resource": "not-an-object"}"#;
    let result = parse_tf_json(content, Path::new("bad-structure.tf.json"));
    assert!(result.is_err());
}

#[test]
fn test_unknown_top_level_keys_ignored() {
    // Unknown top-level keys should be silently ignored (like Terraform)
    let content = r#"{
        "resource": {"null_resource": {"test": {}}},
        "unknown_block_type": {"foo": "bar"}
    }"#;
    let ws = parse_tf_json(content, Path::new("test.tf.json")).unwrap();
    assert_eq!(ws.resources.len(), 1);
}

#[test]
fn test_null_block_values_skipped() {
    // null values at any block level should be silently skipped
    let content = r#"{
        "resource": null,
        "provider": {"aws": null}
    }"#;
    let ws = parse_tf_json(content, Path::new("test.tf.json")).unwrap();
    assert!(ws.resources.is_empty());
    assert!(ws.providers.is_empty());
}

// ─── Phase 5 edge case tests ────────────────────────────────────────────────

#[test]
fn test_tf_json_ordering() {
    // Files should be processed in alphabetical order
    let dir = tempfile::tempdir().unwrap();

    // Create files in reverse order to ensure sorting works
    for (name, res_name) in &[
        ("c.tf.json", "charlie"),
        ("a.tf.json", "alpha"),
        ("b.tf.json", "bravo"),
    ] {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            r#"{{"resource": {{"null_resource": {{"{res_name}": {{}}}}}}}}"#,
        )
        .unwrap();
    }

    let ws = parse_directory(dir.path()).unwrap();
    assert_eq!(ws.resources.len(), 3);
    // Resources should appear in file order: a.tf.json (alpha), b.tf.json (bravo), c.tf.json (charlie)
    assert_eq!(ws.resources[0].name, "alpha");
    assert_eq!(ws.resources[1].name, "bravo");
    assert_eq!(ws.resources[2].name, "charlie");
}

#[test]
fn test_tfvars_json_ignored() {
    // .tfvars.json should NOT be discovered as a .tf.json file
    let dir = tempfile::tempdir().unwrap();

    let main_path = dir.path().join("main.tf.json");
    let mut main_f = std::fs::File::create(&main_path).unwrap();
    writeln!(
        main_f,
        r#"{{"resource": {{"null_resource": {{"from_main": {{}}}}}}}}"#
    )
    .unwrap();

    let tfvars_path = dir.path().join("terraform.tfvars.json");
    let mut tfvars_f = std::fs::File::create(&tfvars_path).unwrap();
    writeln!(
        tfvars_f,
        r#"{{"resource": {{"null_resource": {{"from_tfvars": {{}}}}}}}}"#
    )
    .unwrap();

    let ws = parse_directory(dir.path()).unwrap();
    assert_eq!(ws.resources.len(), 1, "only main.tf.json should be parsed");
    assert_eq!(ws.resources[0].name, "from_main");
}

#[test]
fn test_json_only_directory_mode_detection() {
    // A directory with only .tf.json files should be detected as HCL mode
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.tf.json");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(
        f,
        r#"{{"resource": {{"null_resource": {{"test": {{}}}}}}}}"#
    )
    .unwrap();

    let mode = detect_mode(dir.path());
    assert_eq!(mode, ConfigMode::Hcl);

    let ws = load_workspace(dir.path()).unwrap();
    assert_eq!(ws.resources.len(), 1);
}

// ─── .tfvars.json loading ────────────────────────────────────────────────────

fn literal_default(ws: &WorkspaceConfig, name: &str) -> Value {
    let var = ws
        .variables
        .iter()
        .find(|v| v.name == name)
        .unwrap_or_else(|| panic!("variable {} should exist", name));
    match &var.default {
        Some(Expression::Literal(v)) => v.clone(),
        other => panic!("expected literal default for {}, got {:?}", name, other),
    }
}

#[test]
fn test_auto_tfvars_json_applied() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("main.tf"),
        r#"
variable "region" {}
variable "count_nodes" {}
variable "tags" {}
variable "subnets" {}
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("network.auto.tfvars.json"),
        r#"{
  "region": "uk-london-1",
  "count_nodes": 3,
  "tags": {"env": "dev", "managed": true},
  "subnets": ["10.0.0.0/24", "10.0.1.0/24"]
}"#,
    )
    .unwrap();

    let ws = parse_directory(dir.path()).unwrap();
    assert_eq!(
        literal_default(&ws, "region"),
        Value::String("uk-london-1".into())
    );
    assert_eq!(literal_default(&ws, "count_nodes"), Value::Int(3));
    assert_eq!(
        literal_default(&ws, "tags"),
        Value::Map(vec![
            ("env".into(), Value::String("dev".into())),
            ("managed".into(), Value::Bool(true)),
        ])
    );
    assert_eq!(
        literal_default(&ws, "subnets"),
        Value::List(vec![
            Value::String("10.0.0.0/24".into()),
            Value::String("10.0.1.0/24".into()),
        ])
    );
}

#[test]
fn test_tfvars_precedence_with_json() {
    // terraform.tfvars < terraform.tfvars.json < *.auto.tfvars(.json) in
    // alphabetical order, regardless of format
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("main.tf"),
        r#"
variable "a" {}
variable "b" {}
variable "c" {}
"#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("terraform.tfvars"),
        "a = \"tfvars\"\nb = \"tfvars\"\nc = \"tfvars\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("terraform.tfvars.json"),
        r#"{"b": "tfvars_json", "c": "tfvars_json"}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("x.auto.tfvars"), "c = \"x_auto\"\n").unwrap();
    std::fs::write(
        dir.path().join("y.auto.tfvars.json"),
        r#"{"c": "y_auto_json"}"#,
    )
    .unwrap();

    let ws = parse_directory(dir.path()).unwrap();
    assert_eq!(literal_default(&ws, "a"), Value::String("tfvars".into()));
    assert_eq!(
        literal_default(&ws, "b"),
        Value::String("tfvars_json".into())
    );
    assert_eq!(
        literal_default(&ws, "c"),
        Value::String("y_auto_json".into())
    );
}

#[test]
fn test_tfvars_json_non_object_root_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("main.tf"), "variable \"a\" {}\n").unwrap();
    std::fs::write(dir.path().join("bad.auto.tfvars.json"), "[1, 2]").unwrap();

    let err = parse_directory(dir.path()).unwrap_err();
    assert!(
        format!("{:#}", err).contains("root must be a JSON object"),
        "unexpected error: {:#}",
        err
    );
}
