use oxid::config::parser::parse_config;
use oxid::config::validator::validate;

#[test]
fn test_parse_valid_config() {
    let yaml = r#"
project:
  name: "test-project"
  version: "1.0"
  settings:
    terraform_binary: "terraform"
    parallelism: 5
    working_dir: ".oxid"
  variables:
    region: "us-east-1"
  modules:
    vpc:
      source: "terraform-aws-modules/vpc/aws"
      version: "5.0.0"
      variables:
        cidr: "10.0.0.0/16"
      outputs:
        - vpc_id
    sg:
      source: "./modules/sg"
      depends_on:
        - vpc
      variables:
        vpc_id: "${module.vpc.vpc_id}"
      outputs:
        - sg_id
"#;

    let config = parse_config(yaml).expect("Should parse valid config");
    assert_eq!(config.project.name, "test-project");
    assert_eq!(config.project.modules.len(), 2);
    assert!(config.project.modules.contains_key("vpc"));
    assert!(config.project.modules.contains_key("sg"));
    assert_eq!(config.project.settings.parallelism, 5);
}

#[test]
fn test_parse_minimal_config() {
    let yaml = r#"
project:
  name: "minimal"
  version: "0.1"
  modules:
    single:
      source: "hashicorp/null"
"#;

    let config = parse_config(yaml).expect("Should parse minimal config");
    assert_eq!(config.project.name, "minimal");
    assert_eq!(config.project.modules.len(), 1);
    // Defaults should be applied
    assert_eq!(config.project.settings.terraform_binary, "terraform");
    assert_eq!(config.project.settings.parallelism, 10);
    assert_eq!(config.project.settings.working_dir, ".oxid");
}

#[test]
fn test_parse_invalid_yaml() {
    let yaml = "not: valid: yaml: [";
    let result = parse_config(yaml);
    assert!(result.is_err());
}

#[test]
fn test_validate_missing_dependency() {
    let yaml = r#"
project:
  name: "bad-deps"
  version: "1.0"
  modules:
    app:
      source: "./app"
      depends_on:
        - nonexistent
"#;

    let config = parse_config(yaml).unwrap();
    let result = validate(&config);
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("nonexistent"),
        "Error should mention the missing module"
    );
}

#[test]
fn test_validate_circular_dependency() {
    let yaml = r#"
project:
  name: "circular"
  version: "1.0"
  modules:
    a:
      source: "./a"
      depends_on:
        - b
    b:
      source: "./b"
      depends_on:
        - a
"#;

    let config = parse_config(yaml).unwrap();
    let result = validate(&config);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Circular dependency"));
}

#[test]
fn test_validate_three_node_cycle() {
    let yaml = r#"
project:
  name: "cycle3"
  version: "1.0"
  modules:
    a:
      source: "./a"
      depends_on:
        - c
    b:
      source: "./b"
      depends_on:
        - a
    c:
      source: "./c"
      depends_on:
        - b
"#;

    let config = parse_config(yaml).unwrap();
    let result = validate(&config);
    assert!(result.is_err());
}

#[test]
fn test_validate_undefined_variable_reference() {
    let yaml = r#"
project:
  name: "bad-var"
  version: "1.0"
  variables:
    region: "us-east-1"
  modules:
    vpc:
      source: "./vpc"
      variables:
        region: "${var.nonexistent_var}"
"#;

    let config = parse_config(yaml).unwrap();
    let result = validate(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("nonexistent_var"));
}

#[test]
fn test_validate_undefined_module_reference() {
    let yaml = r#"
project:
  name: "bad-mod-ref"
  version: "1.0"
  modules:
    app:
      source: "./app"
      variables:
        vpc_id: "${module.nonexistent.vpc_id}"
"#;

    let config = parse_config(yaml).unwrap();
    let result = validate(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("nonexistent"));
}

#[test]
fn test_validate_valid_complex_config() {
    let yaml = r#"
project:
  name: "complex"
  version: "1.0"
  variables:
    region: "us-east-1"
    env: "prod"
  modules:
    vpc:
      source: "terraform-aws-modules/vpc/aws"
      version: "5.0.0"
      variables:
        cidr: "10.0.0.0/16"
      outputs:
        - vpc_id
        - subnet_ids
    sg:
      source: "./modules/sg"
      depends_on:
        - vpc
      variables:
        vpc_id: "${module.vpc.vpc_id}"
        env: "${var.env}"
      outputs:
        - sg_id
    db:
      source: "terraform-aws-modules/rds/aws"
      depends_on:
        - vpc
        - sg
      variables:
        subnet_ids: "${module.vpc.subnet_ids}"
        sg_id: "${module.sg.sg_id}"
    eks:
      source: "terraform-aws-modules/eks/aws"
      depends_on:
        - vpc
        - sg
      variables:
        vpc_id: "${module.vpc.vpc_id}"
    app:
      source: "./app"
      depends_on:
        - eks
        - db
      variables:
        region: "${var.region}"
"#;

    let config = parse_config(yaml).unwrap();
    let result = validate(&config);
    assert!(
        result.is_ok(),
        "Valid config should pass validation: {:?}",
        result.err()
    );
}

#[test]
fn test_parse_config_with_hooks() {
    let yaml = r#"
project:
  name: "with-hooks"
  version: "1.0"
  modules:
    vpc:
      source: "./vpc"
  hooks:
    pre_plan:
      - "echo start"
    post_apply:
      - "echo done"
    on_failure:
      - "echo failed"
"#;

    let config = parse_config(yaml).unwrap();
    let hooks = config.project.hooks.unwrap();
    assert_eq!(hooks.pre_plan.len(), 1);
    assert_eq!(hooks.post_apply.len(), 1);
    assert_eq!(hooks.on_failure.len(), 1);
}

#[test]
fn test_parse_module_with_array_variables() {
    let yaml = r#"
project:
  name: "arrays"
  version: "1.0"
  modules:
    vpc:
      source: "./vpc"
      variables:
        azs: ["us-east-1a", "us-east-1b"]
        subnets: ["10.0.1.0/24", "10.0.2.0/24"]
      outputs:
        - vpc_id
"#;

    let config = parse_config(yaml).unwrap();
    let vpc = &config.project.modules["vpc"];
    let azs = vpc.variables.get("azs").unwrap();
    assert!(azs.is_sequence());
}

#[test]
fn test_oci_backend_block_parsed() {
    use oxid::config::types::BackendConfig;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("_backend.tf"),
        r#"
terraform {
  backend "oci" {
    bucket       = "terraform-iac"
    key          = "Dev/DNS/terraform.tfstate"
    namespace    = "axsewdef1qua"
    region       = "uk-london-1"
    tenancy_ocid = "ocid1.tenancy.oc1..example"
  }
}
"#,
    )
    .unwrap();

    let ws = oxid::hcl::parse_directory(dir.path()).unwrap();
    let backend = ws.terraform_settings.unwrap().backend.unwrap();
    match &backend {
        BackendConfig::Oci {
            bucket,
            namespace,
            key,
            region,
            tenancy_ocid,
            config_file_profile,
            ..
        } => {
            assert_eq!(bucket, "terraform-iac");
            assert_eq!(namespace, "axsewdef1qua");
            assert_eq!(key, "Dev/DNS/terraform.tfstate");
            assert_eq!(region.as_deref(), Some("uk-london-1"));
            assert_eq!(tenancy_ocid.as_deref(), Some("ocid1.tenancy.oc1..example"));
            assert_eq!(config_file_profile, &None);
        }
        other => panic!("expected OCI backend, got {:?}", other),
    }
    assert_eq!(
        backend.location(),
        "oci://axsewdef1qua/terraform-iac/Dev/DNS/terraform.tfstate  (region: uk-london-1)"
    );
}

#[test]
fn test_oci_backend_key_defaults_to_terraform_tfstate() {
    use oxid::config::types::BackendConfig;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("main.tf"),
        "terraform {\n  backend \"oci\" {\n    bucket = \"b\"\n    namespace = \"n\"\n  }\n}\n",
    )
    .unwrap();

    let ws = oxid::hcl::parse_directory(dir.path()).unwrap();
    match ws.terraform_settings.unwrap().backend.unwrap() {
        BackendConfig::Oci { key, .. } => assert_eq!(key, "terraform.tfstate"),
        other => panic!("expected OCI backend, got {:?}", other),
    }
}
