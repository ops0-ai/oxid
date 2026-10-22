use oxid::config::types::Expression;
use oxid::executor::engine::{eval_expression, EvalContext};

fn reference(parts: &[&str]) -> Expression {
    Expression::Reference(parts.iter().map(|part| (*part).to_string()).collect())
}

#[test]
fn resolves_terraform_path_references() {
    let ctx = EvalContext::plan_only(std::collections::HashMap::new()).with_paths(
        "/workspace/module".to_string(),
        "/workspace/root".to_string(),
        "/workspace".to_string(),
        "staging".to_string(),
    );

    assert_eq!(
        eval_expression(&reference(&["path", "module"]), &ctx),
        serde_json::json!("/workspace/module")
    );
    assert_eq!(
        eval_expression(&reference(&["path", "root"]), &ctx),
        serde_json::json!("/workspace/root")
    );
    assert_eq!(
        eval_expression(&reference(&["path", "cwd"]), &ctx),
        serde_json::json!("/workspace")
    );
}

#[test]
fn resolves_terraform_workspace_reference() {
    let ctx = EvalContext::plan_only(std::collections::HashMap::new()).with_paths(
        "/workspace/module".to_string(),
        "/workspace/root".to_string(),
        "/workspace".to_string(),
        "staging".to_string(),
    );

    assert_eq!(
        eval_expression(&reference(&["terraform", "workspace"]), &ctx),
        serde_json::json!("staging")
    );
}

#[test]
fn missing_path_reference_remains_null() {
    let ctx = EvalContext::plan_only(std::collections::HashMap::new());

    assert_eq!(
        eval_expression(&reference(&["path", "unknown"]), &ctx),
        serde_json::Value::Null
    );
}

#[test]
fn cli_outputs_resolve_terraform_workspace_to_workspace_name() {
    let config_dir = tempfile::TempDir::new().unwrap();
    let work_dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        config_dir.path().join("main.tf"),
        "output \"workspace\" {\n  value = terraform.workspace\n}\n",
    )
    .unwrap();

    let oxid = |args: &[&str]| {
        let mut cmd = assert_cmd::cargo_bin_cmd!("oxid");
        cmd.arg("-c")
            .arg(config_dir.path())
            .arg("-w")
            .arg(work_dir.path())
            .args(args)
            .env("NO_COLOR", "1");
        cmd
    };

    oxid(&["init"]).assert().success();
    oxid(&["apply", "--auto-approve"]).assert().success();

    // The state store keys workspaces by a generated id; Terraform exposes the name.
    let output = oxid(&["output", "workspace"]).assert().success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    assert_eq!(stdout.trim(), "default");
}
