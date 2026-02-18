//! End-to-end tests for oxid CLI using real fixtures.
//!
//! All tests are `#[ignore]` — plain `cargo test` skips them entirely.
//!
//! ```bash
//! cargo test -- --ignored                       # Tier 1: no-creds e2e
//! OXID_E2E_AWS=1 cargo test -- --ignored        # Tier 1 + Tier 2: all e2e
//! cargo test -- --ignored e2e_03_mixed          # Single fixture
//! ```

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Returns the absolute path to a fixture directory under `tests/e2e/`.
fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("e2e")
        .join(name)
}

/// Builds an `oxid` Command pointing at the given fixture and isolated work dir.
fn oxid_cmd(subcommand: &str, fixture: &Path, work_dir: &Path) -> Command {
    let mut cmd = assert_cmd::cargo_bin_cmd!("oxid");
    cmd.arg("-c")
        .arg(fixture)
        .arg("-w")
        .arg(work_dir)
        .arg(subcommand)
        .env("NO_COLOR", "1");
    cmd
}

/// Builds an `oxid` Command with extra args (e.g., `--auto-approve`).
fn oxid_cmd_with_args(
    subcommand: &str,
    fixture: &Path,
    work_dir: &Path,
    extra_args: &[&str],
) -> Command {
    let mut cmd = assert_cmd::cargo_bin_cmd!("oxid");
    cmd.arg("-c")
        .arg(fixture)
        .arg("-w")
        .arg(work_dir)
        .arg(subcommand)
        .env("NO_COLOR", "1");
    for arg in extra_args {
        cmd.arg(arg);
    }
    cmd
}

/// Returns true if `OXID_E2E_AWS` is set (any non-empty value).
fn aws_enabled() -> bool {
    std::env::var("OXID_E2E_AWS")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

// ─── Tier 1: No-credentials fixtures ─────────────────────────────────────────

// ── 01-pure-hcl ──────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn e2e_01_pure_hcl_validate() {
    let fixture = fixture_dir("01-pure-hcl");
    let work = TempDir::new().unwrap();
    oxid_cmd("validate", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration is valid"));
}

#[test]
#[ignore]
fn e2e_01_pure_hcl_init() {
    let fixture = fixture_dir("01-pure-hcl");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Project initialized successfully"));
}

#[test]
#[ignore]
fn e2e_01_pure_hcl_plan() {
    let fixture = fixture_dir("01-pure-hcl");
    let work = TempDir::new().unwrap();
    // init first to download providers
    oxid_cmd("init", &fixture, work.path()).assert().success();
    // then plan
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.name")
                .and(predicate::str::contains("random_integer.port"))
                .and(predicate::str::contains("random_password.secret"))
                .and(predicate::str::contains("null_resource.example"))
                .and(predicate::str::contains("local_file.config"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("5 to add")),
        );
}

// ── 02-pure-tf-json ──────────────────────────────────────────────────────────

#[test]
#[ignore]
fn e2e_02_pure_tf_json_validate() {
    let fixture = fixture_dir("02-pure-tf-json");
    let work = TempDir::new().unwrap();
    oxid_cmd("validate", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration is valid"));
}

#[test]
#[ignore]
fn e2e_02_pure_tf_json_init() {
    let fixture = fixture_dir("02-pure-tf-json");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Project initialized successfully"));
}

#[test]
#[ignore]
fn e2e_02_pure_tf_json_plan() {
    let fixture = fixture_dir("02-pure-tf-json");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.name")
                .and(predicate::str::contains("random_integer.port"))
                .and(predicate::str::contains("random_password.secret"))
                .and(predicate::str::contains("null_resource.example"))
                .and(predicate::str::contains("local_file.config"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("5 to add")),
        );
}

// ── 03-mixed ─────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn e2e_03_mixed_validate() {
    let fixture = fixture_dir("03-mixed");
    let work = TempDir::new().unwrap();
    oxid_cmd("validate", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration is valid"));
}

#[test]
#[ignore]
fn e2e_03_mixed_init() {
    let fixture = fixture_dir("03-mixed");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Project initialized successfully"));
}

#[test]
#[ignore]
fn e2e_03_mixed_plan() {
    let fixture = fixture_dir("03-mixed");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.name")
                .and(predicate::str::contains("random_integer.port"))
                .and(predicate::str::contains("null_resource.gate"))
                .and(predicate::str::contains("local_file.output"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("4 to add")),
        );
}

// ── 07-apply-destroy ────────────────────────────────────────────────────────
// Regression test for issue #2: map(string) coercion of integer values.
// Exercises full apply + destroy cycle with no cloud credentials.

#[test]
#[ignore]
fn e2e_07_apply_destroy_plan() {
    let fixture = fixture_dir("07-apply-destroy");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.name")
                .and(predicate::str::contains("random_integer.port"))
                .and(predicate::str::contains("null_resource.coerce_test"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("3 to add")),
        );
}

#[test]
#[ignore]
fn e2e_07_apply_destroy_apply() {
    let fixture = fixture_dir("07-apply-destroy");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd_with_args("apply", &fixture, work.path(), &["--auto-approve"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("null_resource.coerce_test: Creation complete")
                .and(predicate::str::contains(
                    "random_integer.port: Creation complete",
                ))
                .and(predicate::str::contains(
                    "random_pet.name: Creation complete",
                ))
                .and(predicate::str::contains("3 added")),
        );
}

#[test]
#[ignore]
fn e2e_07_apply_destroy_full_cycle() {
    let fixture = fixture_dir("07-apply-destroy");
    let work = TempDir::new().unwrap();

    // init
    oxid_cmd("init", &fixture, work.path()).assert().success();

    // apply
    oxid_cmd_with_args("apply", &fixture, work.path(), &["--auto-approve"])
        .assert()
        .success()
        .stdout(predicate::str::contains("3 added"));

    // plan after apply should show no changes
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("No changes"));

    // destroy
    oxid_cmd_with_args("destroy", &fixture, work.path(), &["--auto-approve"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Destroy complete"));
}

// ─── Tier 2: AWS fixtures (require OXID_E2E_AWS=1) ──────────────────────────

// ── 04-aws-hcl ───────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn e2e_04_aws_hcl_validate() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("04-aws-hcl");
    let work = TempDir::new().unwrap();
    oxid_cmd("validate", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration is valid"));
}

#[test]
#[ignore]
fn e2e_04_aws_hcl_init() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("04-aws-hcl");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Project initialized successfully"));
}

#[test]
#[ignore]
fn e2e_04_aws_hcl_plan() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("04-aws-hcl");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.suffix")
                .and(predicate::str::contains("aws_ssm_parameter.test"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("2 to add")),
        );
}

// ── 05-aws-tf-json ───────────────────────────────────────────────────────────

#[test]
#[ignore]
fn e2e_05_aws_tf_json_validate() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("05-aws-tf-json");
    let work = TempDir::new().unwrap();
    oxid_cmd("validate", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration is valid"));
}

#[test]
#[ignore]
fn e2e_05_aws_tf_json_init() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("05-aws-tf-json");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Project initialized successfully"));
}

#[test]
#[ignore]
fn e2e_05_aws_tf_json_plan() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("05-aws-tf-json");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.suffix")
                .and(predicate::str::contains("aws_ssm_parameter.test"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("2 to add")),
        );
}

// ── 06-aws-mixed ─────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn e2e_06_aws_mixed_validate() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("06-aws-mixed");
    let work = TempDir::new().unwrap();
    oxid_cmd("validate", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Configuration is valid"));
}

#[test]
#[ignore]
fn e2e_06_aws_mixed_init() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("06-aws-mixed");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Project initialized successfully"));
}

#[test]
#[ignore]
fn e2e_06_aws_mixed_plan() {
    if !aws_enabled() {
        eprintln!("SKIP: OXID_E2E_AWS not set — skipping AWS fixture");
        return;
    }
    let fixture = fixture_dir("06-aws-mixed");
    let work = TempDir::new().unwrap();
    oxid_cmd("init", &fixture, work.path()).assert().success();
    oxid_cmd("plan", &fixture, work.path())
        .assert()
        .success()
        .stdout(
            predicate::str::contains("random_pet.suffix")
                .and(predicate::str::contains("aws_ssm_parameter.test"))
                .and(predicate::str::contains("will be created"))
                .and(predicate::str::contains("2 to add")),
        );
}
