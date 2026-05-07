<p align="center">
  <h1 align="center">Oxid</h1>
  <p align="center">
    <strong>Open-source infrastructure-as-code engine. Drop-in alternative to Terraform and OpenTofu.</strong>
  </p>
  <p align="center">
    <a href="https://oxid.sh/docs">Documentation</a> |
    <a href="https://oxid.sh/docs/quickstart">Quick Start</a> |
    <a href="https://oxid.sh/docs/blast-radius">Blast Radius</a> |
    <a href="https://oxid.sh/docs/history">Resource History</a> |
    <a href="https://oxid.sh/docs/logging">Observability</a>
  </p>
  <p align="center">
    <a href="https://github.com/ops0-ai/oxid/releases"><img src="https://img.shields.io/github/v/release/ops0-ai/oxid?style=flat-square&color=blue" alt="Release"></a>
    <a href="https://github.com/ops0-ai/oxid/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-green?style=flat-square" alt="License"></a>
    <a href="https://github.com/ops0-ai/oxid/stargazers"><img src="https://img.shields.io/github/stars/ops0-ai/oxid?style=flat-square" alt="Stars"></a>
  </p>
</p>

---

Oxid reads your existing `.tf` files, talks directly to AWS/GCP/Azure providers via gRPC, and stores state in SQLite or PostgreSQL. No Terraform or OpenTofu binary required.

```bash
curl -fsSL https://raw.githubusercontent.com/ops0-ai/oxid/main/install.sh | bash
```

## Why Oxid?

| | Terraform / OpenTofu | Oxid |
|---|---|---|
| Execution Model | Wave-based (batch) | Event-driven per-resource |
| Parallelism | Resources in same wave wait for slowest | Dependents start instantly when deps complete |
| State Backend | JSON file or remote backend | SQLite (local) / PostgreSQL (teams) |
| Config Language | HCL only | HCL + YAML |
| Provider Protocol | Wraps binary / shared lib | Direct gRPC (tfplugin5/6) |
| Queryable State | `terraform show` | Full SQL queries |
| Resource History | No built-in audit trail | Full change history with attribute diffs |
| Blast Radius | No native support | Forward + reverse dependency analysis with depth levels |
| Drift Detection | Manual plan comparison | Built-in `oxid drift` command |
| Sensitive Output Encryption | Stored as plain text in state | AES-256 encrypted at rest (pgcrypto) |
| Structured Logging | `TF_LOG` env var, unstructured text | JSON log file with resource IDs, actions, timing (Datadog/Splunk/OTel ready) |
| Apply Efficiency | Re-evaluates all resources each apply | Smart apply - only touches changed resources, skips unchanged |
| License | BSL / MPL | Apache-2.0 |

## Get Started

```bash
cd your-terraform-project/
oxid init       # download providers, import existing state
oxid plan       # preview changes
oxid apply      # apply infrastructure
```

Your existing `.tf` files, `.tfvars`, and `TF_VAR_*` environment variables work unchanged.

**[Full documentation at oxid.sh/docs](https://oxid.sh/docs)**

## Key Features

### Blast Radius Analysis

See exactly what breaks before you touch anything:

```bash
$ oxid blast-radius aws_vpc.main

  Severity: HIGH  (32 resources, 3 levels deep)

  Types: 4 aws_subnet, 3 aws_route_table, 1 aws_eks_cluster, 1 aws_eks_node_group...

  Depth 1 - direct dependent (10 resources):
    ~ aws_internet_gateway.main
    ~ aws_subnet.private[0]
    ~ aws_security_group.eks_cluster
    ...

  Depth 2 - transitive dependent (12 resources):
    ~ aws_eks_cluster.main
    ~ aws_nat_gateway.main[0]
    ...
```

Reverse mode shows upstream dependencies:

```bash
$ oxid blast-radius aws_eks_node_group.main --why

  Dependencies of aws_eks_node_group.main
  14 upstream dependencies, 2 levels deep
```

Works with resource types too: `oxid blast-radius aws_subnet`



### Resource Change History

Full audit trail for every resource. Track tag changes, instance type changes, scaling events:

```bash
$ oxid history aws_vpc.main

  2026-05-07T10:30:02+00:00 ~  update  (2h ago)
    id: vpc-0626c706762a661e9
    cidr_block: 10.0.0.0/16
    tags: {"env":"prod","iac":"oxid"}
    changes:
      ~ tag "iac" = "terraform" -> "oxid"

  2026-05-07T08:15:00+00:00 +  create  (4h ago)
    id: vpc-0626c706762a661e9
    cidr_block: 10.0.0.0/16
    tags: {"env":"prod","iac":"terraform"}
```

Query history with SQL:

```sql
oxid query "SELECT address, action, captured_at FROM resource_history ORDER BY captured_at DESC"
```



### Structured JSON Logging

Write structured logs for Datadog, Splunk, ELK, or OpenTelemetry:

```bash
oxid apply --log-file ./oxid.log
```

Every event includes resource address, resource ID, action, provider, and timing:

```json
{"timestamp":"...","level":"INFO","fields":{"event":"resource.apply.complete","address":"aws_vpc.main","resource_id":"vpc-0626c706762a661e9","action":"update","provider":"hashicorp/aws","elapsed_secs":1.2}}
```

### Terraform/OpenTofu Log Bridge

Already using Terraform or OpenTofu? Pipe their JSON output through `oxid watch` to get structured logs without changing your workflow:

```bash
terraform apply -auto-approve -json | oxid watch --log-file ./tf.log
tofu apply -auto-approve -json | oxid watch --log-file ./tf.log
```



### SQL-Queryable State

```sql
-- Find all t3.micro instances
oxid query "SELECT address FROM resources WHERE attributes_json->>'instance_type' = 't3.micro'"

-- Resources missing tags
oxid query "SELECT address FROM resources WHERE attributes_json->'tags' = '{}'"

-- Count by resource type
oxid query "SELECT resource_type, COUNT(*) FROM resources GROUP BY resource_type"
```

### Smart Apply

Oxid only sends changed resources to providers. Unchanged resources are skipped entirely - no provider API calls, no noise in the output:

```
Plan: 0 to add, 1 to change, 0 to destroy.

aws_vpc.main: Updating...
aws_vpc.main: Update complete after 0s [id=vpc-0626c706762a661e9]

Apply complete! Resources: 0 added, 1 changed, 0 destroyed. Total time: 0s.
```

## All Commands

```bash
oxid init                              # initialize providers and state
oxid plan                              # preview changes
oxid apply                             # apply changes
oxid destroy                           # tear down infrastructure
oxid state list                        # list managed resources
oxid state show <address>              # show resource details
oxid query "SQL"                       # query state with SQL
oxid blast-radius <resource>           # downstream impact analysis
oxid blast-radius <resource> --why     # upstream dependency chain
oxid blast-radius --plan               # blast radius for planned changes
oxid history <resource>                # resource change audit trail
oxid drift                             # detect configuration drift
oxid sync                              # sync from Terraform remote backend
oxid output --json                     # show outputs (Terraform-compatible)
oxid watch                             # watch Terraform/OpenTofu JSON output
oxid graph                             # dependency graph (DOT format)
oxid validate                          # validate configuration
```

## PostgreSQL for Teams

```bash
export OXID_DATABASE_URL="postgres://user:pass@host:5432/dbname"
oxid init   # tables created automatically
```

Sensitive outputs are AES-256 encrypted at rest via pgcrypto.

## Contributing

We welcome contributions! Here's how:

- **Try it** - Run `oxid plan` against your Terraform projects and report what works
- **Report bugs** - [Open an issue](https://github.com/ops0-ai/oxid/issues)
- **Test providers** - Try AWS, GCP, Azure, Kubernetes, Cloudflare
- **Submit PRs** - Code contributions welcome

### Build from Source

```bash
# Prerequisites: Rust 1.93+, protoc
git clone https://github.com/ops0-ai/oxid.git
cd oxid
cargo build --release
```

## License

Apache-2.0. See [LICENSE](LICENSE).

---

<p align="center">
  <a href="https://oxid.sh">oxid.sh</a> | Built by <a href="https://ops0.com">ops0.com</a>
</p>
