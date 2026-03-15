# Oxid

> **Beta** — Oxid is under active development. It works end-to-end with real AWS providers, but has not been battle-tested in production yet. Use caution for production deployments and please report any issues you find. Community testing and contributions are very welcome!

A standalone infrastructure-as-code engine. Open-source alternative to Terraform.

Oxid parses `.tf` (HCL) and `.tf.json` files natively and communicates directly with Terraform providers via gRPC — no `terraform` or `tofu` binary required.

## Why Oxid?

| | Terraform/OpenTofu | Oxid |
|---|---|---|
| **Execution** | Wave-based (batch) | Event-driven per-resource |
| **Parallelism** | Resources in same wave wait for slowest | Dependents start the instant their deps complete |
| **State** | JSON file or remote backend | SQLite (local) / PostgreSQL (teams) |
| **Config** | HCL + JSON | HCL + JSON + YAML |
| **Provider protocol** | Wraps binary / shared lib | Direct gRPC (tfplugin5/6) |
| **Queryable state** | `terraform show` | Full SQL: `oxid query "SELECT * FROM resources"` |
| **License** | BSL / MPL | Apache-2.0 |

## Features

- Native HCL (.tf) and JSON (.tf.json) parsing — reads your existing Terraform configs
- Mixed-format directories — `.tf` and `.tf.json` files in the same directory are merged automatically
- Direct gRPC communication with all Terraform providers (AWS, GCP, Azure, etc.)
- Event-driven DAG walker with per-resource parallelism
- Real-time progress with elapsed time tracking
- Resource-level plan display (Terraform-style `+`, `~`, `-`, `-/+`)
- SQLite state backend with full SQL query support
- .tfvars and TF_VAR_ environment variable support
- PostgreSQL remote state backend with JSONB, TIMESTAMPTZ, GIN indexes
- Drift detection with `oxid drift`
- Import from existing .tfstate files (auto-detected on `oxid init`)
- `--refresh=false` for fast plan using cached state

## Quick Start

### Install

```bash
# One-line install (Linux & macOS)
curl -fsSL https://raw.githubusercontent.com/ops0-ai/oxid/main/install.sh | bash
```

Or download a specific release manually:

```bash
# macOS (Apple Silicon)
curl -fsSL https://github.com/ops0-ai/oxid/releases/latest/download/oxid-darwin-arm64.tar.gz | tar xz
sudo mv oxid /usr/local/bin/

# macOS (Intel)
curl -fsSL https://github.com/ops0-ai/oxid/releases/latest/download/oxid-darwin-amd64.tar.gz | tar xz
sudo mv oxid /usr/local/bin/

# Linux (x86_64)
curl -fsSL https://github.com/ops0-ai/oxid/releases/latest/download/oxid-linux-amd64.tar.gz | tar xz
sudo mv oxid /usr/local/bin/

# Linux (ARM64)
curl -fsSL https://github.com/ops0-ai/oxid/releases/latest/download/oxid-linux-arm64.tar.gz | tar xz
sudo mv oxid /usr/local/bin/
```

From source:

```bash
git clone https://github.com/ops0-ai/oxid.git
cd oxid
cargo build --release
# Binary at ./target/release/oxid

# With PostgreSQL remote state support
cargo build --release --features postgres
```

### Usage

```bash
# Initialize providers
oxid init

# Preview changes
oxid plan

# Apply infrastructure
oxid apply

# Destroy infrastructure
oxid destroy

# List resources in state
oxid state list

# Show resource details
oxid state show aws_vpc.main

# Query state with SQL
oxid query "SELECT address, resource_type, status FROM resources"

# Fast plan (skip cloud API refresh, use cached state)
oxid plan --refresh=false

# Detect drift
oxid drift

# Visualize dependency graph
oxid graph | dot -Tpng -o graph.png
```

### Example

Given standard Terraform files:

```hcl
# main.tf
provider "aws" {
  region = var.aws_region
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags = {
    Name = "my-vpc"
    iac  = "oxid"
  }
}

resource "aws_subnet" "public" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.1.0/24"
}
```

```bash
$ oxid apply

aws_vpc.main: Refreshing state... [1/2]
aws_subnet.public: Refreshing state... [2/2]

Oxid used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
  + create

Oxid will perform the following actions:

  # aws_vpc.main will be created
  + resource "aws_vpc" "main" {
      + cidr_block           = "10.0.0.0/16"
      + enable_dns_hostnames = true
      + tags                 = { Name = "my-vpc", iac = "oxid" }
    }

  # aws_subnet.public will be created
  + resource "aws_subnet" "public" {
      + cidr_block = "10.0.1.0/24"
      + vpc_id     = (known after apply)
    }

Plan: 2 to add.

Do you want to perform these actions? Only 'yes' will be accepted.
  Enter a value: yes

aws_vpc.main: Creating...
aws_vpc.main: Creation complete after 3s [1/2] [id=vpc-0abc123]
aws_subnet.public: Creating...
aws_subnet.public: Creation complete after 1s [2/2] [id=subnet-0def456]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed. Total time: 4s.
```

## How It Works

1. **Parse** — Reads `.tf` (HCL) and `.tf.json` (JSON) files, extracts resources, data sources, variables, outputs, and providers. Mixed-format directories are merged.
2. **Build DAG** — Constructs a dependency graph from explicit `depends_on` and implicit expression references
3. **Start Providers** — Downloads provider binaries from registry.terraform.io, starts them as subprocesses, connects via gRPC
4. **Plan** — Calls `PlanResourceChange` on each provider to compute diffs
5. **Apply** — Event-driven DAG walker executes resources as dependencies are satisfied, calling `ApplyResourceChange` via gRPC
6. **Store State** — Persists resource attributes to SQLite (local) or PostgreSQL (remote)

## Architecture

```
               .tf / .tf.json files
                        |
              [HCL + JSON Parser]
                        |
                 [WorkspaceConfig]
                        |
                  [DAG Builder]
                        |
                [Resource Graph]
                   /    |    \
            [Provider] [Provider] [Provider]
              gRPC       gRPC       gRPC
               |          |          |
            [AWS]      [GCP]     [Azure]
                        |
            [SQLite / PostgreSQL State]
```

## PostgreSQL Remote State

By default, oxid stores state in a local SQLite database. For team collaboration, you can use PostgreSQL as a remote state backend.

### Setup

```bash
# Build with PostgreSQL support
cargo build --release --features postgres

# Set the database URL (supports Amazon RDS, Supabase, Neon, or any PostgreSQL)
export OXID_DATABASE_URL="postgres://user:password@hostname:5432/dbname"

# Initialize — tables are created automatically
oxid init
```

When `OXID_DATABASE_URL` is set, oxid automatically uses PostgreSQL. When unset, it falls back to SQLite.

### Schema

The PostgreSQL backend uses native Postgres types for performance and queryability:

| Column | Type | Purpose |
|---|---|---|
| `attributes_json` | `JSONB` | Resource attributes (GIN-indexed) |
| `sensitive_attrs` | `JSONB` | Sensitive attribute keys (GIN-indexed) |
| `created_at`, `updated_at` | `TIMESTAMPTZ` | Timestamps with timezone |
| `sensitive` | `BOOLEAN` | Output sensitivity flag |

### Querying

JSONB enables powerful queries against your infrastructure state:

```sql
-- Count resources by type
oxid query "SELECT resource_type, COUNT(*) FROM resources GROUP BY resource_type"

-- Find all VPCs by CIDR
oxid query "SELECT address, attributes_json->>'cidr_block' AS cidr FROM resources WHERE resource_type = 'aws_vpc'"

-- Find resources with a specific tag
oxid query "SELECT address FROM resources WHERE attributes_json->'tags'->>'Environment' = 'production'"

-- Find all t3.micro instances
oxid query "SELECT address FROM resources WHERE attributes_json @> '{\"instance_type\": \"t3.micro\"}'"
```

### Auto-Import from Terraform

When you run `oxid init` in a directory with existing Terraform state, oxid automatically detects and imports it:

```bash
# Auto-detects local terraform.tfstate
oxid init

# Also detects remote S3 backends configured in your .tf files
# and downloads + imports the state automatically
```

### TLS / RDS

Connections to Amazon RDS and other TLS-requiring hosts work out of the box (built with `rustls`).

## Building

```bash
# Prerequisites: Rust 1.75+, protoc (protobuf compiler)
cargo build --release
```

## Contributing

Oxid is in beta and help is appreciated! Here's how you can contribute:

- **Test with your `.tf` / `.tf.json` configs** — Try `oxid plan` against your existing Terraform projects and report what works/breaks
- **Report issues** — File bugs at [github.com/ops0-ai/oxid/issues](https://github.com/ops0-ai/oxid/issues)
- **Provider coverage** — Test with different providers (GCP, Azure, Cloudflare, etc.) beyond AWS
- **Code contributions** — PRs welcome. See the architecture section above for how things fit together

## License

Apache-2.0. See [LICENSE](LICENSE) for details.

---

Built by [ops0.com](https://ops0.com)
