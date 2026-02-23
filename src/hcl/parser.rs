use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::types::*;

/// Parse a single HCL file into a partial WorkspaceConfig.
pub fn parse_hcl(content: &str, file_path: &Path) -> Result<WorkspaceConfig> {
    let body: hcl::Body = hcl::from_str(content)
        .with_context(|| format!("Failed to parse HCL in: {}", file_path.display()))?;
    parse_hcl_body(body, file_path)
}

/// Parse an hcl::Body into a partial WorkspaceConfig.
///
/// This is the core block-iteration logic shared by both HCL and JSON parsers.
/// The JSON parser builds an `hcl::Body` programmatically and feeds it here.
pub(crate) fn parse_hcl_body(body: hcl::Body, file_path: &Path) -> Result<WorkspaceConfig> {
    let mut workspace = WorkspaceConfig::default();
    let file_str = file_path.to_string_lossy().to_string();

    for structure in body.into_inner() {
        match structure {
            hcl::Structure::Block(block) => {
                let ident = block.identifier().to_string();
                match ident.as_str() {
                    "terraform" => {
                        workspace.terraform_settings = Some(parse_terraform_block(&block)?);
                    }
                    "provider" => {
                        if let Some(provider) = parse_provider_block(&block)? {
                            workspace.providers.push(provider);
                        }
                    }
                    "resource" => {
                        if let Some(resource) = parse_resource_block(&block, &file_str)? {
                            workspace.resources.push(resource);
                        }
                    }
                    "data" => {
                        if let Some(data) = parse_data_block(&block, &file_str)? {
                            workspace.data_sources.push(data);
                        }
                    }
                    "variable" => {
                        if let Some(var) = parse_variable_block(&block)? {
                            workspace.variables.push(var);
                        }
                    }
                    "output" => {
                        if let Some(out) = parse_output_block(&block)? {
                            workspace.outputs.push(out);
                        }
                    }
                    "module" => {
                        if let Some(module) = parse_module_block(&block)? {
                            workspace.modules.push(module);
                        }
                    }
                    "locals" => {
                        let locals = parse_locals_block(&block)?;
                        workspace.locals.extend(locals);
                    }
                    _ => {
                        tracing::debug!("Ignoring unknown block type: {}", ident);
                    }
                }
            }
            hcl::Structure::Attribute(attr) => {
                tracing::debug!("Ignoring top-level attribute: {}", attr.key);
            }
        }
    }

    Ok(workspace)
}

// ─── Block Parsers ───────────────────────────────────────────────────────────

fn parse_terraform_block(block: &hcl::Block) -> Result<TerraformSettings> {
    let mut settings = TerraformSettings::default();

    for structure in block.body().iter() {
        match structure {
            hcl::Structure::Block(inner_block) => {
                let ident = inner_block.identifier().to_string();
                match ident.as_str() {
                    "required_providers" => {
                        for attr_structure in inner_block.body().iter() {
                            if let hcl::Structure::Attribute(attr) = attr_structure {
                                let name = attr.key.to_string();
                                let req = parse_required_provider(&attr.expr)?;
                                settings.required_providers.insert(name, req);
                            }
                        }
                    }
                    "backend" => {
                        settings.backend = Some(parse_backend_block(inner_block));
                    }
                    _ => {}
                }
            }
            hcl::Structure::Attribute(attr) => {
                let key: &str = &attr.key;
                if key == "required_version" {
                    settings.required_version = Some(expr_to_string(&attr.expr));
                }
            }
        }
    }

    Ok(settings)
}

fn parse_backend_block(block: &hcl::Block) -> BackendConfig {
    let backend_type = block
        .labels()
        .first()
        .map(|l| l.as_str().to_string())
        .unwrap_or_default();

    let mut attrs: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for structure in block.body().iter() {
        if let hcl::Structure::Attribute(attr) = structure {
            attrs.insert(attr.key.to_string(), expr_to_string(&attr.expr));
        }
    }

    match backend_type.as_str() {
        "s3" => BackendConfig::S3 {
            bucket: attrs.remove("bucket").unwrap_or_default(),
            key: attrs.remove("key").unwrap_or_default(),
            region: attrs.remove("region"),
            dynamodb_table: attrs.remove("dynamodb_table"),
            role_arn: attrs.remove("role_arn"),
            endpoint: attrs.remove("endpoint"),
            profile: attrs.remove("profile"),
        },
        _ => BackendConfig::Unsupported { backend_type },
    }
}

fn parse_required_provider(expr: &hcl::Expression) -> Result<RequiredProvider> {
    let mut source = String::new();
    let mut version = None;

    if let hcl::Expression::Object(obj) = expr {
        for (key_expr, value_expr) in obj {
            let key = object_key_to_string(key_expr);
            match key.as_str() {
                "source" => source = expr_to_string(value_expr),
                "version" => version = Some(expr_to_string(value_expr)),
                _ => {}
            }
        }
    }

    Ok(RequiredProvider { source, version })
}

fn parse_provider_block(block: &hcl::Block) -> Result<Option<ProviderConfig>> {
    let labels: Vec<String> = block
        .labels()
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    if labels.is_empty() {
        return Ok(None);
    }

    let name = labels[0].clone();
    let mut alias = None;
    let mut config = HashMap::new();

    for structure in block.body().iter() {
        if let hcl::Structure::Attribute(attr) = structure {
            let key: &str = &attr.key;
            if key == "alias" {
                alias = Some(expr_to_string(&attr.expr));
            } else {
                config.insert(key.to_string(), hcl_expr_to_expression(&attr.expr));
            }
        }
    }

    Ok(Some(ProviderConfig {
        name: name.clone(),
        source: format!("hashicorp/{}", name),
        version_constraint: None,
        alias,
        config,
    }))
}

fn parse_resource_block(block: &hcl::Block, file: &str) -> Result<Option<ResourceConfig>> {
    let labels: Vec<String> = block
        .labels()
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    if labels.len() < 2 {
        return Ok(None);
    }

    parse_resource_body(block, labels[0].clone(), labels[1].clone(), file)
}

fn parse_data_block(block: &hcl::Block, file: &str) -> Result<Option<ResourceConfig>> {
    let labels: Vec<String> = block
        .labels()
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    if labels.len() < 2 {
        return Ok(None);
    }

    parse_resource_body(block, labels[0].clone(), labels[1].clone(), file)
}

fn parse_resource_body(
    block: &hcl::Block,
    resource_type: String,
    name: String,
    file: &str,
) -> Result<Option<ResourceConfig>> {
    let mut provider_ref = None;
    let mut count = None;
    let mut for_each = None;
    let mut depends_on = Vec::new();
    let mut lifecycle = LifecycleConfig::default();
    let mut attributes = HashMap::new();
    let mut provisioners = Vec::new();

    for structure in block.body().iter() {
        match structure {
            hcl::Structure::Attribute(attr) => {
                let key: &str = &attr.key;
                match key {
                    "provider" => provider_ref = Some(expr_to_string(&attr.expr)),
                    "count" => count = Some(hcl_expr_to_expression(&attr.expr)),
                    "for_each" => for_each = Some(hcl_expr_to_expression(&attr.expr)),
                    "depends_on" => depends_on = expr_to_string_list(&attr.expr),
                    _ => {
                        attributes.insert(key.to_string(), hcl_expr_to_expression(&attr.expr));
                    }
                }
            }
            hcl::Structure::Block(inner_block) => {
                let ident = inner_block.identifier();
                match ident {
                    "lifecycle" => {
                        lifecycle = parse_lifecycle_block(inner_block);
                    }
                    "provisioner" => {
                        let prov_labels: Vec<String> = inner_block
                            .labels()
                            .iter()
                            .map(|l| l.as_str().to_string())
                            .collect();
                        let prov_type = prov_labels.first().cloned().unwrap_or_default();
                        let mut prov_config = HashMap::new();
                        let mut when = ProvisionerWhen::Create;

                        for s in inner_block.body().iter() {
                            if let hcl::Structure::Attribute(a) = s {
                                let k: &str = &a.key;
                                if k == "when" {
                                    if expr_to_string(&a.expr) == "destroy" {
                                        when = ProvisionerWhen::Destroy;
                                    }
                                } else {
                                    prov_config
                                        .insert(k.to_string(), hcl_expr_to_expression(&a.expr));
                                }
                            }
                        }

                        provisioners.push(ProvisionerConfig {
                            provisioner_type: prov_type,
                            config: prov_config,
                            when,
                        });
                    }
                    _ => {
                        let nested = parse_nested_block_as_attribute(inner_block);
                        // If this block name already exists, collect into an array
                        // (e.g. repeated `filter` blocks in data sources)
                        if let Some(existing) = attributes.remove(ident) {
                            let arr = match existing {
                                Expression::Literal(Value::List(mut items)) => {
                                    if let Expression::Literal(val) = nested {
                                        items.push(val);
                                    }
                                    items
                                }
                                _ => {
                                    let mut items = Vec::new();
                                    if let Expression::Literal(val) = existing {
                                        items.push(val);
                                    }
                                    if let Expression::Literal(val) = nested {
                                        items.push(val);
                                    }
                                    items
                                }
                            };
                            attributes
                                .insert(ident.to_string(), Expression::Literal(Value::List(arr)));
                        } else {
                            attributes.insert(ident.to_string(), nested);
                        }
                    }
                }
            }
        }
    }

    Ok(Some(ResourceConfig {
        resource_type,
        name,
        provider_ref,
        count,
        for_each,
        depends_on,
        lifecycle,
        attributes,
        provisioners,
        source_location: Some(SourceLocation {
            file: file.to_string(),
            line: 0,
            column: 0,
            config_type: ConfigType::Hcl,
        }),
    }))
}

fn parse_lifecycle_block(block: &hcl::Block) -> LifecycleConfig {
    let mut lc = LifecycleConfig::default();

    for structure in block.body().iter() {
        if let hcl::Structure::Attribute(attr) = structure {
            let key: &str = &attr.key;
            match key {
                "create_before_destroy" => lc.create_before_destroy = expr_to_bool(&attr.expr),
                "prevent_destroy" => lc.prevent_destroy = expr_to_bool(&attr.expr),
                "ignore_changes" => lc.ignore_changes = expr_to_string_list(&attr.expr),
                "replace_triggered_by" => lc.replace_triggered_by = expr_to_string_list(&attr.expr),
                _ => {}
            }
        }
    }

    lc
}

fn parse_variable_block(block: &hcl::Block) -> Result<Option<VariableConfig>> {
    let labels: Vec<String> = block
        .labels()
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    if labels.is_empty() {
        return Ok(None);
    }

    let name = labels[0].clone();
    let mut var_type = None;
    let mut default = None;
    let mut description = None;
    let mut sensitive = false;
    let mut validation = Vec::new();

    for structure in block.body().iter() {
        match structure {
            hcl::Structure::Attribute(attr) => {
                let key: &str = &attr.key;
                match key {
                    "type" => var_type = Some(expr_to_string(&attr.expr)),
                    "default" => default = Some(hcl_expr_to_expression(&attr.expr)),
                    "description" => description = Some(expr_to_string(&attr.expr)),
                    "sensitive" => sensitive = expr_to_bool(&attr.expr),
                    _ => {}
                }
            }
            hcl::Structure::Block(inner_block) => {
                if inner_block.identifier() == "validation" {
                    let mut condition = Expression::Literal(Value::Bool(true));
                    let mut error_message = String::new();

                    for s in inner_block.body().iter() {
                        if let hcl::Structure::Attribute(a) = s {
                            let k: &str = &a.key;
                            match k {
                                "condition" => condition = hcl_expr_to_expression(&a.expr),
                                "error_message" => error_message = expr_to_string(&a.expr),
                                _ => {}
                            }
                        }
                    }

                    validation.push(ValidationRule {
                        condition,
                        error_message,
                    });
                }
            }
        }
    }

    Ok(Some(VariableConfig {
        name,
        var_type,
        default,
        description,
        sensitive,
        validation,
    }))
}

fn parse_output_block(block: &hcl::Block) -> Result<Option<OutputConfig>> {
    let labels: Vec<String> = block
        .labels()
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    if labels.is_empty() {
        return Ok(None);
    }

    let name = labels[0].clone();
    let mut value = Expression::Literal(Value::Null);
    let mut description = None;
    let mut sensitive = false;
    let mut depends_on = Vec::new();

    for structure in block.body().iter() {
        if let hcl::Structure::Attribute(attr) = structure {
            let key: &str = &attr.key;
            match key {
                "value" => value = hcl_expr_to_expression(&attr.expr),
                "description" => description = Some(expr_to_string(&attr.expr)),
                "sensitive" => sensitive = expr_to_bool(&attr.expr),
                "depends_on" => depends_on = expr_to_string_list(&attr.expr),
                _ => {}
            }
        }
    }

    Ok(Some(OutputConfig {
        name,
        value,
        description,
        sensitive,
        depends_on,
    }))
}

fn parse_module_block(block: &hcl::Block) -> Result<Option<ModuleRef>> {
    let labels: Vec<String> = block
        .labels()
        .iter()
        .map(|l| l.as_str().to_string())
        .collect();
    if labels.is_empty() {
        return Ok(None);
    }

    let name = labels[0].clone();
    let mut source = String::new();
    let mut version = None;
    let mut depends_on = Vec::new();
    let mut variables = HashMap::new();
    let mut providers = HashMap::new();

    for structure in block.body().iter() {
        if let hcl::Structure::Attribute(attr) = structure {
            let key: &str = &attr.key;
            match key {
                "source" => source = expr_to_string(&attr.expr),
                "version" => version = Some(expr_to_string(&attr.expr)),
                "depends_on" => depends_on = expr_to_string_list(&attr.expr),
                "providers" => {
                    if let hcl::Expression::Object(obj) = &attr.expr {
                        for (k, v) in obj {
                            providers.insert(object_key_to_string(k), expr_to_string(v));
                        }
                    }
                }
                _ => {
                    variables.insert(key.to_string(), hcl_expr_to_expression(&attr.expr));
                }
            }
        }
    }

    Ok(Some(ModuleRef {
        name,
        source,
        version,
        depends_on,
        variables,
        providers,
        outputs: Vec::new(),
    }))
}

fn parse_locals_block(block: &hcl::Block) -> Result<HashMap<String, Expression>> {
    let mut locals = HashMap::new();

    for structure in block.body().iter() {
        if let hcl::Structure::Attribute(attr) = structure {
            locals.insert(attr.key.to_string(), hcl_expr_to_expression(&attr.expr));
        }
    }

    Ok(locals)
}

// ─── Expression Conversion ──────────────────────────────────────────────────

/// Convert an hcl::Expression into our unified Expression type.
pub fn hcl_expr_to_expression(expr: &hcl::Expression) -> Expression {
    match expr {
        hcl::Expression::Null => Expression::Literal(Value::Null),
        hcl::Expression::Bool(b) => Expression::Literal(Value::Bool(*b)),
        hcl::Expression::Number(n) => {
            if let Some(i) = n.as_i64() {
                Expression::Literal(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Expression::Literal(Value::Float(f))
            } else {
                Expression::Literal(Value::Null)
            }
        }
        hcl::Expression::String(s) => {
            if s.contains("${") {
                parse_template_string(s)
            } else {
                Expression::Literal(Value::String(s.clone()))
            }
        }
        hcl::Expression::Array(arr) => {
            let items: Vec<Value> = arr.iter().filter_map(expr_to_value).collect();
            Expression::Literal(Value::List(items))
        }
        hcl::Expression::Object(obj) => {
            let entries: Vec<(String, Value)> = obj
                .iter()
                .filter_map(|(k, v)| {
                    let key = object_key_to_string(k);
                    expr_to_value(v).map(|val| (key, val))
                })
                .collect();
            Expression::Literal(Value::Map(entries))
        }
        hcl::Expression::TemplateExpr(template) => {
            let s = template.to_string();
            parse_template_string(&s)
        }
        hcl::Expression::Variable(var) => {
            let parts: Vec<String> = var.as_str().split('.').map(|s| s.to_string()).collect();
            Expression::Reference(parts)
        }
        hcl::Expression::Traversal(traversal) => {
            let mut parts = Vec::new();
            // Access the public field `expr` (not a method)
            if let hcl::Expression::Variable(var) = &traversal.expr {
                parts.push(var.to_string());
            } else {
                parts.push(format!("{:?}", traversal.expr));
            }
            // Access the public field `operators` (not a method)
            for operator in &traversal.operators {
                match operator {
                    hcl::expr::TraversalOperator::GetAttr(ident) => {
                        parts.push(ident.to_string());
                    }
                    hcl::expr::TraversalOperator::Index(idx) => {
                        parts.push(format!("[{}]", expr_to_string(idx)));
                    }
                    hcl::expr::TraversalOperator::LegacyIndex(n) => {
                        parts.push(format!("[{}]", n));
                    }
                    hcl::expr::TraversalOperator::AttrSplat
                    | hcl::expr::TraversalOperator::FullSplat => {
                        parts.push("[*]".to_string());
                    }
                }
            }
            Expression::Reference(parts)
        }
        hcl::Expression::FuncCall(func_call) => {
            let name = func_call.name.to_string();
            let args: Vec<Expression> = func_call.args.iter().map(hcl_expr_to_expression).collect();
            Expression::FunctionCall { name, args }
        }
        hcl::Expression::Conditional(cond) => Expression::Conditional {
            condition: Box::new(hcl_expr_to_expression(&cond.cond_expr)),
            true_val: Box::new(hcl_expr_to_expression(&cond.true_expr)),
            false_val: Box::new(hcl_expr_to_expression(&cond.false_expr)),
        },
        hcl::Expression::Operation(op) => match op.as_ref() {
            hcl::expr::Operation::Unary(unary) => {
                let oxid_op = match unary.operator {
                    hcl::expr::UnaryOperator::Neg => UnaryOp::Neg,
                    hcl::expr::UnaryOperator::Not => UnaryOp::Not,
                };
                Expression::UnaryOp {
                    op: oxid_op,
                    operand: Box::new(hcl_expr_to_expression(&unary.expr)),
                }
            }
            hcl::expr::Operation::Binary(binary) => {
                let oxid_op = match binary.operator {
                    hcl::expr::BinaryOperator::Eq => BinOp::Eq,
                    hcl::expr::BinaryOperator::NotEq => BinOp::NotEq,
                    hcl::expr::BinaryOperator::Less => BinOp::Lt,
                    hcl::expr::BinaryOperator::LessEq => BinOp::Lte,
                    hcl::expr::BinaryOperator::Greater => BinOp::Gt,
                    hcl::expr::BinaryOperator::GreaterEq => BinOp::Gte,
                    hcl::expr::BinaryOperator::Plus => BinOp::Add,
                    hcl::expr::BinaryOperator::Minus => BinOp::Sub,
                    hcl::expr::BinaryOperator::Mul => BinOp::Mul,
                    hcl::expr::BinaryOperator::Div => BinOp::Div,
                    hcl::expr::BinaryOperator::Mod => BinOp::Mod,
                    hcl::expr::BinaryOperator::And => BinOp::And,
                    hcl::expr::BinaryOperator::Or => BinOp::Or,
                };
                Expression::BinaryOp {
                    op: oxid_op,
                    left: Box::new(hcl_expr_to_expression(&binary.lhs_expr)),
                    right: Box::new(hcl_expr_to_expression(&binary.rhs_expr)),
                }
            }
        },
        hcl::Expression::ForExpr(for_expr) => Expression::ForExpr {
            collection: Box::new(hcl_expr_to_expression(&for_expr.collection_expr)),
            key_var: for_expr.key_var.as_ref().map(|v| v.to_string()),
            val_var: for_expr.value_var.to_string(),
            key_expr: for_expr
                .key_expr
                .as_ref()
                .map(|e| Box::new(hcl_expr_to_expression(e))),
            value_expr: Box::new(hcl_expr_to_expression(&for_expr.value_expr)),
            condition: for_expr
                .cond_expr
                .as_ref()
                .map(|e| Box::new(hcl_expr_to_expression(e))),
            grouping: for_expr.grouping,
        },
        hcl::Expression::Parenthesis(inner) => hcl_expr_to_expression(inner),
        _ => Expression::Literal(Value::String(format!("{:?}", expr))),
    }
}

fn parse_nested_block_as_attribute(block: &hcl::Block) -> Expression {
    let mut entries = Vec::new();

    for structure in block.body().iter() {
        match structure {
            hcl::Structure::Attribute(attr) => {
                let key = attr.key.to_string();
                let value = hcl_expr_to_expression(&attr.expr);
                match value {
                    Expression::Literal(val) => {
                        entries.push((key, val));
                    }
                    Expression::Reference(ref parts) => {
                        // Preserve as ${...} interpolation for the evaluator
                        entries.push((key, Value::String(format!("${{{}}}", parts.join(".")))));
                    }
                    Expression::Template(_) => {
                        // Template expressions — evaluate to string via eval
                        entries.push((key, Value::String(format!("{:?}", value))));
                    }
                    _ => {
                        // For other expressions (FunctionCall etc.), wrap as ${...}
                        // so the evaluator has a chance to process them
                        entries.push((key, Value::String(format!("{:?}", value))));
                    }
                }
            }
            hcl::Structure::Block(inner) => {
                let nested = parse_nested_block_as_attribute(inner);
                if let Expression::Literal(val) = nested {
                    entries.push((inner.identifier().to_string(), val));
                }
            }
        }
    }

    Expression::Literal(Value::Map(entries))
}

// ─── Helper Functions ────────────────────────────────────────────────────────

fn expr_to_string(expr: &hcl::Expression) -> String {
    match expr {
        hcl::Expression::String(s) => s.clone(),
        hcl::Expression::Variable(v) => v.to_string(),
        hcl::Expression::Number(n) => n.to_string(),
        hcl::Expression::Bool(b) => b.to_string(),
        hcl::Expression::Null => "null".to_string(),
        hcl::Expression::Traversal(t) => {
            let mut parts = Vec::new();
            if let hcl::Expression::Variable(var) = &t.expr {
                parts.push(var.to_string());
            }
            for op in &t.operators {
                match op {
                    hcl::expr::TraversalOperator::GetAttr(ident) => {
                        parts.push(ident.to_string());
                    }
                    hcl::expr::TraversalOperator::Index(idx) => {
                        parts.push(format!("[{}]", expr_to_string(idx)));
                    }
                    _ => {}
                }
            }
            parts.join(".")
        }
        _ => format!("{:?}", expr),
    }
}

fn object_key_to_string(key: &hcl::expr::ObjectKey) -> String {
    match key {
        hcl::expr::ObjectKey::Identifier(id) => id.to_string(),
        hcl::expr::ObjectKey::Expression(expr) => expr_to_string(expr),
        _ => String::new(),
    }
}

fn expr_to_bool(expr: &hcl::Expression) -> bool {
    matches!(expr, hcl::Expression::Bool(true))
}

fn expr_to_string_list(expr: &hcl::Expression) -> Vec<String> {
    match expr {
        hcl::Expression::Array(arr) => arr.iter().map(expr_to_string).collect(),
        _ => vec![],
    }
}

fn expr_to_value(expr: &hcl::Expression) -> Option<Value> {
    match expr {
        hcl::Expression::Null => Some(Value::Null),
        hcl::Expression::Bool(b) => Some(Value::Bool(*b)),
        hcl::Expression::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::Int(i))
            } else {
                n.as_f64().map(Value::Float)
            }
        }
        hcl::Expression::String(s) => Some(Value::String(s.clone())),
        hcl::Expression::Array(arr) => {
            let items: Vec<Value> = arr.iter().filter_map(expr_to_value).collect();
            Some(Value::List(items))
        }
        hcl::Expression::Object(obj) => {
            let entries: Vec<(String, Value)> = obj
                .iter()
                .filter_map(|(k, v)| {
                    let key = object_key_to_string(k);
                    expr_to_value(v).map(|val| (key, val))
                })
                .collect();
            Some(Value::Map(entries))
        }
        // Preserve variable references as ${...} interpolation strings so the
        // expression evaluator can resolve them later.
        hcl::Expression::Variable(var) => {
            let name: String = var.to_string();
            Some(Value::String(format!("${{{}}}", name)))
        }
        hcl::Expression::Traversal(traversal) => {
            let mut parts = Vec::new();
            if let hcl::Expression::Variable(var) = &traversal.expr {
                parts.push(var.to_string());
            }
            for op in &traversal.operators {
                match op {
                    hcl::TraversalOperator::GetAttr(name) => parts.push(name.to_string()),
                    hcl::TraversalOperator::Index(idx) => parts.push(format!("{:?}", idx)),
                    _ => {}
                }
            }
            Some(Value::String(format!("${{{}}}", parts.join("."))))
        }
        hcl::Expression::TemplateExpr(template) => {
            // Template expressions inside objects — preserve interpolation markers
            Some(Value::String(template.to_string()))
        }
        _ => Some(Value::String(format!("{:?}", expr))),
    }
}

fn parse_template_string(s: &str) -> Expression {
    let mut parts = Vec::new();
    let mut remaining = s;

    while let Some(start) = remaining.find("${") {
        if start > 0 {
            parts.push(TemplatePart::Literal(remaining[..start].to_string()));
        }

        if let Some(end) = remaining[start + 2..].find('}') {
            let ref_str = &remaining[start + 2..start + 2 + end];
            let ref_parts: Vec<String> = ref_str.split('.').map(|s| s.trim().to_string()).collect();
            parts.push(TemplatePart::Interpolation(Box::new(
                Expression::Reference(ref_parts),
            )));
            remaining = &remaining[start + 2 + end + 1..];
        } else {
            parts.push(TemplatePart::Literal(remaining.to_string()));
            remaining = "";
        }
    }

    if !remaining.is_empty() {
        parts.push(TemplatePart::Literal(remaining.to_string()));
    }

    if parts.len() == 1 {
        if let TemplatePart::Interpolation(expr) = &parts[0] {
            return *expr.clone();
        }
    }

    Expression::Template(parts)
}
