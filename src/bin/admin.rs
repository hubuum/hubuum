use clap::Parser;
use std::collections::BTreeMap;
use std::process::exit;
use tracing::warn;

use hubuum::config::{
    DEFAULT_DB_STATEMENT_TIMEOUT_MS, DEFAULT_EXPORT_TEMPLATE_FUEL,
    DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
};
use hubuum::db::prelude::*;
use hubuum::db::{DbPool, init_pool_with_statement_timeout, with_connection};
use hubuum::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use hubuum::logger;
use hubuum::models::{ExportTaskOutputRecord, ExportTemplate, User};
use hubuum::schema::export_task_outputs::dsl::export_task_outputs;
use hubuum::utilities::auth::generate_random_password;
use hubuum::utilities::exporting::validate_template_with_limits;
use hubuum::utilities::is_valid_log_level;

#[derive(Parser)]
#[command(
    author = "Terje Kvernes <terje@kvernes.no>",
    version = env!("CARGO_PKG_VERSION"),
    about = "Admin CLI for Hubuum",
    long_about = None
)]
struct AdminCli {
    /// Reset the password for the specified username
    #[arg(long)]
    reset_password: Option<String>,

    /// Validate all stored export templates against the Jinja renderer
    #[arg(long, default_value_t = false)]
    audit_templates: bool,

    /// Summarize stored export output health by template name
    #[arg(long, default_value_t = false)]
    export_template_health: bool,

    /// Database URL
    #[arg(long, env = "HUBUUM_DATABASE_URL")]
    database_url: Option<String>,

    /// Pool-global Postgres statement_timeout in milliseconds (0 disables it)
    #[arg(
        long,
        env = "HUBUUM_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    )]
    db_statement_timeout_ms: u64,

    /// MiniJinja recursion limit for export template validation
    #[arg(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT
    )]
    export_template_recursion_limit: usize,

    /// MiniJinja fuel budget for export template validation
    #[arg(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_FUEL",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_FUEL
    )]
    export_template_fuel: u64,

    /// Log level
    /// Possible values: trace, debug, info, warn, error
    #[arg(long, env = "HUBUUM_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), ApiError> {
    let admin_cli = AdminCli::parse();
    init_logging(&admin_cli.log_level);

    let database_url = admin_cli.database_url.unwrap_or_else(|| {
        std::env::var("HUBUUM_DATABASE_URL").unwrap_or_else(|_| {
            fatal_error(
                "HUBUUM_DATABASE_URL must be set if not provided as an argument",
                EXIT_CODE_CONFIG_ERROR,
            )
        })
    });

    // Initialize database connection
    let pool =
        init_pool_with_statement_timeout(&database_url, 1, admin_cli.db_statement_timeout_ms);

    if let Some(username) = admin_cli.reset_password {
        reset_password(pool, &username).await?;
    } else if admin_cli.audit_templates {
        audit_templates(
            pool,
            admin_cli.export_template_recursion_limit,
            admin_cli.export_template_fuel,
        )
        .await?;
    } else if admin_cli.export_template_health {
        export_template_health(pool).await?;
    } else {
        println!("No command specified. Use --help for usage information.");
    }

    Ok(())
}

async fn reset_password(pool: DbPool, username: &str) -> Result<(), ApiError> {
    let user = User::get_by_name(&pool, username).await?;
    let new_password = generate_random_password(32);
    user.set_password(&pool, &new_password).await?;
    println!("Password for user {username} reset to: {new_password}");
    Ok(())
}

async fn audit_templates(
    pool: DbPool,
    export_template_recursion_limit: usize,
    export_template_fuel: u64,
) -> Result<(), ApiError> {
    let templates = ExportTemplate::list_all(&pool).await?;
    let mut failures = Vec::new();

    for template in templates {
        let collection_templates = template.collection_siblings(&pool).await?;
        if let Err(error) = validate_template_with_limits(
            &template.name,
            &template.template,
            template.collection_id,
            &collection_templates,
            template.content_type,
            export_template_recursion_limit,
            export_template_fuel,
        ) {
            failures.push((template.collection_id, template.name, error.to_string()));
        }
    }

    if failures.is_empty() {
        println!("All export templates validated successfully.");
        return Ok(());
    }

    for (collection_id, template_name, error) in &failures {
        println!("collection={collection_id} template={template_name}: {error}");
    }

    Err(ApiError::BadRequest(format!(
        "{} template(s) failed validation",
        failures.len()
    )))
}

#[derive(Default)]
struct TemplateHealthRow {
    runs: usize,
    warning_total: i32,
    warning_max: i32,
    total_duration_total: i64,
    total_duration_max: i32,
}

async fn export_template_health(pool: DbPool) -> Result<(), ApiError> {
    let outputs = with_connection(&pool, async |conn| {
        export_task_outputs
            .load::<ExportTaskOutputRecord>(conn)
            .await
    })
    .await?;

    if outputs.is_empty() {
        println!("No stored export outputs found.");
        return Ok(());
    }

    let mut health = BTreeMap::<String, TemplateHealthRow>::new();
    for output in outputs {
        let key = output
            .template_name
            .unwrap_or_else(|| "<json output>".to_string());
        let entry = health.entry(key).or_default();
        entry.runs += 1;
        entry.warning_total += output.warning_count;
        entry.warning_max = entry.warning_max.max(output.warning_count);
        entry.total_duration_total += i64::from(output.total_duration_ms);
        entry.total_duration_max = entry.total_duration_max.max(output.total_duration_ms);
    }

    println!("Export template health:");
    for (template_name, row) in &health {
        let avg_warnings = row.warning_total as f64 / row.runs as f64;
        let avg_total_duration_ms = row.total_duration_total as f64 / row.runs as f64;
        println!(
            "template={} runs={} avg_warning_count={:.2} max_warning_count={} avg_total_duration_ms={:.2} max_total_duration_ms={}",
            template_name,
            row.runs,
            avg_warnings,
            row.warning_max,
            avg_total_duration_ms,
            row.total_duration_max
        );
    }

    println!("\nWarning-prone templates:");
    for (template_name, row) in health.iter().filter(|(_, row)| row.warning_total > 0) {
        println!(
            "template={} warning_runs={} max_warning_count={}",
            template_name, row.runs, row.warning_max
        );
    }

    println!("\nSlow templates:");
    for (template_name, row) in health.iter().filter(|(_, row)| row.total_duration_max > 0) {
        println!(
            "template={} avg_total_duration_ms={:.2} max_total_duration_ms={}",
            template_name,
            row.total_duration_total as f64 / row.runs as f64,
            row.total_duration_max
        );
    }

    Ok(())
}

fn init_logging(log_level: &str) {
    if !is_valid_log_level(log_level) {
        warn!("Invalid log level: {}", log_level);
        exit(EXIT_CODE_CONFIG_ERROR);
    }
    if let Err(err) = logger::init_json_logging(log_level) {
        warn!("{}", err);
        exit(EXIT_CODE_CONFIG_ERROR);
    }
}
