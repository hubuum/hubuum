use clap::Parser;
use diesel::prelude::*;
use std::collections::BTreeMap;
use std::process::exit;
use tracing::warn;
use tracing_subscriber::{
    filter::EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

use hubuum::db::{DbPool, init_pool, with_connection};
use hubuum::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use hubuum::logger;
use hubuum::models::report_template::{list_all_report_templates, report_templates_in_namespace};
use hubuum::models::{ReportTaskOutputRecord, User};
use hubuum::schema::report_task_outputs::dsl::report_task_outputs;
use hubuum::utilities::auth::generate_random_password;
use hubuum::utilities::is_valid_log_level;
use hubuum::utilities::reporting::validate_template;

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

    /// Validate all stored report templates against the Jinja renderer
    #[arg(long, default_value_t = false)]
    audit_templates: bool,

    /// Summarize stored report output health by template name
    #[arg(long, default_value_t = false)]
    report_template_health: bool,

    /// Database URL
    #[arg(long, env = "HUBUUM_DATABASE_URL")]
    database_url: Option<String>,

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
    let pool = init_pool(&database_url, 1);

    if let Some(username) = admin_cli.reset_password {
        reset_password(pool, &username).await?;
    } else if admin_cli.audit_templates {
        audit_templates(pool).await?;
    } else if admin_cli.report_template_health {
        report_template_health(pool).await?;
    } else {
        println!("No command specified. Use --help for usage information.");
    }

    Ok(())
}

async fn reset_password(pool: DbPool, username: &str) -> Result<(), ApiError> {
    let user = User::get_by_username(&pool, username).await?;
    let new_password = generate_random_password(32);
    user.set_password(&pool, &new_password).await?;
    println!("Password for user {username} reset to: {new_password}");
    Ok(())
}

async fn audit_templates(pool: DbPool) -> Result<(), ApiError> {
    let templates = list_all_report_templates(&pool).await?;
    let mut failures = Vec::new();

    for template in templates {
        let namespace_templates =
            report_templates_in_namespace(&pool, template.namespace_id, Some(template.id)).await?;
        if let Err(error) = validate_template(
            &template.name,
            &template.template,
            template.namespace_id,
            &namespace_templates,
            template.content_type,
        ) {
            failures.push((template.namespace_id, template.name, error.to_string()));
        }
    }

    if failures.is_empty() {
        println!("All report templates validated successfully.");
        return Ok(());
    }

    for (namespace_id, template_name, error) in &failures {
        println!("namespace={namespace_id} template={template_name}: {error}");
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

async fn report_template_health(pool: DbPool) -> Result<(), ApiError> {
    let outputs = with_connection(&pool, |conn| {
        report_task_outputs.load::<ReportTaskOutputRecord>(conn)
    })?;

    if outputs.is_empty() {
        println!("No stored report outputs found.");
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

    println!("Report template health:");
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
    let filter = if is_valid_log_level(log_level) {
        EnvFilter::try_new(log_level).unwrap_or_else(|_e| {
            warn!("Error parsing log level: {}", log_level);
            exit(EXIT_CODE_CONFIG_ERROR);
        })
    } else {
        warn!("Invalid log level: {}", log_level);
        exit(EXIT_CODE_CONFIG_ERROR);
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(FmtSpan::CLOSE)
                .event_format(logger::HubuumLoggingFormat),
        )
        .init();
}
