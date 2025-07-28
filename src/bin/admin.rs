use clap::Parser;
use tracing::warn;
use tracing_subscriber::{
    filter::EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

use hubuum::db::{init_pool, DbPool};
use hubuum::errors::ApiError;
use hubuum::logger;
use hubuum::models::User;
use hubuum::utilities::auth::generate_random_password;
use hubuum::utilities::is_valid_log_level;

#[derive(Parser)]
#[command(author = "Terje Kvernes <terje@kvernes.no>", version = "0.0.1", about = "Admin CLI for Hubuum", long_about = None)]
struct AdminCli {
    /// Reset the password for the specified username
    #[arg(long)]
    reset_password: Option<String>,

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
        std::env::var("HUBUUM_DATABASE_URL")
            .expect("HUBUUM_DATABASE_URL must be set if not provided as an argument")
    });

    // Initialize database connection
    let pool = init_pool(&database_url, 1);

    if let Some(username) = admin_cli.reset_password {
        reset_password(pool, &username).await?;
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

fn init_logging(log_level: &str) {
    let filter = if is_valid_log_level(log_level) {
        EnvFilter::try_new(log_level).unwrap_or_else(|_e| {
            warn!("Error parsing log level: {}", log_level);
            std::process::exit(1);
        })
    } else {
        warn!("Invalid log level: {}", log_level);
        std::process::exit(1);
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
