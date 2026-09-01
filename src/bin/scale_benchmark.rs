use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use hubuum::observability::scale_benchmark::{
    DatasetManifest, LimitMode, LoadReport, MeasureOptions, ProfileName, ScaleAssessment,
    ScaleBenchmarkReport, ScaleProfile, WorkloadSpec, load_dataset, measure_scale_benchmark,
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[command(about = "Generate, load, measure, and assess Hubuum scale benchmarks")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write the deterministic semantic manifest without loading PostgreSQL.
    Manifest(ManifestArgs),
    /// Load one freshly migrated PostgreSQL database and verify every invariant.
    Load(LoadArgs),
    /// Measure an already loaded database through a production server process.
    Measure(MeasureArgs),
    /// Load and measure a fresh database in one local-friendly command.
    Run(RunArgs),
    /// Compare equivalent base/head reports and fail only on correctness drift.
    Assess(AssessArgs),
}

#[derive(Debug, Args)]
struct ProfileArgs {
    #[arg(long, value_parser = parse_profile_name)]
    profile: ProfileName,
    #[arg(long)]
    profile_spec: Option<PathBuf>,
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Debug, Args)]
struct ManifestArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Args)]
struct LoadArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[arg(long)]
    database_url: String,
    #[arg(long)]
    manifest_output: PathBuf,
    #[arg(long)]
    load_report_output: PathBuf,
}

#[derive(Debug, Args)]
struct CommonMeasureArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[arg(long)]
    server_binary: PathBuf,
    #[arg(long)]
    admin_binary: Option<PathBuf>,
    #[arg(long)]
    database_url: String,
    #[arg(long)]
    restore_test_database_url: Option<String>,
    #[arg(long)]
    workload_spec: Option<PathBuf>,
    #[arg(long, value_parser = parse_limit_mode)]
    limit_mode: LimitMode,
    #[arg(long)]
    label: String,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    artifact_directory: PathBuf,
    #[arg(long, default_value_t = 18_280)]
    port: u16,
    #[arg(long, default_value_t = 120)]
    startup_timeout_seconds: u64,
    #[arg(long)]
    skip_lifecycle: bool,
}

#[derive(Debug, Args)]
struct MeasureArgs {
    #[command(flatten)]
    common: CommonMeasureArgs,
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    load_report: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[command(flatten)]
    common: CommonMeasureArgs,
    #[arg(long)]
    manifest_output: PathBuf,
    #[arg(long)]
    load_report_output: PathBuf,
}

#[derive(Debug, Args)]
struct AssessArgs {
    #[arg(long)]
    head: PathBuf,
    #[arg(long)]
    base: Option<PathBuf>,
    #[arg(long)]
    markdown_output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Manifest(args) => manifest(args),
        Command::Load(args) => load(args).await,
        Command::Measure(args) => measure(args).await,
        Command::Run(args) => run(args).await,
        Command::Assess(args) => assess(args),
    }
}

fn manifest(args: ManifestArgs) -> Result<()> {
    let profile = load_profile(&args.profile)?;
    let manifest = profile.manifest()?;
    manifest.write(&args.output)?;
    println!(
        "Wrote {} profile manifest {} to {}",
        profile.name.as_str(),
        manifest.semantic_digest,
        args.output.display()
    );
    Ok(())
}

async fn load(args: LoadArgs) -> Result<()> {
    let profile = load_profile(&args.profile)?;
    let report = load_dataset(&profile, &args.database_url).await?;
    report.manifest.write(&args.manifest_output)?;
    write_json(&args.load_report_output, &report)?;
    println!(
        "Loaded and verified {} profile in {} ms",
        profile.name.as_str(),
        report.loading_ms
    );
    Ok(())
}

async fn measure(args: MeasureArgs) -> Result<()> {
    let profile = load_profile(&args.common.profile)?;
    let manifest = DatasetManifest::read(&args.manifest)?;
    let (generation_ms, loading_ms) = args
        .load_report
        .as_deref()
        .map(read_load_report)
        .transpose()?
        .map(|report| (report.generation_ms, report.loading_ms))
        .unwrap_or_default();
    measure_common(args.common, profile, manifest, generation_ms, loading_ms).await
}

async fn run(args: RunArgs) -> Result<()> {
    let profile = load_profile(&args.common.profile)?;
    let load_report = load_dataset(&profile, &args.common.database_url).await?;
    load_report.manifest.write(&args.manifest_output)?;
    write_json(&args.load_report_output, &load_report)?;
    measure_common(
        args.common,
        profile,
        load_report.manifest,
        load_report.generation_ms,
        load_report.loading_ms,
    )
    .await
}

async fn measure_common(
    args: CommonMeasureArgs,
    profile: ScaleProfile,
    manifest: DatasetManifest,
    generation_ms: u64,
    loading_ms: u64,
) -> Result<()> {
    let workload = match args.workload_spec.as_deref() {
        Some(path) => WorkloadSpec::read(path)?,
        None => WorkloadSpec::bundled()?,
    };
    let output = args.output.clone();
    let report = measure_scale_benchmark(
        MeasureOptions {
            server_binary: args.server_binary,
            admin_binary: args.admin_binary,
            database_url: args.database_url,
            restore_test_database_url: args.restore_test_database_url,
            artifact_directory: args.artifact_directory,
            label: args.label,
            port: args.port,
            limit_mode: args.limit_mode,
            run_lifecycle: !args.skip_lifecycle,
            startup_timeout: Duration::from_secs(args.startup_timeout_seconds),
        },
        profile,
        manifest,
        workload,
        generation_ms,
        loading_ms,
    )
    .await?;
    report.write(&output)?;
    println!(
        "Wrote scale benchmark report to {} ({} scenarios, correctness={})",
        output.display(),
        report.scenarios.len(),
        if report.correctness.passed() {
            "passed"
        } else {
            "failed"
        }
    );
    if report.correctness.passed() {
        Ok(())
    } else {
        Err(io_error("scale benchmark correctness checks failed"))
    }
}

fn assess(args: AssessArgs) -> Result<()> {
    let head = ScaleBenchmarkReport::read(&args.head)?;
    let base = args
        .base
        .as_deref()
        .map(ScaleBenchmarkReport::read)
        .transpose()?;
    let assessment = ScaleAssessment::assess(&head, base.as_ref());
    print!("{}", assessment.markdown());
    if let Some(path) = args.markdown_output.as_deref() {
        assessment.append_markdown(path)?;
    }
    assessment.ensure_passed()
}

fn load_profile(args: &ProfileArgs) -> Result<ScaleProfile> {
    let profile = match args.profile_spec.as_deref() {
        Some(path) => ScaleProfile::read(path)?,
        None => ScaleProfile::bundled(args.profile)?,
    };
    if profile.name != args.profile {
        return Err(io_error(format!(
            "profile document declares '{}', but --profile requested '{}'",
            profile.name.as_str(),
            args.profile.as_str()
        )));
    }
    match args.seed {
        Some(seed) => profile.with_seed(seed),
        None => Ok(profile),
    }
}

fn read_load_report(path: &Path) -> Result<LoadReport> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn parse_profile_name(value: &str) -> std::result::Result<ProfileName, String> {
    value.parse()
}

fn parse_limit_mode(value: &str) -> std::result::Result<LimitMode, String> {
    value.parse()
}

fn io_error(message: impl Into<String>) -> Error {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}
