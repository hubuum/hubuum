use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use hubuum_scale_core::{
    BackendComparisonReport, DatasetManifest, LimitMode, LoadReport, ProfileName, ScaleAssessment,
    ScaleAxis, ScaleBenchmarkBackend, ScaleBenchmarkReport, ScaleImpactReport, ScaleProfile,
    ScaleSensitivityReport, SensitivitySpec, WorkloadSpec,
};
use hubuum_storage_postgres::scale_benchmark::PostgresScaleBackend;

mod metrics;
mod runner;

use runner::{MeasureOptions, measure_scale_benchmark};

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
    /// Write the deterministic semantic manifest without loading a backend.
    Manifest(ManifestArgs),
    /// Load one fresh selected backend and verify every invariant.
    Load(LoadArgs),
    /// Measure an already loaded database through a production server process.
    Measure(MeasureArgs),
    /// Load and measure a fresh backend in one local-friendly command.
    Run(RunArgs),
    /// Compare equivalent base/head reports and fail only on correctness drift.
    Assess(AssessArgs),
    /// Compare reports that differ along exactly one controlled scale axis.
    Impact(ImpactArgs),
    /// Expand the calibrated sensitivity matrix into exact corpus increments.
    SensitivityPlan(SensitivityPlanArgs),
    /// Summarize every controlled corpus-growth impact in one report.
    SummarizeSensitivity(SummarizeSensitivityArgs),
    /// Render matching reports from each selected storage backend side by side.
    CompareBackends(CompareBackendsArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BackendName {
    Postgres,
}

impl BackendName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Args)]
struct ProfileArgs {
    #[arg(long, value_parser = parse_profile_name)]
    profile: ProfileName,
    #[arg(long)]
    profile_spec: Option<PathBuf>,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, conflicts_with = "add_object_relations")]
    add_objects: Option<u64>,
    #[arg(long, conflicts_with = "add_objects")]
    add_object_relations: Option<u64>,
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
    #[arg(long, value_enum, default_value_t = BackendName::Postgres)]
    backend: BackendName,
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
    #[arg(long, value_enum, default_value_t = BackendName::Postgres)]
    backend: BackendName,
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

#[derive(Debug, Args)]
struct ImpactArgs {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    comparison: PathBuf,
    #[arg(long, value_parser = parse_scale_axis)]
    axis: ScaleAxis,
    #[arg(long)]
    normalization_unit: Option<u64>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    markdown_output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct SensitivityPlanArgs {
    #[command(flatten)]
    profile: ProfileArgs,
    #[arg(long)]
    sensitivity_spec: Option<PathBuf>,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Args)]
struct SummarizeSensitivityArgs {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    sensitivity_spec: Option<PathBuf>,
    #[arg(long, required = true, num_args = 1..)]
    impacts: Vec<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    markdown_output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct CompareBackendsArgs {
    #[arg(long, required = true, num_args = 1..)]
    reports: Vec<PathBuf>,
    #[arg(long)]
    output: PathBuf,
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
        Command::Impact(args) => impact(args),
        Command::SensitivityPlan(args) => sensitivity_plan(args),
        Command::SummarizeSensitivity(args) => summarize_sensitivity(args),
        Command::CompareBackends(args) => compare_backends(args),
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
    let report = match args.backend {
        BackendName::Postgres => {
            postgres_backend(&args.database_url, &profile)?
                .load_dataset(&profile)
                .await?
        }
    };
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
    let load_report = args
        .load_report
        .as_deref()
        .map(read_load_report)
        .transpose()?;
    if let Some(report) = &load_report
        && report.backend != args.common.backend.as_str()
    {
        return Err(io_error(format!(
            "load report backend '{}' does not match selected backend '{}'",
            report.backend,
            args.common.backend.as_str()
        )));
    }
    let (generation_ms, loading_ms) = load_report
        .map(|report| (report.generation_ms, report.loading_ms))
        .unwrap_or_default();
    measure_common(args.common, profile, manifest, generation_ms, loading_ms).await
}

async fn run(args: RunArgs) -> Result<()> {
    let profile = load_profile(&args.common.profile)?;
    let load_report = match args.common.backend {
        BackendName::Postgres => {
            postgres_backend(&args.common.database_url, &profile)?
                .load_dataset(&profile)
                .await?
        }
    };
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
    let backend = match args.backend {
        BackendName::Postgres => postgres_backend(&args.database_url, &profile)?,
    };
    let report = measure_scale_benchmark(
        &backend,
        MeasureOptions {
            server_binary: args.server_binary,
            admin_binary: args.admin_binary,
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

fn postgres_backend(database_url: &str, profile: &ScaleProfile) -> Result<PostgresScaleBackend> {
    PostgresScaleBackend::connect(database_url, profile.provisioning.db_pool_size as u32)
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

fn impact(args: ImpactArgs) -> Result<()> {
    let baseline = ScaleBenchmarkReport::read(&args.baseline)?;
    let comparison = ScaleBenchmarkReport::read(&args.comparison)?;
    let impact =
        ScaleImpactReport::compare(&baseline, &comparison, args.axis, args.normalization_unit)?;
    impact.write(&args.output)?;
    let markdown = impact.markdown();
    print!("{markdown}");
    if let Some(path) = args.markdown_output.as_deref() {
        impact.append_markdown(path)?;
    }
    Ok(())
}

fn sensitivity_plan(args: SensitivityPlanArgs) -> Result<()> {
    let profile = load_profile(&args.profile)?;
    let spec = load_sensitivity_spec(args.sensitivity_spec.as_deref())?;
    let plan = spec.plan(&profile)?;
    plan.write(&args.output)?;
    println!(
        "Wrote {} calibrated scale sensitivity points to {}",
        plan.points.len(),
        args.output.display()
    );
    Ok(())
}

fn summarize_sensitivity(args: SummarizeSensitivityArgs) -> Result<()> {
    let baseline = ScaleBenchmarkReport::read(&args.baseline)?;
    let impacts = args
        .impacts
        .iter()
        .map(|path| ScaleImpactReport::read(path))
        .collect::<hubuum_scale_core::Result<Vec<_>>>()?;
    let spec = load_sensitivity_spec(args.sensitivity_spec.as_deref())?;
    let summary = ScaleSensitivityReport::summarize(&baseline, &impacts, &spec)?;
    summary.write(&args.output)?;
    let markdown = summary.markdown();
    print!("{markdown}");
    if let Some(path) = args.markdown_output.as_deref() {
        summary.append_markdown(path)?;
    }
    Ok(())
}

fn compare_backends(args: CompareBackendsArgs) -> Result<()> {
    let reports = args
        .reports
        .iter()
        .map(|path| ScaleBenchmarkReport::read(path))
        .collect::<hubuum_scale_core::Result<Vec<_>>>()?;
    let comparison = BackendComparisonReport::compare(&reports)?;
    comparison.write(&args.output)?;
    let markdown = comparison.markdown();
    print!("{markdown}");
    if let Some(path) = args.markdown_output.as_deref() {
        comparison.append_markdown(path)?;
    }
    Ok(())
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
    let profile = match args.seed {
        Some(seed) => profile.with_seed(seed),
        None => Ok(profile),
    }?;
    match (args.add_objects, args.add_object_relations) {
        (Some(amount), None) => profile.with_increment(ScaleAxis::Objects, amount),
        (None, Some(amount)) => profile.with_increment(ScaleAxis::ObjectRelations, amount),
        (None, None) => Ok(profile),
        (Some(_), Some(_)) => Err(io_error(
            "only one scale axis may be changed in a controlled experiment",
        )),
    }
}

fn load_sensitivity_spec(path: Option<&Path>) -> Result<SensitivitySpec> {
    match path {
        Some(path) => Ok(SensitivitySpec::read(path)?),
        None => Ok(SensitivitySpec::bundled()?),
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

fn parse_scale_axis(value: &str) -> std::result::Result<ScaleAxis, String> {
    value.parse()
}

fn io_error(message: impl Into<String>) -> Error {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}
