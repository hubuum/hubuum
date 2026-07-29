//! Config-free data model and assessment helpers for the runtime behavior benchmark.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u32 = 1;
const DB_ACQUISITION_COUNTER: &str = "hubuum_db_connection_acquire_duration_seconds_count";
const DB_ACQUISITION_FAILURE_COUNTER: &str = "hubuum_db_connection_acquire_failures_total";
const DB_OPERATION_ERROR_COUNTER: &str = "hubuum_db_operation_errors_total";
const PROCESS_START_TIME: &str = "hubuum_process_start_time_seconds";
const TASK_IDLE_COUNTER: &str = "hubuum_task_worker_iterations_total";
const FANOUT_WAKEUP_COUNTER: &str = "hubuum_event_worker_wakeups_total";

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricSnapshot {
    values: BTreeMap<MetricKey, f64>,
}

impl MetricSnapshot {
    pub fn parse(text: &str) -> Result<Self> {
        let mut values = BTreeMap::new();
        for (index, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (identifier, value) = line
                .split_once(char::is_whitespace)
                .ok_or_else(|| invalid_data(format!("metrics line {} has no value", index + 1)))?;
            let value = value
                .trim()
                .parse::<f64>()
                .map_err(|error| invalid_data(format!("invalid metrics value: {error}")))?;
            let (name, labels) = parse_identifier(identifier)?;
            let key = MetricKey { name, labels };
            if values.insert(key, value).is_some() {
                return Err(invalid_data(format!(
                    "metrics line {} duplicates a series",
                    index + 1
                )));
            }
        }
        Ok(Self { values })
    }

    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        let labels = labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        self.values
            .get(&MetricKey {
                name: name.to_string(),
                labels,
            })
            .copied()
            .unwrap_or_default()
    }

    pub fn counter_delta(&self, later: &Self, name: &str, labels: &[(&str, &str)]) -> Result<f64> {
        counter_delta(self.value(name, labels), later.value(name, labels), name)
    }

    pub fn total_counter_delta(&self, later: &Self, name: &str) -> Result<f64> {
        let before = self
            .values
            .iter()
            .filter(|(key, _)| key.name == name)
            .map(|(_, value)| *value)
            .sum();
        let after = later
            .values
            .iter()
            .filter(|(key, _)| key.name == name)
            .map(|(_, value)| *value)
            .sum();
        counter_delta(before, after, name)
    }

    pub fn counter_deltas_by_label(
        &self,
        later: &Self,
        name: &str,
        label: &str,
    ) -> Result<BTreeMap<String, f64>> {
        let label_values = self
            .values
            .keys()
            .chain(later.values.keys())
            .filter(|key| key.name == name)
            .filter_map(|key| key.labels.get(label))
            .cloned()
            .collect::<BTreeSet<_>>();
        label_values
            .into_iter()
            .map(|value| {
                let delta = self.counter_delta(later, name, &[(label, value.as_str())])?;
                Ok((value, delta))
            })
            .collect()
    }

    pub fn assert_same_process(&self, later: &Self) -> Result<()> {
        let before = self.value(PROCESS_START_TIME, &[]);
        let after = later.value(PROCESS_START_TIME, &[]);
        if before <= 0.0 || before != after {
            return Err(invalid_data(format!(
                "process changed during measurement (start time {before} -> {after})"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProcessIdleReport {
    role: String,
    sample_seconds: f64,
    db_acquisitions_by_caller: BTreeMap<String, f64>,
    background_db_acquisitions_per_second: f64,
    task_idle_iterations_per_second: f64,
    fanout_poll_wakeups_per_second: f64,
    restore_coordinator_acquisitions_per_second: f64,
    task_worker_acquisitions_per_idle_iteration: Option<f64>,
    fanout_acquisitions_per_poll_wakeup: Option<f64>,
    db_acquisition_failures: f64,
    db_operation_errors: f64,
}

impl ProcessIdleReport {
    pub fn from_snapshots(
        role: impl Into<String>,
        sample_seconds: f64,
        before: &MetricSnapshot,
        after: &MetricSnapshot,
    ) -> Result<Self> {
        if !sample_seconds.is_finite() || sample_seconds <= 0.0 {
            return Err(invalid_data("sample duration must be positive"));
        }
        before.assert_same_process(after)?;
        let db_acquisitions_by_caller =
            before.counter_deltas_by_label(after, DB_ACQUISITION_COUNTER, "caller")?;
        let metrics_refresh = db_acquisitions_by_caller
            .get("metrics_refresh")
            .copied()
            .unwrap_or_default();
        let background_db_acquisitions =
            db_acquisitions_by_caller.values().sum::<f64>() - metrics_refresh;
        let task_idle_iterations =
            before.counter_delta(after, TASK_IDLE_COUNTER, &[("outcome", "idle")])?;
        let fanout_poll_wakeups = before.counter_delta(
            after,
            FANOUT_WAKEUP_COUNTER,
            &[("worker", "fanout"), ("kind", "poll")],
        )?;
        let task_worker_acquisitions = db_acquisitions_by_caller
            .get("task_worker")
            .copied()
            .unwrap_or_default();
        let fanout_acquisitions = db_acquisitions_by_caller
            .get("event_fanout")
            .copied()
            .unwrap_or_default();
        let restore_acquisitions = db_acquisitions_by_caller
            .get("restore_coordinator")
            .copied()
            .unwrap_or_default();

        Ok(Self {
            role: role.into(),
            sample_seconds,
            db_acquisitions_by_caller,
            background_db_acquisitions_per_second: background_db_acquisitions / sample_seconds,
            task_idle_iterations_per_second: task_idle_iterations / sample_seconds,
            fanout_poll_wakeups_per_second: fanout_poll_wakeups / sample_seconds,
            restore_coordinator_acquisitions_per_second: restore_acquisitions / sample_seconds,
            task_worker_acquisitions_per_idle_iteration: ratio(
                task_worker_acquisitions,
                task_idle_iterations,
            ),
            fanout_acquisitions_per_poll_wakeup: ratio(fanout_acquisitions, fanout_poll_wakeups),
            db_acquisition_failures: before
                .total_counter_delta(after, DB_ACQUISITION_FAILURE_COUNTER)?,
            db_operation_errors: before.total_counter_delta(after, DB_OPERATION_ERROR_COUNTER)?,
        })
    }

    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn background_db_acquisitions_per_second(&self) -> f64 {
        self.background_db_acquisitions_per_second
    }

    pub fn task_idle_iterations_per_second(&self) -> f64 {
        self.task_idle_iterations_per_second
    }

    pub fn fanout_poll_wakeups_per_second(&self) -> f64 {
        self.fanout_poll_wakeups_per_second
    }

    pub fn restore_coordinator_acquisitions_per_second(&self) -> f64 {
        self.restore_coordinator_acquisitions_per_second
    }

    pub fn task_worker_acquisitions_per_idle_iteration(&self) -> Option<f64> {
        self.task_worker_acquisitions_per_idle_iteration
    }

    pub fn fanout_acquisitions_per_poll_wakeup(&self) -> Option<f64> {
        self.fanout_acquisitions_per_poll_wakeup
    }

    pub fn db_acquisition_failures(&self) -> f64 {
        self.db_acquisition_failures
    }

    pub fn db_operation_errors(&self) -> f64 {
        self.db_operation_errors
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReadinessReport {
    requests: u32,
    successful_requests: u32,
    db_acquisitions: f64,
    mean_latency_ms: f64,
    maximum_latency_ms: f64,
}

impl ReadinessReport {
    pub fn new(
        requests: u32,
        successful_requests: u32,
        db_acquisitions: f64,
        latencies_ms: &[f64],
    ) -> Result<Self> {
        if latencies_ms.len() != requests as usize {
            return Err(invalid_data(
                "readiness latency count must equal the request count",
            ));
        }
        let mean_latency_ms = if latencies_ms.is_empty() {
            0.0
        } else {
            latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64
        };
        let maximum_latency_ms = latencies_ms.iter().copied().fold(0.0, f64::max);
        Ok(Self {
            requests,
            successful_requests,
            db_acquisitions,
            mean_latency_ms,
            maximum_latency_ms,
        })
    }

    fn is_exact(&self) -> bool {
        self.successful_requests == self.requests
            && (self.db_acquisitions - f64::from(self.requests)).abs() < f64::EPSILON
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskNotificationReport {
    warmup_wakeup_latency_ms: f64,
    claim_latency_ms: f64,
    terminal_status: String,
}

impl TaskNotificationReport {
    pub fn new(
        warmup_wakeup_latency_ms: f64,
        claim_latency_ms: f64,
        terminal_status: impl Into<String>,
    ) -> Self {
        Self {
            warmup_wakeup_latency_ms,
            claim_latency_ms,
            terminal_status: terminal_status.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuntimeBehaviorReport {
    schema_version: u32,
    label: String,
    primary: ProcessIdleReport,
    standby: ProcessIdleReport,
    aggregate_background_db_acquisitions_per_second: f64,
    primary_readiness: ReadinessReport,
    standby_readiness: ReadinessReport,
    task_notification: TaskNotificationReport,
}

impl RuntimeBehaviorReport {
    pub fn new(
        label: impl Into<String>,
        primary: ProcessIdleReport,
        standby: ProcessIdleReport,
        primary_readiness: ReadinessReport,
        standby_readiness: ReadinessReport,
        task_notification: TaskNotificationReport,
    ) -> Self {
        let aggregate_background_db_acquisitions_per_second = primary
            .background_db_acquisitions_per_second
            + standby.background_db_acquisitions_per_second;
        Self {
            schema_version: SCHEMA_VERSION,
            label: label.into(),
            primary,
            standby,
            aggregate_background_db_acquisitions_per_second,
            primary_readiness,
            standby_readiness,
            task_notification,
        }
    }

    pub fn read(path: &Path) -> Result<Self> {
        let report: Self = serde_json::from_slice(&fs::read(path)?)?;
        if report.schema_version != SCHEMA_VERSION {
            return Err(invalid_data(format!(
                "unsupported runtime report schema {}; expected {SCHEMA_VERSION}",
                report.schema_version
            )));
        }
        Ok(report)
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }

    pub fn primary(&self) -> &ProcessIdleReport {
        &self.primary
    }

    pub fn standby(&self) -> &ProcessIdleReport {
        &self.standby
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RuntimeBehaviorBudgets {
    pub max_primary_background_db_per_second: f64,
    pub max_standby_background_db_per_second: f64,
    pub max_aggregate_background_db_per_second: f64,
    pub max_task_idle_per_second: f64,
    pub max_fanout_poll_per_second: f64,
    pub max_restore_db_per_second: f64,
    pub max_task_worker_db_per_idle_iteration: f64,
    pub max_fanout_db_per_poll_wakeup: f64,
    pub max_notification_latency_ms: f64,
    pub max_task_claim_latency_ms: f64,
    pub max_relative_regression_percent: f64,
}

#[derive(Clone, Debug)]
pub struct RuntimeBehaviorAssessment {
    markdown: String,
    failures: Vec<String>,
}

impl RuntimeBehaviorAssessment {
    pub fn assess(
        head: &RuntimeBehaviorReport,
        base: Option<&RuntimeBehaviorReport>,
        budgets: RuntimeBehaviorBudgets,
    ) -> Self {
        let mut failures = Vec::new();
        let rows = measurement_rows(head, base);

        check_maximum(
            &mut failures,
            "primary background DB acquisitions/s",
            head.primary.background_db_acquisitions_per_second,
            budgets.max_primary_background_db_per_second,
        );
        check_maximum(
            &mut failures,
            "standby background DB acquisitions/s",
            head.standby.background_db_acquisitions_per_second,
            budgets.max_standby_background_db_per_second,
        );
        check_maximum(
            &mut failures,
            "aggregate background DB acquisitions/s",
            head.aggregate_background_db_acquisitions_per_second,
            budgets.max_aggregate_background_db_per_second,
        );
        check_maximum(
            &mut failures,
            "task idle iterations/s",
            head.primary.task_idle_iterations_per_second,
            budgets.max_task_idle_per_second,
        );
        check_maximum(
            &mut failures,
            "fan-out poll wakeups/s",
            head.primary.fanout_poll_wakeups_per_second,
            budgets.max_fanout_poll_per_second,
        );
        check_maximum(
            &mut failures,
            "primary restore coordinator acquisitions/s",
            head.primary.restore_coordinator_acquisitions_per_second,
            budgets.max_restore_db_per_second,
        );
        check_maximum(
            &mut failures,
            "standby restore coordinator acquisitions/s",
            head.standby.restore_coordinator_acquisitions_per_second,
            budgets.max_restore_db_per_second,
        );
        check_optional_maximum(
            &mut failures,
            "task-worker acquisitions/idle iteration",
            head.primary.task_worker_acquisitions_per_idle_iteration,
            budgets.max_task_worker_db_per_idle_iteration,
        );
        check_optional_maximum(
            &mut failures,
            "fan-out acquisitions/poll wakeup",
            head.primary.fanout_acquisitions_per_poll_wakeup,
            budgets.max_fanout_db_per_poll_wakeup,
        );
        check_maximum(
            &mut failures,
            "notification wakeup latency (ms)",
            head.task_notification.warmup_wakeup_latency_ms,
            budgets.max_notification_latency_ms,
        );
        check_maximum(
            &mut failures,
            "task claim latency (ms)",
            head.task_notification.claim_latency_ms,
            budgets.max_task_claim_latency_ms,
        );

        if !head.primary_readiness.is_exact() {
            failures.push(format!(
                "primary readiness used {} DB acquisitions for {} successful requests out of {}",
                head.primary_readiness.db_acquisitions,
                head.primary_readiness.successful_requests,
                head.primary_readiness.requests
            ));
        }
        if !head.standby_readiness.is_exact() {
            failures.push(format!(
                "standby readiness used {} DB acquisitions for {} successful requests out of {}",
                head.standby_readiness.db_acquisitions,
                head.standby_readiness.successful_requests,
                head.standby_readiness.requests
            ));
        }
        for process in [&head.primary, &head.standby] {
            if process.db_acquisition_failures > 0.0 {
                failures.push(format!(
                    "{} recorded {} DB acquisition failures",
                    process.role, process.db_acquisition_failures
                ));
            }
            if process.db_operation_errors > 0.0 {
                failures.push(format!(
                    "{} recorded {} DB operation errors",
                    process.role, process.db_operation_errors
                ));
            }
        }
        if head.task_notification.terminal_status != "failed" {
            failures.push(format!(
                "synthetic task ended in unexpected status '{}'",
                head.task_notification.terminal_status
            ));
        }

        if base.is_some() {
            let regression_factor = 1.0 + budgets.max_relative_regression_percent / 100.0;
            for row in &rows {
                if row.regression_sensitive
                    && row.base.is_some_and(|base_value| {
                        base_value > 0.0 && row.head > base_value * regression_factor
                    })
                {
                    failures.push(format!(
                        "{} regressed by more than {:.1}% ({:.3} -> {:.3})",
                        row.name,
                        budgets.max_relative_regression_percent,
                        row.base.unwrap_or_default(),
                        row.head
                    ));
                }
            }
        }

        let markdown = assessment_markdown(head, base, &rows, &failures);
        Self { markdown, failures }
    }

    pub fn append_markdown(&self, path: &Path) -> Result<()> {
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        file.write_all(self.markdown.as_bytes())?;
        Ok(())
    }

    pub fn markdown(&self) -> &str {
        &self.markdown
    }

    pub fn ensure_passed(&self) -> Result<()> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(invalid_data(self.failures.join("; ")))
        }
    }
}

#[derive(Clone, Debug)]
struct MeasurementRow {
    name: &'static str,
    base: Option<f64>,
    head: f64,
    regression_sensitive: bool,
}

fn measurement_rows(
    head: &RuntimeBehaviorReport,
    base: Option<&RuntimeBehaviorReport>,
) -> Vec<MeasurementRow> {
    macro_rules! row {
        ($name:literal, $field:expr, $sensitive:expr) => {
            MeasurementRow {
                name: $name,
                base: base.map($field),
                head: $field(head),
                regression_sensitive: $sensitive,
            }
        };
    }
    vec![
        row!(
            "Primary background DB acquisitions/s",
            |report: &RuntimeBehaviorReport| report.primary.background_db_acquisitions_per_second,
            true
        ),
        row!(
            "Standby background DB acquisitions/s",
            |report: &RuntimeBehaviorReport| report.standby.background_db_acquisitions_per_second,
            true
        ),
        row!(
            "Aggregate background DB acquisitions/s",
            |report: &RuntimeBehaviorReport| report.aggregate_background_db_acquisitions_per_second,
            true
        ),
        row!(
            "Task idle iterations/s",
            |report: &RuntimeBehaviorReport| report.primary.task_idle_iterations_per_second,
            true
        ),
        row!(
            "Fan-out poll wakeups/s",
            |report: &RuntimeBehaviorReport| report.primary.fanout_poll_wakeups_per_second,
            true
        ),
        row!(
            "Task-worker acquisitions/idle iteration",
            |report: &RuntimeBehaviorReport| report
                .primary
                .task_worker_acquisitions_per_idle_iteration
                .unwrap_or_default(),
            true
        ),
        row!(
            "Fan-out acquisitions/poll wakeup",
            |report: &RuntimeBehaviorReport| report
                .primary
                .fanout_acquisitions_per_poll_wakeup
                .unwrap_or_default(),
            true
        ),
        row!(
            "Notification wakeup latency (ms)",
            |report: &RuntimeBehaviorReport| report.task_notification.warmup_wakeup_latency_ms,
            false
        ),
        row!(
            "Task claim latency (ms)",
            |report: &RuntimeBehaviorReport| report.task_notification.claim_latency_ms,
            false
        ),
    ]
}

fn assessment_markdown(
    head: &RuntimeBehaviorReport,
    base: Option<&RuntimeBehaviorReport>,
    rows: &[MeasurementRow],
    failures: &[String],
) -> String {
    let mut markdown = String::from("## Runtime behavior benchmark\n\n");
    markdown.push_str("| Measurement | Base | Head | Change |\n");
    markdown.push_str("| --- | ---: | ---: | ---: |\n");
    for row in rows {
        let base_value = row
            .base
            .map_or_else(|| "—".to_string(), |value| format!("{value:.3}"));
        let change = row.base.map_or_else(
            || "initial".to_string(),
            |value| {
                if value == 0.0 {
                    "n/a".to_string()
                } else {
                    format!("{:+.1}%", ((row.head / value) - 1.0) * 100.0)
                }
            },
        );
        markdown.push_str(&format!(
            "| {} | {} | {:.3} | {} |\n",
            row.name, base_value, row.head, change
        ));
    }
    markdown.push('\n');
    markdown.push_str(&format!(
        "Head report: `{}`. Primary/standby readiness used exactly one DB acquisition per successful request.\n\n",
        head.label
    ));
    if let Some(base) = base {
        markdown.push_str(&format!("Base report: `{}`.\n\n", base.label));
    } else {
        markdown.push_str(
            "This is the initial report; base/head comparison begins after this benchmark lands on the base branch.\n\n",
        );
    }
    if failures.is_empty() {
        markdown.push_str("Result: passed all absolute budgets and comparison checks.\n");
    } else {
        markdown.push_str("Result: failed.\n\n");
        for failure in failures {
            markdown.push_str(&format!("- {failure}\n"));
        }
    }
    markdown
}

fn parse_identifier(identifier: &str) -> Result<(String, BTreeMap<String, String>)> {
    let Some(open) = identifier.find('{') else {
        return Ok((identifier.to_string(), BTreeMap::new()));
    };
    let close = identifier
        .strip_suffix('}')
        .ok_or_else(|| invalid_data("metric labels are not closed"))?
        .len();
    let name = identifier[..open].to_string();
    let labels = parse_labels(&identifier[open + 1..close])?;
    Ok((name, labels))
}

fn parse_labels(mut input: &str) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    while !input.trim().is_empty() {
        input = input.trim_start();
        let equals = input
            .find('=')
            .ok_or_else(|| invalid_data("metric label has no equals sign"))?;
        let key = input[..equals].trim();
        input = &input[equals + 1..];
        let rest = input
            .strip_prefix('"')
            .ok_or_else(|| invalid_data("metric label value is not quoted"))?;
        let (value, consumed) = parse_quoted(rest)?;
        if labels.insert(key.to_string(), value).is_some() {
            return Err(invalid_data(format!("duplicate metric label '{key}'")));
        }
        input = &rest[consumed..];
        input = input.trim_start();
        if let Some(rest) = input.strip_prefix(',') {
            input = rest;
        } else if !input.is_empty() {
            return Err(invalid_data("metric labels are not comma-separated"));
        }
    }
    Ok(labels)
}

fn parse_quoted(input: &str) -> Result<(String, usize)> {
    let mut value = String::new();
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            value.push(match character {
                'n' => '\n',
                '\\' => '\\',
                '"' => '"',
                other => other,
            });
            escaped = false;
        } else {
            match character {
                '\\' => escaped = true,
                '"' => return Ok((value, index + character.len_utf8())),
                other => value.push(other),
            }
        }
    }
    Err(invalid_data("metric label value is not closed"))
}

fn counter_delta(before: f64, after: f64, name: &str) -> Result<f64> {
    if !before.is_finite() || !after.is_finite() || after < before {
        return Err(invalid_data(format!(
            "counter '{name}' moved backwards or was non-finite ({before} -> {after})"
        )));
    }
    Ok(after - before)
}

fn ratio(numerator: f64, denominator: f64) -> Option<f64> {
    (denominator > 0.0).then_some(numerator / denominator)
}

fn check_maximum(failures: &mut Vec<String>, name: &str, value: f64, maximum: f64) {
    if value > maximum {
        failures.push(format!("{name} exceeded {maximum:.3}: {value:.3}"));
    }
}

fn check_optional_maximum(
    failures: &mut Vec<String>,
    name: &str,
    value: Option<f64>,
    maximum: f64,
) {
    match value {
        Some(value) => check_maximum(failures, name, value, maximum),
        None => failures.push(format!("{name} could not be measured")),
    }
}

fn invalid_data(message: impl Into<String>) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEFORE: &str = r#"
# HELP ignored ignored
hubuum_process_start_time_seconds 100
hubuum_db_connection_acquire_duration_seconds_count{caller="task_worker"} 20
hubuum_db_connection_acquire_duration_seconds_count{caller="event_fanout"} 10
hubuum_db_connection_acquire_duration_seconds_count{caller="restore_coordinator"} 50
hubuum_db_connection_acquire_duration_seconds_count{caller="metrics_refresh"} 3
hubuum_task_worker_iterations_total{outcome="idle"} 10
hubuum_event_worker_wakeups_total{kind="poll",worker="fanout"} 10
"#;
    const AFTER: &str = r#"
hubuum_process_start_time_seconds 100
hubuum_db_connection_acquire_duration_seconds_count{caller="task_worker"} 40
hubuum_db_connection_acquire_duration_seconds_count{caller="event_fanout"} 20
hubuum_db_connection_acquire_duration_seconds_count{caller="restore_coordinator"} 100
hubuum_db_connection_acquire_duration_seconds_count{caller="metrics_refresh"} 6
hubuum_task_worker_iterations_total{outcome="idle"} 20
hubuum_event_worker_wakeups_total{kind="poll",worker="fanout"} 20
"#;

    #[test]
    fn prometheus_parser_handles_label_order_and_escapes() {
        let snapshot =
            MetricSnapshot::parse(r#"metric_total{second="quoted\"value",first="line\nvalue"} 7"#)
                .unwrap();

        assert_eq!(
            snapshot.value(
                "metric_total",
                &[("first", "line\nvalue"), ("second", "quoted\"value")]
            ),
            7.0
        );
    }

    #[test]
    fn idle_report_excludes_scrape_refresh_acquisitions() {
        let before = MetricSnapshot::parse(BEFORE).unwrap();
        let after = MetricSnapshot::parse(AFTER).unwrap();

        let report = ProcessIdleReport::from_snapshots("all", 50.0, &before, &after).unwrap();

        assert_eq!(report.background_db_acquisitions_per_second(), 1.6);
        assert_eq!(report.task_idle_iterations_per_second(), 0.2);
        assert_eq!(report.fanout_poll_wakeups_per_second(), 0.2);
        assert_eq!(
            report.task_worker_acquisitions_per_idle_iteration(),
            Some(2.0)
        );
        assert_eq!(report.fanout_acquisitions_per_poll_wakeup(), Some(1.0));
    }

    #[test]
    fn counter_reset_is_rejected() {
        let before = MetricSnapshot::parse("counter_total 2").unwrap();
        let after = MetricSnapshot::parse("counter_total 1").unwrap();

        assert!(before.counter_delta(&after, "counter_total", &[]).is_err());
    }

    #[test]
    fn assessment_rejects_non_exact_readiness_checkout_count() {
        let mut report = passing_report("head");
        report.primary_readiness = ReadinessReport::new(2, 2, 3.0, &[1.0, 1.0]).unwrap();
        let assessment = RuntimeBehaviorAssessment::assess(&report, None, permissive_budgets());

        assert!(assessment.ensure_passed().is_err());
        assert!(assessment.markdown().contains("primary readiness used 3"));
    }

    #[test]
    fn assessment_rejects_relative_rate_regression() {
        let base = passing_report("base");
        let mut head = passing_report("head");
        head.primary.background_db_acquisitions_per_second *= 1.5;
        head.aggregate_background_db_acquisitions_per_second =
            head.primary.background_db_acquisitions_per_second
                + head.standby.background_db_acquisitions_per_second;

        let mut budgets = permissive_budgets();
        budgets.max_relative_regression_percent = 25.0;
        let assessment = RuntimeBehaviorAssessment::assess(&head, Some(&base), budgets);

        assert!(assessment.ensure_passed().is_err());
        assert!(assessment.markdown().contains("regressed by more than"));
    }

    fn passing_report(label: &str) -> RuntimeBehaviorReport {
        let primary = ProcessIdleReport::from_snapshots(
            "all",
            50.0,
            &MetricSnapshot::parse(BEFORE).unwrap(),
            &MetricSnapshot::parse(AFTER).unwrap(),
        )
        .unwrap();
        let standby = ProcessIdleReport::from_snapshots(
            "api",
            50.0,
            &MetricSnapshot::parse(BEFORE).unwrap(),
            &MetricSnapshot::parse(AFTER).unwrap(),
        )
        .unwrap();
        RuntimeBehaviorReport::new(
            label,
            primary,
            standby,
            ReadinessReport::new(2, 2, 2.0, &[1.0, 1.0]).unwrap(),
            ReadinessReport::new(2, 2, 2.0, &[1.0, 1.0]).unwrap(),
            TaskNotificationReport::new(10.0, 10.0, "failed"),
        )
    }

    fn permissive_budgets() -> RuntimeBehaviorBudgets {
        RuntimeBehaviorBudgets {
            max_primary_background_db_per_second: 100.0,
            max_standby_background_db_per_second: 100.0,
            max_aggregate_background_db_per_second: 100.0,
            max_task_idle_per_second: 100.0,
            max_fanout_poll_per_second: 100.0,
            max_restore_db_per_second: 100.0,
            max_task_worker_db_per_idle_iteration: 100.0,
            max_fanout_db_per_poll_wakeup: 100.0,
            max_notification_latency_ms: 100.0,
            max_task_claim_latency_ms: 100.0,
            max_relative_regression_percent: 100.0,
        }
    }
}
