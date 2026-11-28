//! One bounded producer can run while the observer records real expiry clocks.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::thread::{self, JoinHandle};

use hubuum_domain::TaskIdentity;
use hubuum_scale_core::LifecycleStageResources;
use serde_json::{Value, json};

use super::ObserveArgs;
use crate::lifecycle_process::{self, ProcessLimits};
use crate::{Result, write_json};

type ProcessResult = (io::Result<Output>, LifecycleStageResources);

pub(super) struct PendingRefill {
    expected: usize,
    output: PathBuf,
    resources: PathBuf,
    worker: Option<JoinHandle<ProcessResult>>,
}

impl PendingRefill {
    pub(super) fn start(options: &ObserveArgs, pass: usize, count: usize) -> Result<Self> {
        let mut recipe: Value = serde_json::from_slice(&fs::read(
            options
                .refill_workload
                .as_ref()
                .ok_or("Missing refill recipe")?,
        )?)?;
        recipe["count"] = json!(count);
        let recipe_path = options
            .artifact_directory
            .join(format!("refill-{pass}-recipe.json"));
        let output = options
            .artifact_directory
            .join(format!("refill-{pass}.json"));
        write_json(&recipe_path, &recipe)?;
        let metric_directory = options
            .artifact_directory
            .join(format!("refill-{pass}-metrics"));
        fs::create_dir(&metric_directory)?;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["task-retention", "--origins"])
            .args(&options.refill_origins)
            .arg("--database-url")
            .arg(&options.database_url)
            .arg("--token-file")
            .arg(
                options
                    .refill_token_file
                    .as_ref()
                    .ok_or("Missing refill credentials")?,
            )
            .arg("--workload-spec")
            .arg(&recipe_path)
            .arg("--cohort")
            .arg(format!("refill-{}-{pass}", std::process::id()))
            .arg("--output")
            .arg(&output)
            .arg("--metric-directory")
            .arg(metric_directory);
        let process_directory = options
            .artifact_directory
            .join(format!("refill-{pass}-process"));
        let limits = ProcessLimits::new(512 * 1024 * 1024, 120)?;
        let worker = thread::Builder::new()
            .name("retention-refill".into())
            .spawn(move || lifecycle_process::run(&mut command, &process_directory, &limits))?;
        Ok(Self {
            expected: count,
            output,
            resources: options
                .artifact_directory
                .join(format!("refill-{pass}-resources.json")),
            worker: Some(worker),
        })
    }

    pub(super) fn is_finished(&self) -> bool {
        self.worker.as_ref().is_some_and(JoinHandle::is_finished)
    }

    pub(super) fn finish(mut self) -> Result<Vec<TaskIdentity>> {
        let (result, resources) = self
            .worker
            .take()
            .ok_or("Refill was already joined")?
            .join()
            .map_err(|_| "Refill process monitor panicked")?;
        write_json(
            &self.resources,
            &json!({"process":resources,
            "result":result.as_ref().map(|output|json!({
                "status":output.status.to_string(),
                "stderr":String::from_utf8_lossy(&output.stderr),
            })).map_err(ToString::to_string)}),
        )?;
        if !result?.status.success() {
            return Err("Production steady refill failed; inspect refill artifacts".into());
        }
        admitted_identities(
            &serde_json::from_slice(&fs::read(&self.output)?)?,
            self.expected,
        )
    }
}

impl Drop for PendingRefill {
    fn drop(&mut self) {
        // The existing process monitor enforces its 120-second deadline and
        // reaps the complete child group. Even an observer error must finish
        // that cleanup before the fixture can disappear.
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn admitted_identities(report: &Value, expected: usize) -> Result<Vec<TaskIdentity>> {
    let rows = report
        .get("admissions")
        .and_then(Value::as_array)
        .ok_or("Missing refill admissions")?;
    if rows.len() != expected {
        return Err("Refill admission count differs from requested bounded work".into());
    }
    rows.iter()
        .map(|row| {
            serde_json::from_value(
                row.get("task_identity")
                    .cloned()
                    .ok_or("Missing refill task UUID")?,
            )
            .map_err(Into::into)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::complete(1, true, true)]
    #[case::wrong_count(2, true, false)]
    #[case::invalid_identity(1, false, false)]
    fn a_refill_report_must_account_for_every_requested_identity(
        #[case] expected: usize,
        #[case] valid_identity: bool,
        #[case] accepted: bool,
    ) {
        let identity = if valid_identity {
            json!(TaskIdentity::generate())
        } else {
            json!("invalid")
        };
        let report = json!({"admissions":[{"task_identity":identity}]});
        assert_eq!(admitted_identities(&report, expected).is_ok(), accepted);
    }
}
