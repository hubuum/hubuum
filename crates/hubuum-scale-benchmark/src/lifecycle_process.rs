//! Bounded execution of the real administrator binary during scale drills.
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use hubuum_scale_core::LifecycleStageResources;
use sysinfo::{Pid, ProcessesToUpdate, System};

const MAX_DIAGNOSTIC_BYTES: u64 = 1024 * 1024;

pub struct ProcessLimits {
    resident_bytes: u64,
    timeout: Duration,
}

impl ProcessLimits {
    pub fn new(resident_bytes: u64, timeout_seconds: u64) -> io::Result<Self> {
        if resident_bytes == 0 || timeout_seconds == 0 {
            return Err(io::Error::other(
                "lifecycle resource limits must be positive",
            ));
        }
        Ok(Self {
            resident_bytes,
            timeout: Duration::from_secs(timeout_seconds),
        })
    }
}

struct ManagedChild {
    child: Child,
    running: bool,
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if !self.running {
            return;
        }
        // The administrator may own isolated template children. A separate
        // process group prevents cancellation from leaving them behind on Unix.
        #[cfg(unix)]
        unsafe {
            libc::killpg(self.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Captures<'a> {
    directory: &'a Path,
}

impl Drop for Captures<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.directory.join("stdout"));
        let _ = fs::remove_file(self.directory.join("stderr"));
        let _ = fs::remove_dir(self.directory);
    }
}

pub fn run(
    command: &mut Command,
    directory: &Path,
    limits: &ProcessLimits,
) -> (io::Result<Output>, LifecycleStageResources) {
    let started = Instant::now();
    let mut resources = LifecycleStageResources::default();
    let result = run_inner(command, directory, limits, started, &mut resources);
    resources.elapsed_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    (result, resources)
}

fn run_inner(
    command: &mut Command,
    directory: &Path,
    limits: &ProcessLimits,
    started: Instant,
    resources: &mut LifecycleStageResources,
) -> io::Result<Output> {
    // Refuse to overwrite or delete artifacts owned by an earlier run.
    fs::create_dir(directory)?;
    let _captures = Captures { directory };
    let stdout = directory.join("stdout");
    let stderr = directory.join("stderr");
    command.stdout(Stdio::from(File::create_new(&stdout)?));
    command.stderr(Stdio::from(File::create_new(&stderr)?));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = ManagedChild {
        child: command.spawn()?,
        running: true,
    };
    let root = Pid::from_u32(child.child.id());
    let mut system = System::new();
    loop {
        system.refresh_processes(ProcessesToUpdate::All, true);
        let mut descendants = vec![root];
        let mut index = 0;
        while index < descendants.len() {
            let parent = descendants[index];
            descendants.extend(
                system.processes().iter().filter_map(|(pid, process)| {
                    (process.parent() == Some(parent)).then_some(*pid)
                }),
            );
            index += 1;
        }
        let resident = descendants
            .iter()
            .filter_map(|pid| system.process(*pid))
            .fold(0_u64, |bytes, process| {
                bytes.saturating_add(process.memory())
            });
        resources.peak_resident_bytes = resources.peak_resident_bytes.max(resident);
        let violation = if resident > limits.resident_bytes {
            Some("resident_memory_limit")
        } else if started.elapsed() >= limits.timeout {
            Some("deadline")
        } else if fs::metadata(&stdout)?.len() > MAX_DIAGNOSTIC_BYTES
            || fs::metadata(&stderr)?.len() > MAX_DIAGNOSTIC_BYTES
        {
            Some("diagnostic_size_limit")
        } else {
            None
        };
        if let Some(reason) = violation {
            resources.termination = Some(reason.to_string());
            return Err(io::Error::other(format!(
                "lifecycle process stopped: {reason}"
            )));
        }
        if let Some(status) = child.child.try_wait()? {
            // The PID has been reaped; do not address its process group again.
            child.running = false;
            let read = |path| -> io::Result<Vec<u8>> {
                let mut bytes = Vec::new();
                File::open(path)?
                    .take(MAX_DIAGNOSTIC_BYTES + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_DIAGNOSTIC_BYTES {
                    return Err(io::Error::other("lifecycle diagnostic limit exceeded"));
                }
                Ok(bytes)
            };
            return Ok(Output {
                status,
                stdout: read(stdout)?,
                stderr: read(stderr)?,
            });
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn directory() -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "hubuum-lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn captures_successful_output_and_cleans_temporary_files() {
        let path = directory();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf verified"]);
        let (output, _) = run(
            &mut command,
            &path,
            &ProcessLimits::new(128 * 1024 * 1024, 10).unwrap(),
        );
        assert_eq!(output.unwrap().stdout, b"verified");
        assert!(!path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn deadline_terminates_the_process_group() {
        let path = directory();
        let pid_file = path.with_extension("pid");
        let mut command = Command::new("/bin/sh");
        command.env("CHILD_PID_FILE", &pid_file);
        command.args([
            "-c",
            "sleep 30 & printf '%s' \"$!\" > \"$CHILD_PID_FILE\"; wait",
        ]);
        let started = Instant::now();
        let (output, usage) = run(
            &mut command,
            &path,
            &ProcessLimits::new(128 * 1024 * 1024, 1).unwrap(),
        );
        assert!(output.is_err());
        assert_eq!(usage.termination.as_deref(), Some("deadline"));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(!path.exists());
        let pid = fs::read_to_string(&pid_file).unwrap();
        fs::remove_file(pid_file).unwrap();
        let child_state = fs::read_to_string(format!("/proc/{pid}/stat"));
        assert!(
            child_state.is_err() || child_state.unwrap().split_whitespace().nth(2) == Some("Z"),
            "the administrator's descendant must also be terminated"
        );
    }

    #[test]
    fn rejects_excess_resident_memory() {
        let path = directory();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 30"]);
        let (output, usage) = run(&mut command, &path, &ProcessLimits::new(1, 10).unwrap());
        assert!(output.is_err());
        assert_eq!(usage.termination.as_deref(), Some("resident_memory_limit"));
    }

    #[test]
    fn bounds_diagnostic_capture() {
        let path = directory();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "head -c 2097152 /dev/zero; sleep 30"]);
        let (output, usage) = run(
            &mut command,
            &path,
            &ProcessLimits::new(128 * 1024 * 1024, 10).unwrap(),
        );
        assert!(output.is_err());
        assert_eq!(usage.termination.as_deref(), Some("diagnostic_size_limit"));
    }
}
