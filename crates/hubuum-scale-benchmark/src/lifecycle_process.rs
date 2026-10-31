//! Bounded execution of the real administrator binary during scale drills.
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use hubuum_scale_core::LifecycleStageResources;
#[cfg(unix)]
use libc::{P_PID, SIGKILL, WEXITED, WNOHANG, WNOWAIT, id_t, killpg, siginfo_t, waitid};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const MAX_DIAGNOSTIC_BYTES: u64 = 1024 * 1024;

/// The private administrator refusal is retryable without weakening protection.
/// Other unavailable outcomes can require recovery or a different key.
pub fn control_repository_busy(stderr: &[u8]) -> bool {
    std::str::from_utf8(stderr).is_ok_and(|text| {
        text.lines().any(|line| {
            line == "Error: ServiceUnavailable(\"Control repository is busy; retry without bypassing protection\")"
        })
    })
}

#[cfg(target_os = "linux")]
fn cpu_mask(value: &str) -> io::Result<std::collections::BTreeSet<usize>> {
    let mut result = std::collections::BTreeSet::new();
    for segment in value.trim().split(',') {
        let (first, last) = segment.split_once('-').unwrap_or((segment, segment));
        let parse = |value: &str| value.parse::<usize>().map_err(io::Error::other);
        let (first, last) = (parse(first)?, parse(last)?);
        if first > last || last > 65536 {
            return Err(io::Error::other("Invalid measured CPU affinity mask"));
        }
        result.extend(first..=last);
    }
    Ok(result)
}

#[cfg(target_os = "linux")]
fn process_disappeared(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

#[cfg(target_os = "linux")]
fn verify_cpu_affinity(pids: &[Pid], expected: &str) -> io::Result<()> {
    let expected = cpu_mask(expected)?;
    for pid in pids {
        let tasks = match fs::read_dir(format!("/proc/{pid}/task")) {
            Ok(tasks) => tasks,
            Err(error) if process_disappeared(&error) => continue,
            Err(error) => return Err(error),
        };
        for task in tasks {
            let task = match task {
                Ok(task) => task,
                Err(error) if process_disappeared(&error) => continue,
                Err(error) => return Err(error),
            };
            let status = match fs::read_to_string(task.path().join("status")) {
                Ok(status) => status,
                Err(error) if process_disappeared(&error) => continue,
                Err(error) => return Err(error),
            };
            let mask = status
                .lines()
                .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
                .ok_or_else(|| io::Error::other("Missing kernel CPU affinity"))?;
            if !cpu_mask(mask)?.is_subset(&expected) {
                return Err(io::Error::other("Measured process escaped CPU affinity"));
            }
        }
    }
    Ok(())
}

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
    kernel_peak_resident_bytes: Option<u64>,
}

impl ManagedChild {
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        #[cfg(unix)]
        {
            use std::mem::MaybeUninit;

            // Observe exit without reaping the leader. Its PID must remain
            // reserved until the whole process group has been terminated.
            let mut info = MaybeUninit::<siginfo_t>::zeroed();
            let result = unsafe {
                waitid(
                    P_PID,
                    self.child.id() as id_t,
                    info.as_mut_ptr(),
                    WEXITED | WNOHANG | WNOWAIT,
                )
            };
            if result == -1 {
                return Err(io::Error::last_os_error());
            }
            if unsafe { info.assume_init().si_pid() } == 0 {
                return Ok(None);
            }
            // The administrator may exit before its descendants, including on
            // ordinary failures. Clean them up while the group ID is pinned.
            unsafe {
                killpg(self.child.id() as i32, SIGKILL);
            }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        let status = Some(self.child.wait()?);
        #[cfg(target_os = "linux")]
        let status = {
            use std::mem::MaybeUninit;
            use std::os::unix::process::ExitStatusExt;

            let mut status = 0;
            let mut usage = MaybeUninit::<libc::rusage>::zeroed();
            let waited =
                unsafe { libc::wait4(self.child.id() as i32, &mut status, 0, usage.as_mut_ptr()) };
            if waited == -1 {
                return Err(io::Error::last_os_error());
            }
            self.kernel_peak_resident_bytes =
                u64::try_from(unsafe { usage.assume_init() }.ru_maxrss)
                    .ok()
                    .map(|kilobytes| kilobytes.saturating_mul(1024));
            Some(ExitStatus::from_raw(status))
        };
        #[cfg(not(unix))]
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.running = false;
        }
        Ok(status)
    }
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
            killpg(self.child.id() as i32, SIGKILL);
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
    let mut resources = LifecycleStageResources {
        cpu_affinity: std::env::var("HUBUUM_MEASUREMENT_CPU_AFFINITY").ok(),
        ..Default::default()
    };
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
        kernel_peak_resident_bytes: None,
    };
    let root = Pid::from_u32(child.child.id());
    let mut system = System::new();
    loop {
        // Linux tasks share their process's address space. Listing them as
        // descendants counts the same RSS once per thread (including Tokio's
        // worker threads), causing false memory-limit terminations.
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_memory().without_tasks(),
        );
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
        #[cfg(target_os = "linux")]
        if let Some(mask) = &resources.cpu_affinity {
            verify_cpu_affinity(&descendants, mask)?;
            resources.affinity_verification_samples += 1;
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
        if let Some(status) = child.try_wait()? {
            resources.kernel_peak_resident_bytes = child.kernel_peak_resident_bytes;
            if resources
                .kernel_peak_resident_bytes
                .is_some_and(|peak| peak > limits.resident_bytes)
            {
                resources.termination = Some("resident_memory_limit".to_string());
                return Err(io::Error::other(
                    "lifecycle process exceeded kernel memory high-water limit",
                ));
            }
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
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use rstest::rstest;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn directory() -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "hubuum-lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[cfg(target_os = "linux")]
    fn assert_descendant_terminated(pid: &str) {
        let started = Instant::now();
        loop {
            let state = fs::read_to_string(format!("/proc/{pid}/stat"));
            if state.is_err() || state.unwrap().split_whitespace().nth(2) == Some("Z") {
                return;
            }
            if started.elapsed() >= Duration::from_secs(2) {
                // Clean up even if this assertion catches a regression.
                unsafe {
                    libc::kill(pid.parse().unwrap(), libc::SIGKILL);
                }
                panic!("administrator exit must not leave a running descendant");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(target_os = "linux")]
    #[rstest]
    #[case::missing_process(libc::ENOENT, true)]
    #[case::exited_thread(libc::ESRCH, true)]
    #[case::permission_denied(libc::EACCES, false)]
    #[case::operation_denied(libc::EPERM, false)]
    #[case::io_failure(libc::EIO, false)]
    fn affinity_sampling_only_tolerates_disappearing_processes(
        #[case] code: i32,
        #[case] disappeared: bool,
    ) {
        assert_eq!(
            process_disappeared(&io::Error::from_raw_os_error(code)),
            disappeared
        );
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
        assert_descendant_terminated(&pid);
    }

    #[cfg(target_os = "linux")]
    #[rstest]
    #[case::success(0)]
    #[case::failure(1)]
    fn administrator_exit_terminates_remaining_descendants(#[case] exit_code: i32) {
        let path = directory();
        let pid_file = path.with_extension("pid");
        let mut command = Command::new("/bin/sh");
        command.env("CHILD_PID_FILE", &pid_file);
        command.env("EXIT_CODE", exit_code.to_string());
        command.args([
            "-c",
            "sleep 30 & printf '%s' \"$!\" > \"$CHILD_PID_FILE\"; exit \"$EXIT_CODE\"",
        ]);
        let (output, _) = run(
            &mut command,
            &path,
            &ProcessLimits::new(128 * 1024 * 1024, 10).unwrap(),
        );
        assert_eq!(output.unwrap().status.code(), Some(exit_code));
        let pid = fs::read_to_string(&pid_file).unwrap();
        fs::remove_file(pid_file).unwrap();
        assert_descendant_terminated(&pid);
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

    #[cfg(target_os = "linux")]
    #[test]
    fn shared_thread_memory_is_counted_once() {
        const FIXTURE: &str = "LIFECYCLE_TEST_THREAD_MEMORY_FIXTURE";
        if std::env::var_os(FIXTURE).is_some() {
            let allocation = vec![1_u8; 64 * 1024 * 1024];
            let threads = (0..8)
                .map(|_| thread::spawn(|| thread::sleep(Duration::from_millis(750))))
                .collect::<Vec<_>>();
            for thread in threads {
                thread.join().unwrap();
            }
            std::hint::black_box(allocation);
            return;
        }
        let path = directory();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "lifecycle_process::tests::shared_thread_memory_is_counted_once",
            ])
            .env(FIXTURE, "1");
        let (output, usage) = run(
            &mut command,
            &path,
            &ProcessLimits::new(192 * 1024 * 1024, 10).unwrap(),
        );
        assert!(output.unwrap().status.success(), "{usage:?}");
        assert!(usage.peak_resident_bytes >= 64 * 1024 * 1024, "{usage:?}");
        assert!(usage.peak_resident_bytes < 192 * 1024 * 1024, "{usage:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn separate_child_memory_is_counted() {
        const FIXTURE: &str = "LIFECYCLE_TEST_CHILD_MEMORY_FIXTURE";
        const TEST: &str = "lifecycle_process::tests::separate_child_memory_is_counted";
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", TEST]);
        match std::env::var(FIXTURE).as_deref() {
            Ok("allocate") => {
                let allocation = vec![1_u8; 64 * 1024 * 1024];
                thread::sleep(Duration::from_secs(5));
                std::hint::black_box(allocation);
                return;
            }
            Ok("spawn") => {
                let mut child = command.env(FIXTURE, "allocate").spawn().unwrap();
                assert!(child.wait().unwrap().success());
                return;
            }
            _ => {}
        }
        let path = directory();
        let (output, usage) = run(
            command.env(FIXTURE, "spawn"),
            &path,
            &ProcessLimits::new(48 * 1024 * 1024, 10).unwrap(),
        );
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
