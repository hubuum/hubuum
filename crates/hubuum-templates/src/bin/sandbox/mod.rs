//! Enter the worker's OS protection boundary before reading its protocol.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(super) use linux::enter;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::{enter, prepare};

pub(super) enum Worker {
    Render,
    #[cfg(windows)]
    BrokerFinished,
}

#[cfg(target_os = "macos")]
pub(super) fn enter() -> Result<Worker, Box<dyn std::error::Error>> {
    use std::ffi::{CStr, c_char};
    use std::ptr;

    #[link(name = "sandbox")]
    unsafe extern "C" {
        fn sandbox_init(profile: *const c_char, flags: u64, error: *mut *mut c_char) -> i32;
        fn sandbox_free_error(error: *mut c_char);
    }

    // The dynamic loader has finished. Rendering needs only inherited pipes,
    // anonymous memory and clocks. The profile grants no path or network access.
    let profile = c"(version 1)(deny default)(allow sysctl-read)";
    let mut error = ptr::null_mut();
    // SAFETY: the profile is NUL-terminated and the API initializes the error pointer.
    if unsafe { sandbox_init(profile.as_ptr(), 0, &mut error) } != 0 {
        if error.is_null() {
            return Err("could not initialize the template worker sandbox".into());
        }
        // SAFETY: sandbox_init returned an owned NUL-terminated diagnostic.
        let message = unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned();
        unsafe { sandbox_free_error(error) };
        return Err(message.into());
    }
    Ok(Worker::Render)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn enter() -> Result<Worker, Box<dyn std::error::Error>> {
    Err("template worker isolation is unavailable on this platform".into())
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::{fs, net::TcpListener, process::Command};

    #[test]
    fn worker_policy_denies_private_files_and_network() {
        const PROBE: &str = "HUBUUM_TEST_WORKER_SANDBOX_FILE";
        if let Some(path) = std::env::var_os(PROBE) {
            super::enter().expect("install worker policy");
            let file_denied = fs::File::open(path)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
            let net_denied = TcpListener::bind(("127.0.0.1", 0))
                .is_err_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
            std::process::exit(i32::from(!(file_denied && net_denied)));
        }
        let fixture = tempfile::NamedTempFile::new().unwrap();
        assert!(fs::File::open(fixture.path()).is_ok());
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sandbox::tests::worker_policy_denies_private_files_and_network",
            ])
            .env(PROBE, fixture.path())
            .status()
            .unwrap();
        assert!(status.success());
    }
}
