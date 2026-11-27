use std::error::Error;
use std::io;
use std::path::Path;

use rappct::acl::{AccessMask, ResourcePath, grant_to_package};
use rappct::{
    AppContainerProfile, JobLimits, LaunchOptions, SecurityCapabilitiesBuilder, StdioConfig,
    derive_sid_from_name, launch_in_container_with_io,
};

use super::Worker;

const PROFILE: &str = "Hubuum.TemplateWorker";

/// An operator installing a read-only binary can provision its read/execute ACE once.
pub(crate) fn prepare() -> Result<(), Box<dyn Error>> {
    let sid = derive_sid_from_name(PROFILE)?;
    const FILE_EXECUTE: u32 = 0x20;
    grant_to_package(
        ResourcePath::File(std::env::current_exe()?),
        &sid,
        AccessMask(AccessMask::FILE_GENERIC_READ.0 | FILE_EXECUTE),
    )?;
    Ok(())
}

pub(crate) fn enter() -> Result<Worker, Box<dyn Error>> {
    let token = rappct::token::query_current_process_token()?;
    if token.is_appcontainer {
        if token.is_lpac
            && token.capability_sids.is_empty()
            && token.package_sid == Some(derive_sid_from_name(PROFILE)?)
        {
            return Ok(Worker::Render);
        }
        return Err("unexpected template worker AppContainer token".into());
    }
    let profile = AppContainerProfile::ensure(PROFILE, "Hubuum template worker", None)?;
    let security = SecurityCapabilitiesBuilder::new(&profile.sid)
        .lpac(true)
        .build()?;
    let system_root =
        std::env::var_os("SystemRoot").ok_or("Windows system directory is missing")?;
    let options = LaunchOptions {
        exe: std::env::current_exe()?,
        cwd: Some(Path::new(&system_root).join("System32")),
        env: Some(vec![("SystemRoot".into(), system_root)]),
        stdio: StdioConfig::Pipe,
        join_job: Some(JobLimits {
            kill_on_job_close: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut child = match launch_in_container_with_io(&security, &options) {
        Err(rappct::AcError::LaunchFailed {
            stage: "CreateProcessW",
            ref source,
            ..
        }) if source
            .downcast_ref::<io::Error>()
            .is_some_and(|e| e.kind() == io::ErrorKind::PermissionDenied) =>
        {
            prepare()?;
            launch_in_container_with_io(&security, &options)?
        }
        result => result?,
    };
    // Only forward bytes once the OS has placed the child into the kill-on-close
    // job. Cancelling the broker also ends the rendering process.
    let mut input = child.stdin.take().ok_or("missing isolated input pipe")?;
    io::copy(&mut io::stdin().lock(), &mut input)?;
    drop(input);
    let mut output = child.stdout.take().ok_or("missing isolated output pipe")?;
    io::copy(&mut output, &mut io::stdout().lock())?;
    if child.wait(None)? != 0 {
        return Err("isolated template worker failed".into());
    }
    Ok(Worker::BrokerFinished)
}
