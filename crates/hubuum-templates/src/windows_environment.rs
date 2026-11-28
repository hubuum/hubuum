use std::ffi::OsString;
use std::path::PathBuf;

/// The service and its broker each replace the entire Windows environment.
/// Give CreateProcessW the system and profile directories at both boundaries.
/// Application configuration and arbitrary executable search paths stay out.
pub fn windows_worker_environment() -> Vec<(OsString, OsString)> {
    let system32 = std::env::var_os("SystemRoot").map(|root| PathBuf::from(root).join("System32"));
    // Windows expects a Unicode environment block in case-insensitive name order.
    [
        "ALLUSERSPROFILE",
        "APPDATA",
        "ComSpec",
        "LOCALAPPDATA",
        "PATH",
        "PATHEXT",
        "ProgramData",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "PUBLIC",
        "SystemDrive",
        "SystemRoot",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "windir",
    ]
    .into_iter()
    .filter_map(|name| {
        let value = match name {
            "ComSpec" => system32
                .as_ref()
                .map(|directory| directory.join("cmd.exe").into_os_string()),
            "PATH" => system32
                .as_ref()
                .map(|directory| directory.as_os_str().to_owned()),
            "PATHEXT" => Some(OsString::from(".COM;.EXE;.BAT;.CMD")),
            _ => std::env::var_os(name),
        }?;
        Some((OsString::from(name), value))
    })
    .collect()
}
