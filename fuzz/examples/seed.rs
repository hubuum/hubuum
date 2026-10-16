//! Generate synthetic seeds from the language-owned corpus and exact limits.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use hubuum_regex::{MAX_NESTING_DEPTH, MAX_PATTERN_BYTES, MAX_REPETITION, conformance};

fn write_case(root: &Path, name: &str, pattern: &str, text: &str) -> std::io::Result<()> {
    fs::write(root.join("regex_pattern").join(name), pattern)?;
    fs::write(
        root.join("regex_backends").join(name),
        format!("{pattern}\0{text}"),
    )
}

fn main() -> std::io::Result<()> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest.join("corpus"));
    for target in ["regex_pattern", "regex_backends"] {
        fs::create_dir_all(root.join(target))?;
    }
    for (index, case) in conformance::cases().iter().enumerate() {
        write_case(
            &root,
            &format!("contract-{index}"),
            case.pattern(),
            case.text(),
        )?;
    }
    for (index, pattern) in conformance::invalid_patterns().iter().enumerate() {
        write_case(&root, &format!("invalid-{index}"), pattern, "")?;
    }
    let boundaries = [
        "a".repeat(MAX_PATTERN_BYTES),
        "a".repeat(MAX_PATTERN_BYTES + 1),
        "β".repeat(MAX_PATTERN_BYTES / 2),
        "β".repeat(MAX_PATTERN_BYTES / 2 + 1),
        format!(
            "{}a{}",
            "(".repeat(MAX_NESTING_DEPTH as usize),
            ")".repeat(MAX_NESTING_DEPTH as usize)
        ),
        format!(
            "{}a{}",
            "(".repeat(MAX_NESTING_DEPTH as usize + 1),
            ")".repeat(MAX_NESTING_DEPTH as usize + 1)
        ),
        format!("a{{{MAX_REPETITION}}}"),
        format!("a{{{}}}", MAX_REPETITION + 1),
        format!("(?:a{{254}}){{16}}{}", "a".repeat(14)),
        format!("(?:a{{254}}){{16}}{}", "a".repeat(15)),
    ];
    for (index, pattern) in boundaries.iter().enumerate() {
        write_case(&root, &format!("boundary-{index}"), pattern, "a\nβ\r\n💡")?;
    }
    for target in ["regex_pattern", "regex_backends"] {
        let regressions = manifest.join("regressions").join(target);
        if regressions.is_dir() {
            for entry in fs::read_dir(regressions)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    let mut name = OsString::from("regression-");
                    name.push(entry.file_name());
                    fs::copy(entry.path(), root.join(target).join(name))?;
                }
            }
        }
    }
    Ok(())
}
