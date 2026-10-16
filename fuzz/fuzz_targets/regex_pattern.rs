#![no_main]

use hubuum_regex::{MAX_PATTERN_BYTES, Pattern};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(source) = std::str::from_utf8(data) else {
        return;
    };
    let result = Pattern::new(source);
    assert_eq!(
        result,
        Pattern::new(source),
        "parsing must be deterministic"
    );
    assert!(source.len() <= MAX_PATTERN_BYTES || result.is_err());
    if let Err(error) = result {
        // Includes the parser's source excerpt and caret diagnostics. Oversized
        // sources must be rejected before such an excerpt is constructed.
        assert!(error.to_string().len() <= 64 * 1024);
    }
});
