use hubuum_regex::{MAX_NESTING_DEPTH, MAX_PATTERN_BYTES, MAX_REPETITION, Pattern};
use rstest::rstest;

#[rstest]
#[case::ascii_exact("a".repeat(MAX_PATTERN_BYTES), true)]
#[case::ascii_over("a".repeat(MAX_PATTERN_BYTES + 1), false)]
#[case::unicode_exact("β".repeat(MAX_PATTERN_BYTES / 2), true)]
#[case::unicode_over("β".repeat(MAX_PATTERN_BYTES / 2 + 1), false)]
#[case::nesting_exact(format!("{}a{}", "(".repeat(MAX_NESTING_DEPTH as usize), ")".repeat(MAX_NESTING_DEPTH as usize)), true)]
#[case::nesting_over(format!("{}a{}", "(".repeat(MAX_NESTING_DEPTH as usize + 1), ")".repeat(MAX_NESTING_DEPTH as usize + 1)), false)]
#[case::repetition_exact(format!("a{{{MAX_REPETITION}}}"), true)]
#[case::repetition_over(format!("a{{{}}}", MAX_REPETITION + 1), false)]
#[case::complexity_exact(format!("(?:a{{254}}){{16}}{}", "a".repeat(14)), true)]
#[case::complexity_over(format!("(?:a{{254}}){{16}}{}", "a".repeat(15)), false)]
fn acceptance_boundaries(#[case] source: String, #[case] accepted: bool) {
    assert_eq!(Pattern::new(&source).is_ok(), accepted, "{source}");
}
