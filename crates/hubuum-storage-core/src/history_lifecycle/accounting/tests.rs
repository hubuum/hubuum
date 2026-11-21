use rstest::rstest;

use super::*;

#[rstest]
#[case("1", 1)]
#[case("1.0000", 1)]
#[case("1e3", 4)]
#[case("1000.00", 4)]
#[case("1e-7", 9)]
#[case("-0.000", 1)]
#[case("-12.3400", 6)]
#[case("[ 1.00, 0.001e3, null, false ]", 16)]
fn logical_json_v1_normalizes_numbers(#[case] raw: &str, #[case] bytes: u64) {
    let value: Value = serde_json::from_str(raw).unwrap();
    assert_eq!(
        HistoryLogicalBytes::measure_json_v1(&value)
            .unwrap()
            .value(),
        bytes
    );
}

#[rstest]
#[case("plain UTF-8: æøå🦀")]
#[case("\0\u{0001}\u{0008}\t\n\u{000b}\u{000c}\r\"\\/")]
fn logical_json_v1_preserves_standard_string_escaping(#[case] value: &str) {
    let json = serde_json::json!({(value): [value]});
    assert_eq!(
        HistoryLogicalBytes::measure_json_v1(&json).unwrap().value(),
        serde_json::to_vec(&json).unwrap().len() as u64,
    );
}

#[test]
fn logical_json_v1_rejects_exponent_expansion_outside_the_byte_range() {
    let value: Value = serde_json::from_str("1e9223372036854775807").unwrap();
    assert!(HistoryLogicalBytes::measure_json_v1(&value).is_err());
}

#[rstest]
#[case(128, true)]
#[case(129, false)]
fn logical_json_v1_depth_is_bounded(#[case] depth: usize, #[case] accepted: bool) {
    let mut value = Value::Null;
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    assert_eq!(
        HistoryLogicalBytes::measure_json_v1(&value).is_ok(),
        accepted
    );
}
