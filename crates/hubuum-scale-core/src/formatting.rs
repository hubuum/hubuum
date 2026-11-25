pub(super) fn format_bytes(value: f64) -> String {
    format!("{:.1} MiB", value / 1_048_576.0)
}

pub(super) fn format_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:+.1}%"))
        .unwrap_or_else(|| "-".to_string())
}

pub(super) fn short_report_label(label: &str) -> String {
    label.chars().take(12).collect()
}

pub(super) fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index != 0 && (digits.len() - index).is_multiple_of(3) {
            output.push(',');
        }
        output.push(character);
    }
    output
}

pub(super) fn format_saturation(basis_points: u64) -> String {
    if basis_points < 100 {
        format!("{:.2}%", basis_points as f64 / 100.0)
    } else {
        format!("{:.1}%", basis_points as f64 / 100.0)
    }
}
