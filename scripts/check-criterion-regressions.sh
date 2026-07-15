#!/usr/bin/env bash

set -euo pipefail

criterion_dir="${1:-target/criterion}"
warning_threshold_pct="${2:-10}"
failure_threshold_pct="${3:-20}"

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to evaluate Criterion comparison results" >&2
    exit 2
fi

result_count=0
failure_count=0

while IFS= read -r estimate_file; do
    result_count=$((result_count + 1))
    benchmark="${estimate_file#"$criterion_dir"/}"
    benchmark="${benchmark%/change/estimates.json}"
    median_pct="$(jq -r '.median.point_estimate * 100' "$estimate_file")"
    lower_pct="$(jq -r '.median.confidence_interval.lower_bound * 100' "$estimate_file")"
    credible_failure="$(
        jq -r --argjson threshold "$failure_threshold_pct" \
            '(.median.point_estimate * 100 > $threshold) and
             (.median.confidence_interval.lower_bound > 0)' \
            "$estimate_file"
    )"
    warning="$(
        jq -r --argjson threshold "$warning_threshold_pct" \
            '.median.point_estimate * 100 > $threshold' \
            "$estimate_file"
    )"

    printf '%s: median change %.2f%% (95%% CI lower bound %.2f%%)\n' \
        "$benchmark" "$median_pct" "$lower_pct"

    if [[ "$credible_failure" == "true" ]]; then
        failure_count=$((failure_count + 1))
        echo "::error title=PostgreSQL benchmark regression::$benchmark regressed by ${median_pct}%"
    elif [[ "$warning" == "true" ]]; then
        echo "::warning title=PostgreSQL benchmark warning::$benchmark changed by ${median_pct}%"
    fi
done < <(find "$criterion_dir" -type f -path '*/change/estimates.json' | sort)

if [[ "$result_count" -eq 0 ]]; then
    echo "No Criterion base/head comparison results were found under $criterion_dir" >&2
    exit 2
fi

if [[ "$failure_count" -gt 0 ]]; then
    echo "$failure_count credible PostgreSQL benchmark regression(s) exceeded ${failure_threshold_pct}%" >&2
    exit 1
fi

echo "PostgreSQL benchmark comparison passed ($result_count result(s))."
