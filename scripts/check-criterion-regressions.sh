#!/usr/bin/env bash

set -euo pipefail

criterion_dir="${1:-target/criterion}"
warning_threshold_pct="${2:-10}"
failure_threshold_pct="${3:-20}"
absolute_failure_threshold_ns="${4:-0}"
direction="${5:-forward}"
annotation_level="${6:-error}"
failure_output="${7:-}"
failure_filter="${8:-}"
baseline_name="${9:-base}"

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to evaluate Criterion comparison results" >&2
    exit 2
fi

case "$direction" in
    forward | reverse) ;;
    *)
        echo "direction must be 'forward' or 'reverse', got '$direction'" >&2
        exit 2
        ;;
esac

case "$annotation_level" in
    error | warning | none) ;;
    *)
        echo "annotation level must be 'error', 'warning', or 'none', got '$annotation_level'" >&2
        exit 2
        ;;
esac

if [[ -z "$baseline_name" || "$baseline_name" == */* ]]; then
    echo "Criterion baseline name must be a non-empty directory name, got '$baseline_name'" >&2
    exit 2
fi

if [[ -n "$failure_output" ]]; then
    : > "$failure_output"
fi

benchmark_is_selected() {
    local benchmark="$1"
    local selected

    [[ -z "$failure_filter" ]] && return 0
    [[ -f "$failure_filter" ]] || return 1
    while IFS= read -r selected; do
        [[ "$selected" == "$benchmark" ]] && return 0
    done < "$failure_filter"
    return 1
}

annotate() {
    local title="$1"
    local message="$2"

    [[ "$annotation_level" == "none" ]] && return
    printf '::%s title=%s::%s\n' "$annotation_level" "$title" "$message"
}

result_count=0
failure_count=0

while IFS= read -r estimate_file; do
    benchmark_dir="${estimate_file%/change/estimates.json}"
    benchmark="${benchmark_dir#"$criterion_dir"/}"
    benchmark_is_selected "$benchmark" || continue

    base_estimate_file="$benchmark_dir/$baseline_name/estimates.json"
    new_estimate_file="$benchmark_dir/new/estimates.json"
    if [[ ! -f "$base_estimate_file" || ! -f "$new_estimate_file" ]]; then
        echo "Missing Criterion '$baseline_name'/new estimates for $benchmark" >&2
        exit 2
    fi

    result_count=$((result_count + 1))
    metrics="$({
        jq -nr \
            --arg direction "$direction" \
            --slurpfile change "$estimate_file" \
            --slurpfile base "$base_estimate_file" \
            --slurpfile new "$new_estimate_file" '
                if $direction == "forward" then
                    [
                        ($change[0].median.point_estimate * 100),
                        ($change[0].median.confidence_interval.lower_bound * 100),
                        ($new[0].median.point_estimate - $base[0].median.point_estimate)
                    ]
                else
                    [
                        ((1 / (1 + $change[0].median.point_estimate) - 1) * 100),
                        ((1 / (1 + $change[0].median.confidence_interval.upper_bound) - 1) * 100),
                        ($base[0].median.point_estimate - $new[0].median.point_estimate)
                    ]
                end
                | @tsv
            '
    })"
    IFS=$'\t' read -r median_pct lower_pct absolute_change_ns <<< "$metrics"

    credible_failure="$({
        jq -nr \
            --argjson lower_pct "$lower_pct" \
            --argjson failure_pct "$failure_threshold_pct" \
            --argjson absolute_ns "$absolute_change_ns" \
            --argjson absolute_threshold_ns "$absolute_failure_threshold_ns" \
            '($lower_pct > $failure_pct) and ($absolute_ns > $absolute_threshold_ns)'
    })"
    warning="$({
        jq -nr \
            --argjson median_pct "$median_pct" \
            --argjson warning_pct "$warning_threshold_pct" \
            '$median_pct > $warning_pct'
    })"

    printf '%s: median regression %.2f%% (95%% CI lower bound %.2f%%, absolute change %.0f ns)\n' \
        "$benchmark" "$median_pct" "$lower_pct" "$absolute_change_ns"

    if [[ "$credible_failure" == "true" ]]; then
        failure_count=$((failure_count + 1))
        if [[ -n "$failure_output" ]]; then
            printf '%s\n' "$benchmark" >> "$failure_output"
        fi
        annotate \
            "PostgreSQL benchmark regression" \
            "$benchmark credibly regressed by ${median_pct}% (${absolute_change_ns} ns)"
    elif [[ "$warning" == "true" ]]; then
        annotate \
            "PostgreSQL benchmark warning" \
            "$benchmark changed by ${median_pct}% (${absolute_change_ns} ns)"
    fi
done < <(find "$criterion_dir" -type f -path '*/change/estimates.json' | sort)

if [[ "$result_count" -eq 0 ]]; then
    echo "No selected Criterion base/head comparison results were found under $criterion_dir" >&2
    exit 2
fi

if [[ "$failure_count" -gt 0 ]]; then
    echo "$failure_count credible PostgreSQL benchmark regression(s) exceeded both the ${failure_threshold_pct}% and ${absolute_failure_threshold_ns} ns thresholds" >&2
    exit 1
fi

echo "PostgreSQL benchmark comparison passed ($result_count result(s))."
