#!/usr/bin/env bash

set -euo pipefail

initial_dir="${1:?initial Criterion directory is required}"
confirmation_dir="${2:?confirmation Criterion directory is required}"
stability_threshold_pct="${3:-10}"
benchmark_filter="${4:?benchmark filter is required}"

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to evaluate Criterion estimates" >&2
    exit 2
fi

if [[ ! -s "$benchmark_filter" ]]; then
    echo "The benchmark filter is empty: $benchmark_filter" >&2
    exit 2
fi

result_count=0
unstable_count=0

while IFS= read -r benchmark; do
    [[ -n "$benchmark" ]] || continue
    initial_estimate="$initial_dir/$benchmark/base/estimates.json"
    confirmation_estimate="$confirmation_dir/$benchmark/new/estimates.json"
    if [[ ! -f "$initial_estimate" || ! -f "$confirmation_estimate" ]]; then
        echo "Missing base-run stability estimates for $benchmark" >&2
        exit 2
    fi

    result_count=$((result_count + 1))
    drift_pct="$({
        jq -nr \
            --slurpfile initial "$initial_estimate" \
            --slurpfile confirmation "$confirmation_estimate" '
                (($confirmation[0].median.point_estimate
                    / $initial[0].median.point_estimate) - 1) * 100
            '
    })"
    absolute_drift_pct="$(
        jq -nr --argjson drift "$drift_pct" \
            'if $drift < 0 then -$drift else $drift end'
    )"

    printf '%s: base-to-base drift %+.2f%%\n' "$benchmark" "$drift_pct"
    if [[ "$(
        jq -nr \
            --argjson drift "$absolute_drift_pct" \
            --argjson threshold "$stability_threshold_pct" \
            '$drift > $threshold'
    )" == "true" ]]; then
        unstable_count=$((unstable_count + 1))
        echo "::warning title=Unstable PostgreSQL benchmark runner::$benchmark base measurements drifted by ${drift_pct}%"
    fi
done < "$benchmark_filter"

if [[ "$result_count" -eq 0 ]]; then
    echo "No benchmark stability results were evaluated" >&2
    exit 2
fi

if [[ "$unstable_count" -gt 0 ]]; then
    echo "$unstable_count PostgreSQL benchmark base measurement(s) drifted by more than ${stability_threshold_pct}%" >&2
    exit 1
fi

echo "PostgreSQL benchmark runner was stable ($result_count result(s))."
