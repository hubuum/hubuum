use super::metrics::metric;
use hubuum_task_core::TaskStopReason;
use rstest::rstest;

use crate::errors::ApiError;

use super::*;

const METRIC_SOURCES: &[&str] = &[
    include_str!("../observability/metrics.rs"),
    include_str!("../observability/metrics/computed_field.rs"),
    include_str!("../observability/metrics/db.rs"),
    include_str!("../observability/metrics/event.rs"),
    include_str!("../observability/metrics/export.rs"),
    include_str!("../observability/metrics/http.rs"),
    include_str!("../observability/metrics/import.rs"),
    include_str!("../observability/metrics/inventory.rs"),
    include_str!("../observability/metrics/login.rs"),
    include_str!("../observability/metrics/process.rs"),
    include_str!("../observability/metrics/registry.rs"),
    include_str!("../observability/metrics/remote_call.rs"),
    include_str!("../observability/metrics/scrape.rs"),
    include_str!("../observability/metrics/schema.rs"),
    include_str!("../observability/metrics/secret.rs"),
    include_str!("../observability/metrics/security.rs"),
    include_str!("../observability/metrics/storage.rs"),
    include_str!("../observability/metrics/task.rs"),
    include_str!("../observability/metrics/template.rs"),
    include_str!("../observability/metrics/token.rs"),
    include_str!("../observability/metrics/tracing.rs"),
];

#[test]
fn operational_contract_is_deterministic_and_contains_every_family() {
    let first = generate_json();
    let second = generate_json();
    let value: serde_json::Value = serde_json::from_str(&first).unwrap();

    assert_eq!(first, second);
    assert_eq!(value["schema_version"], CONTRACT_SCHEMA_VERSION);
    assert!(
        value["metrics"]
            .as_array()
            .is_some_and(|metrics| !metrics.is_empty())
    );
    assert!(
        value["configuration"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );
    assert!(value["events"].is_object());
    assert!(value["documents"].is_object());
    assert_eq!(value["cli"].as_array().map(Vec::len), Some(2));
}

#[rstest]
#[case("HUBUUM_ACTIX_WORKERS", 1)]
#[case("HUBUUM_TASK_WORKERS", 2)]
fn cpu_derived_defaults_do_not_capture_machine_capacity(
    #[case] environment: &str,
    #[case] divisor: usize,
) {
    let configuration = configuration_contracts();
    let configuration_item = configuration
        .iter()
        .find(|item| item.name == environment)
        .unwrap();
    assert!(configuration_item.default_is_set);
    assert!(configuration_item.default.is_empty());
    let configuration_default = configuration_item.dynamic_default.unwrap();
    assert_eq!(configuration_default.source, "available_parallelism");
    assert_eq!(configuration_default.divisor, divisor);
    assert_eq!(configuration_default.rounding, "ceiling");
    assert_eq!(configuration_default.minimum, 1);

    let cli = cli_contract("hubuum-server", crate::config::app_command());
    let cli_option = cli
        .options
        .iter()
        .find(|option| option.environment.as_deref() == Some(environment))
        .unwrap();
    assert!(cli_option.default.is_empty());
    let cli_default = cli_option.dynamic_default.unwrap();
    assert_eq!(cli_default.source, "available_parallelism");
    assert_eq!(cli_default.divisor, divisor);
    assert_eq!(cli_default.rounding, "ceiling");
    assert_eq!(cli_default.minimum, 1);
}

#[test]
#[should_panic(expected = "CLI requirement registry disagrees")]
fn cli_requirement_validation_detects_an_unregistered_dependency() {
    let mut command = crate::administration::admin_command()
        .mut_arg("reset_password", |argument| argument.requires("backup"));
    command.build();

    validate_cli_requirements("hubuum-admin", &command);
}

#[rstest]
#[case(false)]
#[case(true)]
fn cli_contract_includes_command_wide_required_options(#[case] takes_value: bool) {
    let command = Command::new("fixture")
        .arg(Arg::new("optional").long("optional"))
        .arg(
            Arg::new("required")
                .long("required")
                .required(true)
                .action(if takes_value {
                    ArgAction::Set
                } else {
                    ArgAction::SetTrue
                }),
        );

    let contract = cli_contract("fixture", command);

    assert!(
        contract
            .options
            .iter()
            .find(|option| option.id == "required")
            .unwrap()
            .required
    );
}

#[test]
fn cli_dependency_probes_do_not_run_value_validators() {
    let command = Command::new("fixture").arg(
        Arg::new("port")
            .long("port")
            .value_parser(clap::value_parser!(u16).range(1024..)),
    );
    assert!(
        command
            .clone()
            .try_get_matches_from(["fixture", "--port", "8080"])
            .is_ok()
    );
    let contract = cli_contract("fixture", command);
    assert_eq!(contract.options[0].value_kind, "integer");
}

#[test]
fn cli_dependency_probes_accept_custom_validated_types() {
    let command =
        Command::new("fixture").arg(Arg::new("name").long("name").value_parser(|value: &str| {
            value
                .strip_prefix("valid:")
                .map(str::to_string)
                .ok_or("missing prefix")
        }));
    let contract = cli_contract("fixture", command);
    assert!(contract.options[0].requires.is_empty());
}

#[rstest]
#[case(false)]
#[case(true)]
fn cli_dependency_probes_supply_required_positionals_in_index_order(#[case] with_defaults: bool) {
    let mut command = Command::new("fixture")
        .arg(Arg::new("optional").long("optional"))
        .arg(Arg::new("z_first").index(1).required(true))
        .arg(Arg::new("a_second").index(2).required(true));
    if with_defaults {
        command = command
            .mut_arg("z_first", |argument| argument.default_value("first"))
            .mut_arg("a_second", |argument| argument.default_value("second"));
    }
    command.build();
    let invocation = cli_invocation("fixture", &command, ["a_second", "optional", "z_first"]);
    assert_eq!(
        invocation,
        if with_defaults {
            ["fixture", "--optional", "value", "--", "first", "second"]
        } else {
            ["fixture", "--optional", "value", "--", "value", "value"]
        }
    );
    let contract = cli_contract("fixture", command);
    assert_eq!(
        contract
            .options
            .iter()
            .filter(|option| option.required)
            .count(),
        2
    );
}

#[test]
fn cli_dependency_probes_supply_last_positionals_after_the_separator() {
    let command = Command::new("fixture")
        .arg(Arg::new("optional").long("optional"))
        .arg(Arg::new("last").last(true).required(true));
    let contract = cli_contract("fixture", command);
    assert!(
        contract
            .options
            .iter()
            .find(|option| option.id == "last")
            .unwrap()
            .required
    );
}

#[rstest]
#[case("backup")]
#[case("backup_without_history")]
fn cli_dependencies_are_checked_separately_from_command_requirements(#[case] required: &str) {
    let command = Command::new("hubuum-admin")
        .arg(Arg::new("optional").long("optional"))
        .arg(Arg::new("backup").long("backup"))
        .arg(
            Arg::new("backup_without_history")
                .long("backup-without-history")
                .action(ArgAction::SetTrue)
                .requires("backup"),
        )
        .mut_arg(required, |argument| argument.required(true));

    let contract = cli_contract("hubuum-admin", command);

    assert_eq!(
        contract
            .options
            .iter()
            .find(|option| option.id == "backup_without_history")
            .unwrap()
            .requires,
        ["backup"]
    );
}

#[test]
#[should_panic(expected = "CLI requirement registry disagrees")]
fn cli_required_options_do_not_hide_unregistered_dependencies() {
    let command = Command::new("fixture")
        .arg(Arg::new("optional").long("optional").requires("required"))
        .arg(Arg::new("required").long("required").required(true));

    cli_contract("fixture", command);
}

#[test]
#[should_panic(expected = "CLI requirement registry disagrees")]
fn cli_required_options_do_not_hide_removed_dependencies() {
    let command = Command::new("hubuum-admin")
        .arg(Arg::new("backup").long("backup").required(true))
        .arg(
            Arg::new("backup_without_history")
                .long("backup-without-history")
                .action(ArgAction::SetTrue),
        );

    cli_contract("hubuum-admin", command);
}

#[test]
fn document_fields_follow_serde_names_and_missing_field_behavior() {
    #[derive(Serialize, serde::Deserialize)]
    struct Fixture {
        #[serde(rename = "wire_name")]
        rust_name: u32,
        optional: Option<String>,
    }

    let fields = serialized_document_fields(&Fixture {
        rust_name: 1,
        optional: None,
    });

    assert_eq!(fields.required, ["wire_name"]);
    assert_eq!(fields.optional, ["optional"]);
}

#[test]
fn document_sections_follow_serde_renames_and_omissions() {
    #[derive(Serialize)]
    struct Fixture {
        #[serde(rename = "memberships")]
        group_memberships: Vec<()>,
        #[serde(skip_serializing_if = "Option::is_none")]
        removed: Option<()>,
    }

    let sections = serialized_section_names(&Fixture {
        group_memberships: Vec::new(),
        removed: None,
    });

    assert_eq!(sections, ["memberships"]);
}

#[test]
fn metric_registry_rejects_obvious_high_cardinality_labels() {
    let metric = metric!("hubuum_bad", Gauge, None, ["user_id"], &[], Process, "bad");

    assert!(validate_metrics(&[metric]).is_err());
}

#[test]
fn metric_registry_has_unique_labels_and_names() {
    validate_metrics(METRICS).unwrap();
    for metric in METRICS {
        assert_eq!(
            metric.labels.iter().copied().collect::<BTreeSet<_>>().len(),
            metric.labels.len(),
            "duplicate labels on {}",
            metric.name
        );
    }
}

#[test]
fn metric_registry_matches_runtime_instrument_names() {
    let source_names = METRIC_SOURCES
        .iter()
        .flat_map(|source| quoted_strings(source))
        .filter(|value| value.starts_with("hubuum_") || value.starts_with("process_"))
        .filter(|value| !value.contains(' '))
        .collect::<BTreeSet<_>>();
    let contract_names = METRICS
        .iter()
        .map(MetricDefinition::runtime_name)
        .collect::<BTreeSet<_>>();

    assert_eq!(source_names, contract_names);
}

#[test]
fn metric_registry_covers_every_recorded_label_name() {
    let recorded_labels = METRIC_SOURCES
        .iter()
        .flat_map(|source| string_literals_after_call(source, "KeyValue::new("))
        .chain(["template_id", "template_name"])
        .collect::<BTreeSet<_>>();
    let contract_labels = METRICS
        .iter()
        .flat_map(|metric| metric.labels.iter().copied())
        .collect::<BTreeSet<_>>();

    assert_eq!(recorded_labels, contract_labels);
}

#[test]
fn remote_call_method_labels_use_runtime_method_names() {
    let contract = metric_label_contract("hubuum_remote_call_results_total", "method");
    let runtime_names = RemoteHttpMethod::ALL
        .iter()
        .copied()
        .map(RemoteHttpMethod::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();

    assert_eq!(contract.values, runtime_names);
    assert!(
        contract
            .values
            .iter()
            .all(|value| value == &value.to_lowercase())
    );
}

#[test]
fn configuration_inventory_covers_the_environment_registry() {
    let inventory = configuration_contracts();
    for variable in APP_CONFIG_ENVIRONMENT.iter().chain(PROCESS_ENVIRONMENT) {
        assert!(inventory.iter().any(|item| item.name == variable.name));
    }
}

#[test]
fn tracing_context_stays_out_of_public_event_contract() {
    let envelope = serialized_event_envelope(true, true);
    assert_eq!(envelope["correlation_id"], "sample");
    assert!(envelope.get("trace_link").is_none());
}

#[rstest]
#[case("HUBUUM_TRACING_CONNECT_TIMEOUT_MS", 1.0, 60_000.0)]
#[case("HUBUUM_TRACING_EXPORT_TIMEOUT_MS", 1.0, 60_000.0)]
#[case("HUBUUM_TRACING_FLUSH_TIMEOUT_MS", 1.0, 60_000.0)]
#[case("HUBUUM_TRACING_QUEUE_CAPACITY", 1.0, 65_536.0)]
#[case("HUBUUM_TRACING_BATCH_SIZE", 1.0, 8_192.0)]
#[case("HUBUUM_TRACING_SAMPLE_RATIO", 0.0, 1.0)]
fn tracing_configuration_publishes_numeric_bounds(
    #[case] name: &str,
    #[case] minimum: f64,
    #[case] maximum: f64,
) {
    let contract = configuration_contracts()
        .into_iter()
        .find(|item| item.name == name)
        .unwrap();
    assert_eq!(contract.minimum.unwrap().as_f64(), Some(minimum));
    assert_eq!(contract.maximum.unwrap().as_f64(), Some(maximum));
}

#[test]
fn token_lifetime_settings_publish_the_runtime_integer_cap() {
    let expected = (Some(1), Some(hubuum_domain::TokenLifetime::MAX_HOURS));

    assert_eq!(
        configuration_bounds("HUBUUM_TOKEN_LIFETIME_HOURS"),
        expected
    );
    assert_eq!(
        configuration_bounds("HUBUUM_MAX_TOKEN_LIFETIME_HOURS"),
        expected
    );
}

#[rstest]
#[case(
    "HUBUUM_EVENT_FANOUT_BATCH_SIZE",
    1,
    hubuum_domain::MAX_EVENT_WORKER_BATCH_SIZE
)]
#[case(
    "HUBUUM_EVENT_DELIVERY_BATCH_SIZE",
    1,
    hubuum_domain::MAX_EVENT_WORKER_BATCH_SIZE
)]
#[case(
    "HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE",
    1,
    hubuum_domain::MAX_EVENT_WORKER_BATCH_SIZE
)]
#[case(
    "HUBUUM_TOKEN_RETENTION_PURGE_BATCH_SIZE",
    hubuum_domain::TokenRetentionBatchSize::MIN,
    hubuum_domain::TokenRetentionBatchSize::MAX
)]
#[case(
    "HUBUUM_COMPUTED_REINDEX_BATCH_SIZE",
    1,
    crate::config::MAX_COMPUTED_REINDEX_BATCH_SIZE
)]
#[case(
    "HUBUUM_MAX_TRANSITIVE_DEPTH",
    1,
    hubuum_query::MAX_TRAVERSAL_DEPTH as usize
)]
fn numeric_settings_publish_the_authoritative_runtime_bounds(
    #[case] name: &str,
    #[case] minimum: usize,
    #[case] maximum: usize,
) {
    assert_eq!(
        configuration_bounds(name),
        (Some(minimum as i64), Some(maximum as i64))
    );
}

#[rstest]
#[case(serde_json::json!(1), "integer")]
#[case(serde_json::json!(1.5), "number")]
#[case(serde_json::json!("1"), "string")]
#[case(serde_json::json!(true), "boolean")]
#[case(serde_json::json!([]), "array")]
#[case(serde_json::json!({}), "object")]
fn event_fields_follow_serialized_wire_types(
    #[case] value: serde_json::Value,
    #[case] expected: &str,
) {
    let minimal = serde_json::json!({"id": null});
    let populated = serde_json::json!({"id": value});
    let direct = direct_field_contracts(&minimal, &populated);
    let nested = nested_field_contracts("", &minimal, &populated, &populated);
    assert_eq!(direct, nested);
    assert_eq!(direct[0].wire_type, expected);
    assert!(direct[0].nullable);
}

#[test]
fn provenance_name_wire_types_preserve_optional_nullability() {
    let contract = event_contract();
    let name = contract
        .provenance_fields
        .iter()
        .find(|field| field.name == "actor.principal.name")
        .unwrap();
    assert_eq!(name.wire_type, "string");
    assert!(name.nullable);
}

#[test]
#[should_panic(expected = "wire-type fixture must populate every nullable field")]
fn event_type_fixtures_cannot_silently_omit_nullable_values() {
    direct_field_contracts(
        &serde_json::json!({"id": null}),
        &serde_json::json!({"id": null}),
    );
}

#[test]
fn artifact_retention_settings_publish_the_runtime_duration_caps() {
    use crate::models::retention::{MAX_FUTURE_RETENTION_HOURS, MAX_FUTURE_RETENTION_MINUTES};

    for name in [
        "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS",
        "HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS",
    ] {
        assert_eq!(
            configuration_bounds(name),
            (Some(1), Some(MAX_FUTURE_RETENTION_HOURS))
        );
    }
    assert_eq!(
        configuration_bounds("HUBUUM_RESTORE_STAGE_RETENTION_MINUTES"),
        (Some(1), Some(MAX_FUTURE_RETENTION_MINUTES))
    );
}

#[test]
fn process_only_configuration_preserves_typed_metadata() {
    let inventory = configuration_contracts();
    let secret_source = inventory
        .iter()
        .find(|item| item.name == "HUBUUM_SECRET_SOURCE")
        .unwrap();
    assert_eq!(secret_source.value_kind, "enum");
    assert!(secret_source.default_is_set);
    assert_eq!(secret_source.default, ["environment"]);
    assert_eq!(secret_source.allowed_values, ["environment", "file"]);

    let stable_key = inventory
        .iter()
        .find(|item| item.name == "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY")
        .unwrap();
    assert_eq!(stable_key.value_kind, "boolean");
    assert!(stable_key.default_is_set);
    assert_eq!(stable_key.default, ["false"]);
    assert_eq!(stable_key.allowed_values, ["true", "false", "1", "0"]);

    let secret_root = inventory
        .iter()
        .find(|item| item.name == "HUBUUM_SECRET_FILE_ROOT")
        .unwrap();
    assert_eq!(secret_root.value_kind, "path");
    assert!(!secret_root.default_is_set);
}

#[test]
fn configuration_constraints_include_secret_file_root_requirement() {
    assert!(configuration_constraints().iter().any(|constraint| {
        constraint == "HUBUUM_SECRET_SOURCE=file requires HUBUUM_SECRET_FILE_ROOT"
    }));
}

#[test]
fn configuration_constraints_include_token_key_startup_requirements() {
    let constraints = configuration_constraints();
    assert!(constraints.iter().any(|constraint| {
        constraint == "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS requires HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID"
    }));
    assert!(constraints.iter().any(|constraint| {
        constraint
            == "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true requires resolvable stable token hash key material"
    }));
}

#[test]
fn configuration_constraints_include_the_treetop_feature_prerequisite() {
    assert!(configuration_constraints().iter().any(|constraint| {
        constraint
            == "HUBUUM_PERMISSION_BACKEND=treetop requires HUBUUM_TREETOP_URL and the compiled permissions-treetop feature"
    }));
}

#[test]
fn page_limit_constraint_expression_comes_from_its_executable_operator() {
    assert!(crate::config::environment::constraints::PAGE_LIMITS.ordered_values_satisfy(100, 100));
    assert_eq!(
        crate::config::environment::constraints::PAGE_LIMITS.expression(),
        "HUBUUM_DEFAULT_PAGE_LIMIT must not exceed HUBUUM_MAX_PAGE_LIMIT"
    );
}

#[test]
fn administrator_configuration_includes_shared_database_and_token_settings() {
    let inventory = configuration_contracts();
    for name in [
        "HUBUUM_DATABASE_URL",
        "HUBUUM_DATABASE_ROLE_MODE",
        "HUBUUM_TOKEN_HASH_KEY",
        "HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID",
        "HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS",
        "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY",
        "HUBUUM_SECRET_SOURCE",
        "HUBUUM_SECRET_FILE_ROOT",
    ] {
        let setting = inventory.iter().find(|item| item.name == name).unwrap();
        assert!(
            setting.runtime_roles.contains(&"admin"),
            "{name} is consumed by hubuum-admin"
        );
    }
}

#[test]
fn closed_metric_label_domains_are_enumerated() {
    for metric in metric_contracts() {
        for label in metric.labels {
            if label.bounded_by == "enumerated values" {
                assert!(
                    !label.values.is_empty(),
                    "{} label {} has no enumerated values",
                    metric.name,
                    label.name
                );
            }
        }
    }
}

#[rstest]
#[case(ApiError::ReauthenticationRequired)]
#[case(ApiError::TaskStopped(TaskStopReason::Cancelled))]
#[case(ApiError::TaskStopped(TaskStopReason::DeadlineExceeded))]
fn credential_and_task_error_classes_are_recordable(#[case] error: ApiError) {
    assert!(metric_label_value_is_allowed(
        "hubuum_api_errors_total",
        "class",
        error.class(),
    ));
}

#[rstest]
#[case("queued")]
#[case("coalesced")]
#[case("queue_full")]
#[case("queue_busy")]
#[case("enqueue_failure")]
#[case("enqueue_timeout")]
#[case("success")]
#[case("failure")]
fn computed_read_repair_outcomes_are_recordable(#[case] outcome: &str) {
    assert!(metric_label_value_is_allowed(
        "hubuum_computed_field_read_repairs_total",
        "outcome",
        outcome,
    ));
}

#[test]
fn event_contract_uses_serialized_envelope_shape() {
    let contract = event_contract();
    let serialized = serialized_event_envelope(false, false);
    let serialized_fields = serialized
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let contract_fields = contract
        .envelope_fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(contract_fields, serialized_fields);
    assert_eq!(contract.envelope_fields, contract.sink_payload_fields);
    assert!(
        contract
            .provenance_fields
            .iter()
            .any(|field| field.name == "actor.kind" && !field.nullable)
    );
}

#[rstest]
#[case(false)]
#[case(true)]
fn event_contract_versions_match_production_events(#[case] revision_aware: bool) {
    let contract = event_contract();
    let version = if revision_aware {
        contract.revision_aware_schema_version
    } else {
        contract.schema_version
    };
    let event = sample_event(revision_aware);

    assert_eq!(version, event.schema_version());
    assert_eq!(
        serialized_event_envelope(revision_aware, false)["schema_version"],
        version
    );
}

#[test]
fn token_metric_contracts_match_runtime_labels() {
    let metrics = metric_contracts();
    for (name, expected) in [
        (
            "hubuum_token_authentications_total",
            &["format", "key_state", "outcome"][..],
        ),
        ("hubuum_token_hash_keys", &["state"][..]),
        ("hubuum_token_hash_stored", &["key_state", "lifecycle"][..]),
    ] {
        let metric = metrics.iter().find(|metric| metric.name == name).unwrap();
        assert_eq!(
            metric
                .labels
                .iter()
                .map(|label| label.name)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn metric_domains_include_emitted_refresh_and_wakeup_values() {
    let metrics = metric_contracts();
    let refresh_sources = &metrics
        .iter()
        .find(|metric| metric.name == "hubuum_metrics_refresh_duration_seconds")
        .unwrap()
        .labels[0]
        .values;
    assert!(refresh_sources.iter().any(|value| value == "token_keys"));

    let wakeup_kinds = &metrics
        .iter()
        .find(|metric| metric.name == "hubuum_event_worker_wakeups_total")
        .unwrap()
        .labels
        .iter()
        .find(|label| label.name == "kind")
        .unwrap()
        .values;
    assert!(
        wakeup_kinds
            .iter()
            .any(|value| value == "notifications_sent")
    );
    assert!(
        !wakeup_kinds
            .iter()
            .any(|value| value == "notification_send")
    );
}

#[test]
fn secret_configuration_defaults_are_never_serialized() {
    for item in configuration_contracts()
        .into_iter()
        .filter(|item| item.exposure == "secret")
    {
        assert!(item.default.is_empty(), "{} exposes a default", item.name);
    }
}

#[test]
fn generated_metric_reference_is_committed() {
    assert_eq!(
        normalize_line_endings(include_str!("../../docs/metrics-reference.md")),
        generate_metrics_markdown()
    );
}

#[cfg(feature = "embedded-migrations")]
#[test]
fn generated_operational_contract_is_committed() {
    assert_eq!(
        normalize_line_endings(include_str!("../../docs/operational-contract.json")),
        generate_json()
    );
}

#[rstest]
#[case("\n")]
#[case("\r\n")]
fn generated_artifact_comparison_normalizes_platform_line_endings(#[case] line_ending: &str) {
    let committed = ["first", "second", ""].join(line_ending);

    assert_eq!(normalize_line_endings(&committed), "first\nsecond\n");
}

fn normalize_line_endings(text: &str) -> String {
    text.replace("\r\n", "\n")
}

fn quoted_strings(source: &str) -> impl Iterator<Item = &str> {
    // Test assertions may contain exported metric names and sample labels;
    // this check inventories the production instrument definitions.
    let source = source
        .split_once("#[cfg(test)]")
        .map_or(source, |(production, _)| production);
    source.split('"').skip(1).step_by(2)
}

fn string_literals_after_call<'a>(
    source: &'a str,
    marker: &'a str,
) -> impl Iterator<Item = &'a str> {
    source.match_indices(marker).filter_map(move |(index, _)| {
        source[index + marker.len()..]
            .trim_start()
            .strip_prefix('"')?
            .split('"')
            .next()
    })
}
