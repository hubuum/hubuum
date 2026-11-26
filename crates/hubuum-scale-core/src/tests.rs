use super::*;

#[test]
fn bundled_profiles_are_deterministic_and_preserve_scale() {
    let large = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let huge = ScaleProfile::bundled(ProfileName::Huge).unwrap();
    let first = large.manifest().unwrap();
    let second = large.manifest().unwrap();

    assert_eq!(first, second);
    assert_eq!(first.schema_version, 3);
    assert_eq!(first.semantic_digest.len(), 64);
    assert_eq!(huge.totals.objects, large.totals.objects * 4);
    assert_eq!(
        huge.totals.object_relations,
        large.totals.object_relations * 4
    );
    assert_eq!(
        huge.overlays.event_deliveries,
        large.overlays.event_deliveries * 4
    );
    assert!(
        huge.provisioning.restore_max_upload_bytes >= large.provisioning.restore_max_upload_bytes
    );
    assert!(huge.manifest().unwrap().semantic_digest != first.semantic_digest);
}

#[test]
fn older_profile_files_keep_default_capture_and_process_limits() {
    let value = serde_json::json!({"backup_max_output_bytes": 256,
        "restore_max_upload_bytes": 256, "db_statement_timeout_ms": 0, "db_pool_size": 20});
    let limits: ProvisioningLimits = serde_json::from_value(value).unwrap();
    assert_eq!(limits.backup_max_capture_rows, 1_000_000);
    assert_eq!(
        limits.lifecycle_max_resident_bytes,
        default_lifecycle_memory()
    );
    assert_eq!(
        limits.lifecycle_stage_timeout_seconds,
        default_lifecycle_timeout()
    );
}

#[test]
fn large_capture_budget_covers_live_and_open_history_rows() {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let state_rows = profile.totals.objects
        + profile.totals.object_relations
        + profile.totals.classes
        + profile.totals.class_relations;
    assert!(profile.provisioning.backup_max_capture_rows > 2 * state_rows);
    assert!(profile.provisioning.backup_max_output_bytes > 477_540_456);
}

#[test]
fn scale_increments_are_arbitrary_and_change_one_declared_region() {
    let baseline = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let classes = baseline
        .clone()
        .with_increment(ScaleAxis::Classes, 400)
        .unwrap();
    let objects = baseline
        .clone()
        .with_increment(ScaleAxis::Objects, 2_500)
        .unwrap();
    let object_heavy = baseline
        .clone()
        .with_increment(ScaleAxis::ObjectHeavyObjects, 2_500)
        .unwrap();
    let relations = baseline
        .clone()
        .with_increment(ScaleAxis::ObjectRelations, 125_000)
        .unwrap();
    let concentrated = baseline
        .clone()
        .with_increment(ScaleAxis::ConcentratedObjectRelations, 125_000)
        .unwrap();
    let dense = baseline
        .clone()
        .with_increment(ScaleAxis::DenseObjectRelations, 100_000)
        .unwrap();

    assert_eq!(classes.totals.classes, baseline.totals.classes + 400);
    assert_eq!(
        classes.regions.balanced.classes,
        baseline.regions.balanced.classes + 400
    );
    assert_eq!(classes.totals.objects, baseline.totals.objects);
    assert_eq!(
        classes.totals.object_relations,
        baseline.totals.object_relations
    );
    assert_eq!(objects.totals.objects, baseline.totals.objects + 2_500);
    assert_eq!(
        objects.regions.balanced.objects,
        baseline.regions.balanced.objects + 2_500
    );
    assert_eq!(objects.regions.class_heavy, baseline.regions.class_heavy);
    assert_eq!(
        object_heavy.regions.object_heavy.objects,
        baseline.regions.object_heavy.objects + 2_500
    );
    assert_eq!(object_heavy.regions.balanced, baseline.regions.balanced);
    assert_eq!(
        relations.totals.object_relations,
        baseline.totals.object_relations + 125_000
    );
    assert_eq!(
        relations.regions.balanced.object_relations,
        baseline.regions.balanced.object_relations + 125_000
    );
    assert_eq!(relations.regions.class_heavy, baseline.regions.class_heavy);
    assert_eq!(
        concentrated.regions.object_heavy.object_relations,
        baseline.regions.object_heavy.object_relations + 125_000
    );
    assert_eq!(concentrated.regions.balanced, baseline.regions.balanced);
    assert_eq!(
        dense.regions.class_heavy.object_relations,
        baseline.regions.class_heavy.object_relations + 100_000
    );
    assert_eq!(dense.regions.balanced, baseline.regions.balanced);
    classes.manifest().unwrap();
    object_heavy.manifest().unwrap();
    relations.manifest().unwrap();
    concentrated.manifest().unwrap();
    dense.manifest().unwrap();
}

#[test]
fn calibrated_sensitivity_plan_uses_signal_bearing_independent_steps() {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let spec = SensitivitySpec::bundled().unwrap();
    let plan = spec.plan(&profile, LimitMode::Standard).unwrap();
    let extended = spec.plan(&profile, LimitMode::Extended).unwrap();

    assert_eq!(plan.points.len(), 18);
    assert_eq!(plan.limit_mode, LimitMode::Standard);
    assert_eq!(
        plan.points[0],
        SensitivityPlanPoint {
            axis: ScaleAxis::Classes,
            added_percent: 20,
            added_count: 800,
            comparison_total: 4_800,
        }
    );
    assert_eq!(plan.points[5].comparison_total, 500_000);
    assert_eq!(plan.points[6].axis, ScaleAxis::ObjectHeavyObjects);
    assert_eq!(plan.points[9].added_count, 200_000);
    assert_eq!(plan.points[14].comparison_total, 2_000_000);
    assert_eq!(plan.points[15].axis, ScaleAxis::DenseObjectRelations);
    assert_eq!(plan.points[15].added_count, 100_000);
    assert_eq!(plan.points[17].added_count, 550_000);
    assert_eq!(extended.limit_mode, LimitMode::Extended);
    assert_eq!(extended.points.len(), 6);
    assert!(
        extended
            .points
            .iter()
            .all(|point| matches!(point.axis, ScaleAxis::Objects | ScaleAxis::ObjectRelations))
    );
}

#[test]
fn sensitivity_spec_rejects_non_increasing_steps() {
    let mut spec = SensitivitySpec::bundled().unwrap();
    spec.axes[0].percent_steps = vec![20, 20];

    let error = spec.validate().unwrap_err();

    assert!(error.to_string().contains("increasing non-zero"));
}

#[test]
fn relation_manifest_models_eligible_pairs_and_rejects_saturation() {
    let baseline = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let classes = baseline.class_plan();
    let relations = class_relation_plan(&baseline).unwrap();
    let counts = object_relation_counts(&baseline, &classes, &relations).unwrap();
    let manifest = baseline.manifest().unwrap();

    assert_eq!(counts.iter().sum::<u64>(), baseline.totals.object_relations);
    assert_eq!(
        Distribution::from_values(&counts),
        manifest.object_relations_per_class_relation
    );
    let spread = &manifest.relation_shapes["spread_class_relation"];
    assert_eq!(spread.edge_count, 45);
    assert_eq!(spread.pair_capacity, 196);
    assert_eq!(spread.saturation_basis_points, 2_295);

    let mut saturated = baseline;
    saturated.totals.object_relations += 1_000_000;
    saturated.regions.class_heavy.object_relations += 1_000_000;
    let error = saturated.manifest().unwrap_err();
    assert!(error.to_string().contains("class pair capacity"));
}

#[test]
fn limit_modes_preserve_production_defaults_and_declare_elevated_values() {
    let standard = LimitMode::Standard.settings();
    let extended = LimitMode::Extended.settings();

    assert_eq!(standard.default_page_limit, 100);
    assert_eq!(standard.maximum_page_limit, 250);
    assert_eq!(standard.maximum_graph_depth, 100);
    assert_eq!(standard.maximum_export_output_bytes, 262_144);
    assert_eq!(LimitMode::Standard.candidate_page_limit(), 250);
    assert_eq!(
        LimitMode::Extended.candidate_page_limit(),
        MAX_STORAGE_CANDIDATE_PAGE_SIZE
    );
    assert!(extended.maximum_page_limit > standard.maximum_page_limit);
    assert!(extended.maximum_graph_depth > standard.maximum_graph_depth);
    assert!(extended.maximum_export_output_bytes > standard.maximum_export_output_bytes);
}

#[test]
fn manifest_contains_every_named_skew_and_anchor() {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let manifest = profile.manifest().unwrap();

    assert_eq!(manifest.objects_per_class.maximum, 60_000);
    assert!(manifest.objects_per_class.median <= 20);
    assert!(manifest.classes_per_collection.maximum >= 750);
    assert!(manifest.object_relation_degree.maximum >= 10_000);
    assert!(manifest.object_relations_per_class_relation.maximum >= 600_000);
    assert_eq!(manifest.history_revisions.maximum, 48);
    assert!(manifest.principals["sparse"].visible_percent <= 1.0);
    for anchor in [
        "hot_class_id",
        "sparse_class_id",
        "empty_class_id",
        "medium_class_id",
        "hub_object_id",
        "heavy_history_object_id",
        "adversarial_class_id",
        "deep_graph_class_id",
        "deep_graph_object_id",
        "balanced_spread_class_relation_id",
    ] {
        assert!(manifest.anchors.contains_key(anchor));
    }
}

#[test]
fn acceptance_rejects_flattened_shape_even_when_totals_match() {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let mut manifest = profile.manifest().unwrap();
    manifest.objects_per_class = Distribution {
        minimum: 62,
        median: 62,
        p95: 63,
        p99: 63,
        maximum: 63,
    };
    manifest.semantic_digest = semantic_digest(&manifest).unwrap();

    let error = manifest.validate(&profile).unwrap_err();
    assert!(error.to_string().contains("hot class"));
}

#[test]
fn workload_paths_resolve_only_declared_anchors() {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let manifest = profile.manifest().unwrap();
    let workload = WorkloadSpec::bundled().unwrap();

    for limit_mode in [LimitMode::Standard, LimitMode::Extended] {
        for scenario in &workload.scenarios {
            let path = workload
                .render_path(scenario, &manifest, limit_mode)
                .unwrap();
            assert!(path.starts_with("/api/"));
            assert!(!path.contains(['{', '}']));
            if scenario.name == "unified-search" {
                assert!(path.ends_with(&format!(
                    "limit_per_kind={}",
                    limit_mode.candidate_page_limit()
                )));
            }
        }
    }
    assert!(
        workload
            .scenarios
            .iter()
            .find(|scenario| scenario.name == "relations-balanced-spread-class-relation")
            .unwrap()
            .traverse
    );
    for depth in [1, 2, 4] {
        let scenario = workload
            .scenarios
            .iter()
            .find(|scenario| scenario.name == format!("object-graph-depth-{depth}"))
            .unwrap();
        assert!(!scenario.include_in_mixed);
        assert_eq!(scenario.warmup_requests, Some(1));
        assert_eq!(scenario.single_client_samples, Some(3));
        assert!(scenario.path.ends_with(&format!("depth__lte={depth}")));
        assert!(scenario.applies_to(None));
        assert!(scenario.applies_to(Some(ScaleAxis::ObjectRelations)));
        assert_eq!(
            scenario.applies_to(Some(ScaleAxis::DenseObjectRelations)),
            depth <= 2
        );
        assert!(!scenario.applies_to(Some(ScaleAxis::Objects)));
    }
    assert!(
        workload
            .scenarios
            .iter()
            .all(|scenario| scenario.name != "object-graph-depth-7")
    );
}

#[test]
fn traversal_validation_detects_duplicates_missing_and_unauthorized_rows() {
    let allowed = BTreeSet::from([1, 2, 3]);
    let result = validate_traversal(&[1, 2, 2, 4], Some(4), Some(&allowed));

    assert_eq!(result.traversal_duplicates, 1);
    assert_eq!(result.traversal_missing, 1);
    assert_eq!(result.unauthorized_rows, 1);
    assert!(!result.passed());
}

fn report(label: &str) -> ScaleBenchmarkReport {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let workload = WorkloadSpec::bundled().unwrap();
    ScaleBenchmarkReport::new(
        label,
        profile.manifest().unwrap(),
        &workload,
        LimitMode::Standard,
        None,
        RuntimeIdentity {
            runner: "test".to_string(),
            backend: BackendIdentity {
                name: "postgres".to_string(),
                version: "test".to_string(),
                settings: BTreeMap::new(),
            },
            process_fresh: true,
            database_fresh: true,
            deliberate_warmup_requests: 3,
        },
        vec![ScenarioReport {
            name: "point".to_string(),
            phase: "warm".to_string(),
            principal: "admin".to_string(),
            concurrency: 1,
            requests: 1,
            successful_requests: 1,
            failures: 0,
            timeouts: 0,
            status_counts: BTreeMap::from([(200, 1)]),
            requests_per_second: 10.0,
            latency: LatencyDistribution::from_samples(&[5.0]),
            response_bytes: 100,
            response_items: 1,
            graph_nodes: None,
            graph_edges: None,
            pages: 1,
            traversal_ms: None,
            traversal_first_page_ms: None,
            traversal_middle_page_ms: None,
            traversal_final_page_ms: None,
            duplicate_rows: 0,
            missing_rows: 0,
            unauthorized_rows: 0,
            authorization_candidates: None,
            authorized_rows: None,
        }],
        CorrectnessReport {
            request_failures: 0,
            traversal_duplicates: 0,
            traversal_missing: 0,
            unauthorized_rows: 0,
            manifest_mismatches: 0,
            lifecycle_failures: 0,
        },
        ResourceReport {
            application_cpu_seconds: 1.0,
            peak_application_resident_bytes: 10,
            backend: BackendResourceReport {
                cpu_seconds: None,
                peak_resident_bytes: None,
                storage_bytes: 20,
                data_bytes: Some(15),
                index_bytes: Some(5),
                write_ahead_bytes: None,
                metrics: BTreeMap::new(),
            },
            storage_metric_deltas: BTreeMap::new(),
            pool_metric_deltas: BTreeMap::new(),
        },
        LifecycleReport {
            dataset_generation_ms: 1,
            dataset_loading_ms: 2,
            stages: BTreeMap::new(),
            backup_preflight_ms: None,
            backup_size_lower_bound_bytes: None,
            backup_generation_ms: None,
            backup_artifact_bytes: None,
            backup_logical_rows: None,
            backup_section_counts: BTreeMap::new(),
            offline_verification_ms: None,
            restore_ms: None,
            semantic_verification_ms: None,
            computed_rebuild_ms: None,
            outcome: "not_run".to_string(),
            supported_ceiling_bytes: 268_435_456,
        },
    )
    .unwrap()
}

#[test]
fn reports_without_backup_preflight_fields_remain_readable() {
    let expected = report("legacy");
    let mut value = serde_json::to_value(&expected).unwrap();
    let lifecycle = value["lifecycle"].as_object_mut().unwrap();
    lifecycle.remove("backup_preflight_ms");
    lifecycle.remove("backup_size_lower_bound_bytes");
    let decoded: ScaleBenchmarkReport = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, expected);
}

fn report_for_sensitivity(label: &str, profile: &ScaleProfile) -> ScaleBenchmarkReport {
    let mut report = report(label);
    report.manifest = profile.manifest().unwrap();
    let prototype = &report.scenarios[0];
    let mut scenarios = Vec::new();
    for name in [
        "classes-fragmented-global",
        "unified-search",
        "objects-hot-class",
        "relations-balanced-spread-class-relation",
        "relations-concentrated-class-relation",
        "relations-spread-class-relation",
    ] {
        let mut scenario = prototype.clone();
        scenario.name = name.to_string();
        scenario.phase = "warm_single_client".to_string();
        scenarios.push(scenario);
    }
    for name in [
        "objects-hot-class",
        "relations-balanced-spread-class-relation",
    ] {
        let mut traversal = prototype.clone();
        traversal.name = name.to_string();
        traversal.phase = "complete_cursor_traversal".to_string();
        traversal.traversal_ms = Some(10.0);
        scenarios.push(traversal);
    }
    for depth in [1_u64, 2, 4] {
        let mut graph = prototype.clone();
        graph.name = format!("object-graph-depth-{depth}");
        graph.phase = "warm_single_client".to_string();
        graph.graph_nodes = Some(depth * 3 + 1);
        graph.graph_edges = Some(depth * 3);
        scenarios.push(graph);
    }
    report.scenarios = scenarios;
    report
}

#[test]
fn assessment_keeps_performance_informational() {
    let base = report("base");
    let mut head = report("head");
    head.scenarios[0].latency = LatencyDistribution::from_samples(&[5_000.0]);

    let assessment = ScaleAssessment::assess(&head, Some(&base));

    assessment.ensure_passed().unwrap();
    assert!(assessment.markdown().contains("Base `base` → PR `head`"));
    assert!(assessment.markdown().contains("Median p95 change"));
    assert!(assessment.markdown().contains("+99900.0%"));
    assert!(assessment.markdown().contains("<details>"));
    assert!(assessment.markdown().contains("Resource and lifecycle"));
    assert!(
        assessment
            .markdown()
            .contains("single paired run is evidence")
    );
}

#[test]
fn assessment_fails_on_correctness_or_corpus_drift() {
    let base = report("base");
    let mut head = report("head");
    head.correctness.traversal_duplicates = 1;
    head.manifest.seed += 1;

    let error = ScaleAssessment::assess(&head, Some(&base))
        .ensure_passed()
        .unwrap_err();
    assert!(error.to_string().contains("correctness"));
    assert!(error.to_string().contains("not semantically equivalent"));
}

#[test]
fn impact_normalizes_to_the_measured_step_when_no_unit_is_requested() {
    let baseline = report("baseline");
    let mut comparison = report("comparison");
    comparison.manifest = ScaleProfile::bundled(ProfileName::Large)
        .unwrap()
        .with_increment(ScaleAxis::Objects, 2_500)
        .unwrap()
        .manifest()
        .unwrap();
    comparison.scenarios[0].latency = LatencyDistribution::from_samples(&[7.0]);
    comparison.scenarios[0].requests_per_second = 8.0;
    comparison.resources.backend.storage_bytes = 30;
    comparison.scale_axis = Some(ScaleAxis::Objects);

    let impact =
        ScaleImpactReport::compare(&baseline, &comparison, ScaleAxis::Objects, None).unwrap();
    let latency = &impact.scenarios[0].metrics["latency_p95_ms"];

    assert_eq!(impact.axis_delta, 2_500);
    assert_eq!(impact.normalization_unit, 2_500);
    assert_eq!(latency.absolute, 2.0);
    assert_eq!(latency.percent, Some(40.0));
    assert_eq!(latency.per_normalization_unit, 2.0);
    assert_eq!(impact.scenarios[0].metrics["pages"].comparison, 1.0);
    assert!(impact.markdown().contains("Repeat paired trials"));
}

#[test]
fn sensitivity_summary_validates_the_calibrated_matrix_and_states_exact_growth() {
    let profile = ScaleProfile::bundled(ProfileName::Large).unwrap();
    let baseline = report_for_sensitivity("fixed-binary-baseline", &profile);
    let spec = SensitivitySpec::bundled().unwrap();
    let plan = spec.plan(&profile, LimitMode::Standard).unwrap();
    let mut impacts = Vec::new();
    for point in plan.points {
        let comparison_profile = profile
            .clone()
            .with_increment(point.axis, point.added_count)
            .unwrap();
        let mut comparison = report_for_sensitivity(
            &format!("{}-{}", point.axis.as_str(), point.added_percent),
            &comparison_profile,
        );
        comparison.scale_axis = Some(point.axis);
        comparison.resources.backend.storage_bytes += point.added_count;
        comparison.resources.backend.index_bytes = Some(5 + point.added_count / 2);
        let axis_spec = spec
            .axes
            .iter()
            .find(|axis| axis.axis == point.axis)
            .unwrap();
        let primary = comparison
            .scenarios
            .iter_mut()
            .find(|scenario| {
                scenario.name == axis_spec.scenario && scenario.phase == axis_spec.phase
            })
            .unwrap();
        primary.latency =
            LatencyDistribution::from_samples(&[5.0 + point.added_percent as f64 / 10.0]);
        if let Some(traversal_phase) = axis_spec.traversal_phase.as_deref() {
            let traversal = comparison
                .scenarios
                .iter_mut()
                .find(|scenario| {
                    scenario.name == axis_spec.scenario && scenario.phase == traversal_phase
                })
                .unwrap();
            traversal.pages = match point.added_percent {
                20 => 1,
                50 => 2,
                _ => 3,
            };
            traversal.traversal_ms = Some(10.0 + point.added_percent as f64 / 10.0);
        }
        let workload = WorkloadSpec::bundled().unwrap();
        comparison.scenarios.retain(|report| {
            workload
                .scenarios
                .iter()
                .find(|scenario| scenario.name == report.name)
                .is_none_or(|scenario| scenario.applies_to(Some(point.axis)))
        });
        impacts.push(ScaleImpactReport::compare(&baseline, &comparison, point.axis, None).unwrap());
    }

    let summary = ScaleSensitivityReport::summarize(&baseline, &impacts, &spec).unwrap();
    let markdown = summary.markdown();

    assert_eq!(summary.axes.len(), 6);
    assert_eq!(summary.axes[3].depth_probes.len(), 3);
    assert_eq!(summary.axes[5].depth_probes.len(), 2);
    assert_eq!(
        summary.axes[3].points[1]
            .traversal_pages
            .as_ref()
            .unwrap()
            .comparison,
        2.0
    );
    assert!(markdown.contains("Baseline corpus:"));
    assert!(markdown.contains("Signals at a glance"));
    assert_eq!(markdown.matches("<details>").count(), 6);
    assert_eq!(markdown.matches("</details>").count(), 6);
    assert!(markdown.contains("Measurement baseline:"));
    assert!(markdown.contains("| Growth | p95 ms | Requests/s |"));
    assert!(markdown.contains("## postgres / large / page limit 250 scale growth"));
    assert!(markdown.contains("Class-count growth"));
    assert!(markdown.contains("Object-heavy growth"));
    assert!(markdown.contains("Concentrated object-relation growth"));
    assert!(markdown.contains("Dense class-pair relation growth"));
    assert!(markdown.contains("Pair saturation"));
    assert!(markdown.contains("85.2%"));
    assert!(markdown.contains("250,000"));
    assert!(markdown.contains("+20% (+50,000)"));
    assert!(markdown.contains("300,000"));
    assert!(markdown.contains("+100% (+1,000,000)"));
    assert!(markdown.contains("pages `1 → 1 / 2 / 3`"));
    assert!(markdown.contains("depth 4 graph p95"));
    assert!(markdown.contains("Returned nodes/edges by maximum depth"));
    assert!(markdown.contains("reachable graph stays fixed"));
    assert!(markdown.contains("extra work caused by local fan-out"));
    assert!(markdown.contains("Each percentage is relative to the stated baseline"));

    let mut extended_summary = summary.clone();
    extended_summary.limit_mode = LimitMode::Extended;
    assert!(
        extended_summary
            .markdown()
            .contains("## postgres / large / page limit 1500 scale growth")
    );
}

#[test]
fn impact_rejects_experiments_that_change_multiple_axes() {
    let baseline = report("baseline");
    let mut comparison = report("comparison");
    comparison.manifest = ScaleProfile::bundled(ProfileName::Large)
        .unwrap()
        .with_increment(ScaleAxis::Objects, 2_500)
        .unwrap()
        .with_increment(ScaleAxis::ObjectRelations, 125_000)
        .unwrap()
        .manifest()
        .unwrap();
    comparison.scale_axis = Some(ScaleAxis::Objects);

    let error =
        ScaleImpactReport::compare(&baseline, &comparison, ScaleAxis::Objects, None).unwrap_err();

    assert!(error.to_string().contains("totals other than objects"));
}

#[test]
fn backend_comparison_accepts_matching_logical_runs() {
    let postgres = report("postgres-run");
    let mut memory = report("memory-run");
    memory.runtime.backend.name = "memory".to_string();
    memory.runtime.backend.version = "1".to_string();
    memory.scenarios[0].latency = LatencyDistribution::from_samples(&[2.5]);

    let comparison = BackendComparisonReport::compare(&[postgres, memory]).unwrap();

    assert_eq!(comparison.backends[0].identity.name, "memory");
    assert_eq!(comparison.backends[1].identity.name, "postgres");
    assert!(comparison.markdown().contains("Same `large` corpus"));
    assert!(comparison.markdown().contains("| memory | 1 | point |"));
}

#[test]
fn serialized_report_contains_no_credential_fields() {
    let encoded = serde_json::to_string(&report("head")).unwrap();

    for forbidden in ["password", "database_url", "bearer", "token"] {
        assert!(!encoded.to_ascii_lowercase().contains(forbidden));
    }
}
