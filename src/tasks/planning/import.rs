use super::*;
use crate::tasks::control;
use crate::traits::AuthzSubject;

struct ImportPlanner<'a, C, S> {
    backend: &'a C,
    user: &'a S,
    request: &'a ImportRequest,
    mode: ImportMode,
    state: PlanningState,
    outcome: PlanningOutcome,
}

impl<C: AuthorizationContext, S: AuthzSubject> ImportPlanner<'_, C, S> {
    fn checkpoint(&mut self) -> bool {
        if control::checkpoint().is_err() {
            self.outcome.aborted = true;
        }
        !self.outcome.aborted
    }

    fn record(&mut self, result: Result<PlannedItem, PlanningFailure>) -> bool {
        match result {
            Ok(item) => self.outcome.planned_items.push(item),
            Err(failure) => {
                self.outcome.aborted = should_abort_import(
                    self.mode.atomicity.unwrap_or(ImportAtomicity::Strict),
                    self.mode
                        .permission_policy
                        .unwrap_or(ImportPermissionPolicy::Abort),
                    self.mode
                        .collision_policy
                        .unwrap_or(ImportCollisionPolicy::Abort),
                    failure.kind,
                );
                self.outcome.failures.push(failure);
            }
        }
        !self.outcome.aborted
    }

    fn lookup_failed(&mut self, kind: &str, message: String) {
        self.outcome.failures.push(PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result(kind, "lookup", None, None),
            message,
        });
        self.outcome.aborted = true;
    }

    fn log_phase(&self, phase: &str, item_count: usize, start: Instant) {
        info!(message = "Import planning phase finished", phase, item_count,
            planned_items = self.outcome.planned_items.len(), failures = self.outcome.failures.len(),
            aborted = self.outcome.aborted, elapsed = ?start.elapsed());
    }
    fn plan_identities(&mut self) {
        let request = self.request;
        let mode = &self.mode;
        let planned_items = &mut self.outcome.planned_items;
        let aborted = &mut self.outcome.aborted;
        let state = &mut self.state;
        for input in &request.graph.identity_scopes {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            if let Some(reference) = &input.ref_ {
                state
                    .planned_identity_scope_names_by_ref
                    .insert(reference.clone(), input.name.clone());
            }
            planned_items.push(plan_system_item(
                "identity_scope",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertIdentityScope {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in &request.graph.groups {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            let scope = input
                .identity_scope_key
                .as_ref()
                .map(|key| key.name.clone())
                .or_else(|| {
                    input.identity_scope_ref.as_ref().and_then(|reference| {
                        state
                            .planned_identity_scope_names_by_ref
                            .get(reference)
                            .cloned()
                    })
                });
            if let Some(scope) = &scope {
                state
                    .planned_group_keys
                    .insert((scope.clone(), input.groupname.clone()));
            }
            let identifier = scope
                .as_deref()
                .map(|scope| format!("{scope}/{}", input.groupname))
                .unwrap_or_else(|| {
                    format!(
                        "{}/{}",
                        input.identity_scope_ref.as_deref().unwrap_or("unresolved"),
                        input.groupname
                    )
                });
            planned_items.push(plan_system_item(
                "group",
                input.ref_.clone(),
                identifier,
                PlannedExecution::UpsertGroup {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in request
            .graph
            .principals
            .iter()
            .filter(|input| matches!(input.subtype, ImportPrincipalSubtype::Human { .. }))
        {
            planned_items.push(plan_system_item(
                "principal",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertPrincipal {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in
            request.graph.principals.iter().filter(|input| {
                matches!(input.subtype, ImportPrincipalSubtype::ServiceAccount { .. })
            })
        {
            planned_items.push(plan_system_item(
                "principal",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertPrincipal {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in &request.graph.group_memberships {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            planned_items.push(plan_system_item(
                "group_membership",
                input.ref_.clone(),
                input
                    .ref_
                    .clone()
                    .unwrap_or_else(|| "membership".to_string()),
                PlannedExecution::UpsertGroupMembership {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
    }

    fn plan_integrations(&mut self) {
        let request = self.request;
        let mode = &self.mode;
        let planned_items = &mut self.outcome.planned_items;
        let aborted = &mut self.outcome.aborted;
        for input in &request.graph.computed_fields {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            planned_items.push(plan_system_item(
                "computed_field",
                input.ref_.clone(),
                input.key.clone(),
                PlannedExecution::UpsertComputedField {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }

        for input in &request.graph.export_templates {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            planned_items.push(plan_system_item(
                "export_template",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertExportTemplate {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in &request.graph.remote_targets {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            planned_items.push(plan_system_item(
                "remote_target",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertRemoteTarget {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in &request.graph.event_sinks {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            planned_items.push(plan_system_item(
                "event_sink",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertEventSink {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
        for input in &request.graph.event_subscriptions {
            if control::checkpoint().is_err() {
                *aborted = true;
                break;
            }
            planned_items.push(plan_system_item(
                "event_subscription",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertEventSubscription {
                    input: input.clone(),
                    overwrite: import_item_allows_overwrite(input.condition, mode),
                },
            ));
        }
    }

    async fn collections(&mut self) {
        let count = self.request.graph.collections.len();
        let start = Instant::now();
        async {
            for input in &self.request.graph.collections {
                if !self.checkpoint() {
                    break;
                }
                let result =
                    plan_collection(self.backend, self.user, &self.mode, &mut self.state, input)
                        .await;
                if !self.record(result) {
                    break;
                }
            }
        }
        .instrument(info_span!(
            "import_planning_phase",
            phase = "collections",
            item_count = count
        ))
        .await;
        self.log_phase("collections", count, start);
    }

    async fn classes(&mut self) {
        let count = self.request.graph.classes.len();
        let start = Instant::now();
        async {
            for input in &self.request.graph.classes {
                if !self.checkpoint() {
                    break;
                }
                let result =
                    plan_class(self.backend, self.user, &self.mode, &mut self.state, input).await;
                if !self.record(result) {
                    break;
                }
            }
        }
        .instrument(info_span!(
            "import_planning_phase",
            phase = "classes",
            item_count = count
        ))
        .await;
        self.log_phase("classes", count, start);
    }

    async fn objects(&mut self) {
        let count = self.request.graph.objects.len();
        let start = Instant::now();
        async {
            for input in &self.request.graph.objects {
                if !self.checkpoint() {
                    break;
                }
                let result =
                    plan_object(self.backend, self.user, &self.mode, &mut self.state, input).await;
                if !self.record(result) {
                    break;
                }
            }
        }
        .instrument(info_span!(
            "import_planning_phase",
            phase = "objects",
            item_count = count
        ))
        .await;
        self.log_phase("objects", count, start);
    }

    async fn class_relations(&mut self) {
        let count = self.request.graph.class_relations.len();
        let start = Instant::now();
        async {
            for input in &self.request.graph.class_relations {
                if !self.checkpoint() {
                    break;
                }
                let result = plan_class_relation(
                    self.backend,
                    self.user,
                    &self.mode,
                    &mut self.state,
                    input,
                )
                .await;
                if !self.record(result) {
                    break;
                }
            }
        }
        .instrument(info_span!(
            "import_planning_phase",
            phase = "class_relations",
            item_count = count
        ))
        .await;
        self.log_phase("class_relations", count, start);
    }

    async fn object_relations(&mut self) {
        let count = self.request.graph.object_relations.len();
        let start = Instant::now();
        async {
            for input in &self.request.graph.object_relations {
                if !self.checkpoint() {
                    break;
                }
                let result = plan_object_relation(
                    self.backend,
                    self.user,
                    &self.mode,
                    &mut self.state,
                    input,
                )
                .await;
                if !self.record(result) {
                    break;
                }
            }
        }
        .instrument(info_span!(
            "import_planning_phase",
            phase = "object_relations",
            item_count = count
        ))
        .await;
        self.log_phase("object_relations", count, start);
    }

    async fn collection_permissions(&mut self) {
        let count = self.request.graph.collection_permissions.len();
        let start = Instant::now();
        async {
            for input in &self.request.graph.collection_permissions {
                if !self.checkpoint() {
                    break;
                }
                let result = plan_collection_permission(
                    self.backend,
                    self.user,
                    &self.mode,
                    &mut self.state,
                    input,
                )
                .await;
                if !self.record(result) {
                    break;
                }
            }
        }
        .instrument(info_span!(
            "import_planning_phase",
            phase = "collection_permissions",
            item_count = count
        ))
        .await;
        self.log_phase("collection_permissions", count, start);
    }
}

pub(super) async fn plan_import_with_admin_status<C: AuthorizationContext>(
    backend: &C,
    user: &impl AuthzSubject,
    scopes: Option<&TokenScope>,
    request: &ImportRequest,
    admin_status: ImportAdminStatus,
) -> PlanningOutcome {
    let mut state = PlanningState::new();
    state.scopes = scopes.cloned();
    state.admin_status = admin_status;
    let mut planner = ImportPlanner {
        backend,
        user,
        request,
        mode: request.mode(),
        state,
        outcome: PlanningOutcome {
            planned_items: Vec::with_capacity(request.total_items() as usize),
            failures: Vec::new(),
            aborted: false,
        },
    };
    if extended_graph_items(request) > 0 {
        let authorized = scopes.is_none()
            && is_import_admin(backend, user, &mut planner.state)
                .await
                .unwrap_or(false);
        if !authorized {
            planner.outcome.failures.push(PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result("system", "import", None, None),
                message: "Identity, template, and integration imports require an unscoped administrator token".to_string(),
            });
            planner.outcome.aborted = true;
            return planner.outcome;
        }
        if let Some((kind, reference)) = duplicate_extended_ref(request) {
            planner.outcome.failures.push(PlanningFailure {
                kind: FailureKind::Validation,
                item: planned_result(kind, "create", Some(reference.clone()), None),
                message: format!("Duplicate {kind} ref '{reference}'"),
            });
            planner.outcome.aborted = true;
            return planner.outcome;
        }
        planner.plan_identities();
    }
    planner.collections().await;
    if planner.outcome.aborted {
        return planner.outcome;
    }
    if let Err(message) = preload_existing_classes(backend, &mut planner.state, request).await {
        planner.lookup_failed("class", message);
        return planner.outcome;
    }
    planner.classes().await;
    if planner.outcome.aborted {
        return planner.outcome;
    }
    if let Err(message) = preload_existing_objects(backend, &mut planner.state, request).await {
        planner.lookup_failed("object", message);
        return planner.outcome;
    }
    planner.objects().await;
    if planner.outcome.aborted {
        return planner.outcome;
    }
    planner.class_relations().await;
    if planner.outcome.aborted {
        return planner.outcome;
    }
    planner.object_relations().await;
    if planner.outcome.aborted {
        return planner.outcome;
    }
    planner.collection_permissions().await;
    planner.plan_integrations();
    if control::checkpoint().is_err() {
        planner.outcome.aborted = true;
        return planner.outcome;
    }
    let PlanningOutcome {
        planned_items,
        mut failures,
        aborted,
    } = planner.outcome;
    let mode = planner.mode;
    let pool = backend;
    if request.dry_run() {
        match preflight_dry_run(pool, &mode, planned_items).await {
            Ok(mut preflight) => {
                preflight.aborted |= aborted;
                preflight.failures.splice(0..0, failures);
                return preflight;
            }
            Err(error) => {
                failures.push(PlanningFailure {
                    kind: FailureKind::Runtime,
                    item: planned_result("system", "dry_run", None, None),
                    message: sanitize_error_for_storage(&error),
                });
                return PlanningOutcome {
                    planned_items: Vec::new(),
                    failures,
                    aborted: true,
                };
            }
        }
    }

    PlanningOutcome {
        planned_items,
        failures,
        aborted,
    }
}
