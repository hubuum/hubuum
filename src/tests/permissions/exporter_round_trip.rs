//! Round-trip test for the SQL → Cedar exporter.
//!
//! Builds a fixture in the SQL `permissions` table via
//! `LocalPermissionBackend`, runs the exporter, parses the Cedar output
//! into `MockAllowRule`s, installs them on a `MockTreetopBackend`, and
//! verifies both backends produce identical decisions for representative
//! requests.
//!
//! The relation OR-vs-AND deviation (Phase 6.1) is acknowledged but not
//! asserted here — that's a documented divergence. We test scenarios
//! where the two backends MUST agree.

#[cfg(test)]
mod tests {
    use regex::Regex;
    use std::sync::Arc;

    use actix_web::test as actix_test;

    use crate::models::{Permissions, PermissionsList};
    use crate::permissions::backend::PermissionBackend;
    use crate::permissions::export::export_cedar_to;
    use crate::permissions::local::LocalPermissionBackend;
    use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::types::{
        PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs, ResourceKind,
        ResourceRef,
    };
    use crate::tests::{
        create_collection_fixture, create_test_group, create_test_user, get_pool_and_config,
    };
    use crate::utilities::auth::generate_random_password;

    #[actix_test]
    async fn exported_cedar_grants_same_non_relation_decisions_as_local() {
        let (pool, _) = get_pool_and_config().await;
        let local: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
            pool.clone(),
            "admin".to_string(),
        ));

        // ─── Fixture ────────────────────────────────────────────────────
        let unique = generate_random_password(8);

        // Create users and groups with proper membership
        let user_alpha = create_test_user(&pool).await;
        let user_beta = create_test_user(&pool).await;
        let g_alpha = create_test_group(&pool).await;
        let g_beta = create_test_group(&pool).await;

        g_alpha
            .add_member_without_events(&pool, &user_alpha)
            .await
            .expect("add user_alpha to g_alpha");
        g_beta
            .add_member_without_events(&pool, &user_beta)
            .await
            .expect("add user_beta to g_beta");

        let ns_one = create_collection_fixture(&pool, &format!("export_one_{unique}")).await;
        let ns_two = create_collection_fixture(&pool, &format!("export_two_{unique}")).await;

        // Grants via the production path so the SQL row shape is correct.
        local
            .apply_permissions(
                ns_one.collection.id,
                g_alpha.id,
                PermissionsList::new(vec![Permissions::ReadCollection, Permissions::ReadClass]),
                false,
            )
            .await
            .expect("grant alpha on one");
        local
            .apply_permissions(
                ns_two.collection.id,
                g_alpha.id,
                PermissionsList::new(vec![Permissions::ReadObject]),
                false,
            )
            .await
            .expect("grant alpha on two");
        local
            .apply_permissions(
                ns_one.collection.id,
                g_beta.id,
                PermissionsList::new(vec![Permissions::DelegateCollection]),
                false,
            )
            .await
            .expect("grant beta on one");

        // ─── Export ─────────────────────────────────────────────────────
        let mut buf: Vec<u8> = Vec::new();
        export_cedar_to(&pool, &mut buf)
            .await
            .expect("export failed");
        let cedar = String::from_utf8(buf).expect("export produced non-utf8");

        // ─── Parse into mock rules ──────────────────────────────────────
        let mock = MockTreetopBackend::new();
        let parsed_count = parse_cedar_into_mock(&cedar, &mock);
        assert!(
            parsed_count > 0,
            "exporter produced zero permit clauses; fixture may not have committed"
        );

        // ─── Compare decisions (non-relation only) ──────────────────────
        let alpha = PrincipalRef::new(user_alpha.id, vec![g_alpha.id]);
        let beta = PrincipalRef::new(user_beta.id, vec![g_beta.id]);

        let cases: Vec<(
            &'static str,
            PrincipalRef,
            PermissionRequest,
            PermissionDecision,
        )> = vec![
            (
                "alpha can read ns_one",
                alpha.clone(),
                req_collection(ns_one.collection.id, Permissions::ReadCollection),
                PermissionDecision::Allow,
            ),
            (
                "alpha cannot delete ns_one",
                alpha.clone(),
                req_collection(ns_one.collection.id, Permissions::DeleteCollection),
                PermissionDecision::Deny,
            ),
            (
                "alpha can read class on ns_one",
                alpha.clone(),
                req_class_on(ns_one.collection.id, 12345, Permissions::ReadClass),
                PermissionDecision::Allow,
            ),
            (
                "alpha cannot read class on ns_two",
                alpha.clone(),
                req_class_on(ns_two.collection.id, 12345, Permissions::ReadClass),
                PermissionDecision::Deny,
            ),
            (
                "alpha can read object on ns_two",
                alpha.clone(),
                req_object_on(ns_two.collection.id, 67890, Permissions::ReadObject),
                PermissionDecision::Allow,
            ),
            (
                "alpha cannot read object on ns_one",
                alpha.clone(),
                req_object_on(ns_one.collection.id, 67890, Permissions::ReadObject),
                PermissionDecision::Deny,
            ),
            (
                "beta can delegate ns_one",
                beta.clone(),
                req_collection(ns_one.collection.id, Permissions::DelegateCollection),
                PermissionDecision::Allow,
            ),
            (
                "beta cannot read ns_one",
                beta.clone(),
                req_collection(ns_one.collection.id, Permissions::ReadCollection),
                PermissionDecision::Deny,
            ),
        ];

        for (label, principal, request, expected) in cases {
            let local_decision = local.authorize(&principal, request.clone()).await.unwrap();
            let mock_decision = mock.authorize(&principal, request).await.unwrap();
            assert_eq!(
                local_decision, expected,
                "{label}: local backend disagrees with expected"
            );
            assert_eq!(
                mock_decision, expected,
                "{label}: exported policy (via mock) disagrees with expected"
            );
        }
    }

    /// Test the relation export shape parses into the documented OR-doubled
    /// MockAllowRules. Doesn't assert decision parity — that's the
    /// documented OR-vs-AND divergence.
    #[actix_test]
    async fn exporter_relation_permits_emit_or_doubled_rules() {
        let (pool, _) = get_pool_and_config().await;
        let local: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
            pool.clone(),
            "admin".to_string(),
        ));

        let unique = generate_random_password(8);
        let user_rel = create_test_user(&pool).await;
        let g_rel = create_test_group(&pool).await;
        g_rel
            .add_member_without_events(&pool, &user_rel)
            .await
            .expect("add user_rel to g_rel");

        let ns_rel = create_collection_fixture(&pool, &format!("export_rel_{unique}")).await;

        local
            .apply_permissions(
                ns_rel.collection.id,
                g_rel.id,
                PermissionsList::new(vec![Permissions::ReadClassRelation]),
                false,
            )
            .await
            .expect("grant relation perm");

        let mut buf: Vec<u8> = Vec::new();
        export_cedar_to(&pool, &mut buf)
            .await
            .expect("export failed");
        let cedar = String::from_utf8(buf).expect("non-utf8");

        // Relations have no `collection_id` attribute in the Cedar schema.
        // The exporter MUST NOT prepend `resource.collection_id == N` for
        // relation resources — Cedar would treat that as undefined-attr
        // and the permit would always evaluate false on a real engine.
        // (Caught while reviewing Phase 6.1: emit_block previously
        // prepended collection_id unconditionally.)
        assert!(
            !cedar.contains("HubuumClassRelation && resource.collection_id"),
            "exporter emitted dead relation policy: HubuumClassRelation with resource.collection_id check.\n\
             Full output:\n{cedar}"
        );
        assert!(
            !cedar.contains("HubuumObjectRelation && resource.collection_id"),
            "exporter emitted dead relation policy: HubuumObjectRelation with resource.collection_id check.\n\
             Full output:\n{cedar}"
        );

        let mock = MockTreetopBackend::new();
        parse_cedar_into_mock(&cedar, &mock);

        // Verify decision shape: a relation with from=ns_rel,to=ns_rel matches
        // (both endpoints satisfy the OR), and one with from=ns_rel,to=other
        // also matches (OR allows it). A relation with from=other,to=other
        // does NOT match.
        let principal = PrincipalRef::new(user_rel.id, vec![g_rel.id]);
        let other_ns = ns_rel.collection.id + 999_999;

        let same_ns_relation = PermissionRequest {
            resource: ResourceRef {
                kind: ResourceKind::ClassRelation,
                id: 42,
                attrs: ResourceAttrs {
                    from_collection_id: Some(ns_rel.collection.id),
                    to_collection_id: Some(ns_rel.collection.id),
                    ..Default::default()
                },
            },
            permissions: vec![Permissions::ReadClassRelation],
        };
        assert_eq!(
            mock.authorize(&principal, same_ns_relation).await.unwrap(),
            PermissionDecision::Allow,
            "OR-doubled rules: relation entirely within ns_rel should allow"
        );

        let cross_ns_relation = PermissionRequest {
            resource: ResourceRef {
                kind: ResourceKind::ClassRelation,
                id: 43,
                attrs: ResourceAttrs {
                    from_collection_id: Some(ns_rel.collection.id),
                    to_collection_id: Some(other_ns),
                    ..Default::default()
                },
            },
            permissions: vec![Permissions::ReadClassRelation],
        };
        assert_eq!(
            mock.authorize(&principal, cross_ns_relation).await.unwrap(),
            PermissionDecision::Allow,
            "OR-doubled rules: relation with one endpoint in ns_rel should allow (this is the documented divergence from Local's AND)"
        );

        let no_endpoint_relation = PermissionRequest {
            resource: ResourceRef {
                kind: ResourceKind::ClassRelation,
                id: 44,
                attrs: ResourceAttrs {
                    from_collection_id: Some(other_ns),
                    to_collection_id: Some(other_ns),
                    ..Default::default()
                },
            },
            permissions: vec![Permissions::ReadClassRelation],
        };
        assert_eq!(
            mock.authorize(&principal, no_endpoint_relation)
                .await
                .unwrap(),
            PermissionDecision::Deny,
            "no endpoint in ns_rel → no rule matches"
        );
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    fn req_collection(ns_id: i32, perm: Permissions) -> PermissionRequest {
        PermissionRequest {
            resource: ResourceRef::collection(ns_id),
            permissions: vec![perm],
        }
    }

    fn req_class_on(ns_id: i32, class_id: i32, perm: Permissions) -> PermissionRequest {
        PermissionRequest {
            resource: ResourceRef {
                kind: ResourceKind::Class,
                id: class_id,
                attrs: ResourceAttrs {
                    collection_id: Some(ns_id),
                    ..Default::default()
                },
            },
            permissions: vec![perm],
        }
    }

    fn req_object_on(ns_id: i32, obj_id: i32, perm: Permissions) -> PermissionRequest {
        PermissionRequest {
            resource: ResourceRef {
                kind: ResourceKind::Object,
                id: obj_id,
                attrs: ResourceAttrs {
                    collection_id: Some(ns_id),
                    ..Default::default()
                },
            },
            permissions: vec![perm],
        }
    }

    /// Minimal Cedar parser tuned to the shape `export_cedar_to` emits.
    /// Returns the number of permit clauses parsed.
    fn parse_cedar_into_mock(cedar: &str, mock: &MockTreetopBackend) -> usize {
        // Capture each permit block as a multi-line chunk.
        let permit_re = Regex::new(r#"(?ms)^permit\(\s*principal in Group::"(\d+)",\s*action in \[([^\]]+)\],\s*resource\s*\)\s*when\s*\{\s*([^}]+)\s*\};"#).unwrap();
        let action_re = Regex::new(r#"Action::"([^"]+)""#).unwrap();

        let mut count = 0;
        for caps in permit_re.captures_iter(cedar) {
            count += 1;
            let group_id: i32 = caps[1].parse().expect("group id");
            let actions_raw = &caps[2];
            let when_clause = caps[3].trim();

            let actions: Vec<Permissions> = action_re
                .captures_iter(actions_raw)
                .map(|c| Permissions::from_string(&c[1]).expect("known permission name"))
                .collect();

            // Decide the resource shape from the when clause.
            if let Some(ns_id) = parse_collection_ns(when_clause) {
                for action in &actions {
                    mock.add_rule(MockAllowRule {
                        group_id,
                        action: *action,
                        resource_kind: ResourceKind::Collection,
                        resource_id: Some(ns_id),
                        attrs: ResourceAttrs::default(),
                    });
                }
            } else if let Some((kind, ns_id)) = parse_child_resource(when_clause) {
                for action in &actions {
                    mock.add_rule(MockAllowRule {
                        group_id,
                        action: *action,
                        resource_kind: kind.clone(),
                        resource_id: None,
                        attrs: ResourceAttrs {
                            collection_id: Some(ns_id),
                            ..Default::default()
                        },
                    });
                }
            } else if let Some((kind, ns_id)) = parse_relation_resource(when_clause) {
                // OR-on-endpoints → emit two rules, one per endpoint.
                // The MockAllowRule attrs match is conjunctive (AND): each Some
                // field on the rule requires an exact match on the request.
                // To express "from_collection_id == N OR to_collection_id == N",
                // emit TWO rules: one with from_collection_id=Some(N), one with
                // to_collection_id=Some(N). The mock evaluates rules
                // disjunctively (any matching rule → Allow), so two rules
                // express the OR semantics.
                for action in &actions {
                    mock.add_rule(MockAllowRule {
                        group_id,
                        action: *action,
                        resource_kind: kind.clone(),
                        resource_id: None,
                        attrs: ResourceAttrs {
                            from_collection_id: Some(ns_id),
                            ..Default::default()
                        },
                    });
                    mock.add_rule(MockAllowRule {
                        group_id,
                        action: *action,
                        resource_kind: kind.clone(),
                        resource_id: None,
                        attrs: ResourceAttrs {
                            to_collection_id: Some(ns_id),
                            ..Default::default()
                        },
                    });
                }
            } else {
                panic!("could not classify when clause: {}", when_clause);
            }
        }
        count
    }

    fn parse_collection_ns(when: &str) -> Option<i32> {
        let re =
            Regex::new(r#"resource is HubuumCollection && resource == HubuumCollection::"(\d+)""#)
                .unwrap();
        re.captures(when).and_then(|c| c[1].parse().ok())
    }

    fn parse_child_resource(when: &str) -> Option<(ResourceKind, i32)> {
        let re = Regex::new(
            r"resource is (HubuumClass|HubuumObject|HubuumTemplate) && resource\.collection_id == (\d+)",
        )
        .unwrap();
        let caps = re.captures(when)?;
        let kind = match &caps[1] {
            "HubuumClass" => ResourceKind::Class,
            "HubuumObject" => ResourceKind::Object,
            "HubuumTemplate" => ResourceKind::Template,
            _ => return None,
        };
        let ns_id: i32 = caps[2].parse().ok()?;
        Some((kind, ns_id))
    }

    fn parse_relation_resource(when: &str) -> Option<(ResourceKind, i32)> {
        // Relations have NO collection_id attribute in the Cedar schema —
        // the predicate is "resource is HubuumXRelation && (from == N || to == N)".
        // Whitespace inside the OR is flexible because the emitter
        // wraps the OR clause across lines for readability.
        let re = Regex::new(
            r"resource is (HubuumClassRelation|HubuumObjectRelation)\s*&&\s*\(\s*resource\.from_collection_id == (\d+)\s*\|\|\s*resource\.to_collection_id == \d+\s*\)",
        )
        .unwrap();
        let caps = re.captures(when)?;
        let kind = match &caps[1] {
            "HubuumClassRelation" => ResourceKind::ClassRelation,
            "HubuumObjectRelation" => ResourceKind::ObjectRelation,
            _ => return None,
        };
        let ns_id: i32 = caps[2].parse().ok()?;
        Some((kind, ns_id))
    }
}
