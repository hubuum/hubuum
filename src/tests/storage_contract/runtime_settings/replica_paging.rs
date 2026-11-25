use super::*;

async fn page_limit_header(backend: &StorageHandle, token: &str, query: &str) -> Option<String> {
    let response = send(
        backend,
        Some(token),
        test::TestRequest::get().uri(&format!("/api/v1/collections{query}")),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    response
        .headers()
        .get(crate::pagination::PAGE_LIMIT_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

#[actix_web::test]
async fn pagination_default_applies_to_the_next_request_on_each_replica() {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let reader = replica(&storage, false);
    let user = admin(&writer).await;
    let deployment = deployment_page_limits()
        .unwrap()
        .default_limit()
        .to_string();
    let (status, _) = send_json(
        &writer,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 1, "default_page_limit": 2}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let writer_limit = page_limit_header(&writer, &user.raw_token, "").await;
    // The reader has not reconciled yet, so its deployment value still applies.
    let reader_before = page_limit_header(&reader, &user.raw_token, "").await;
    reader.refresh_runtime_settings().await.unwrap();
    let reader_after = page_limit_header(&reader, &user.raw_token, "").await;
    assert_eq!(
        (writer_limit, reader_before, reader_after),
        (Some("2".into()), Some(deployment), Some("2".into()))
    );
}

#[actix_web::test]
async fn a_request_keeps_the_page_limits_it_started_with() {
    let backend = StorageHandle::memory();
    backend.refresh_runtime_settings().await.unwrap();
    let app = test::init_service(
        App::new().app_data(context(&backend)).service(
            web::scope("/probe")
                .wrap(from_fn(crate::middlewares::apply_runtime_settings))
                .route(
                    "",
                    web::get().to(|context: Data<AppContext>| async move {
                        let before = page_limits().unwrap();
                        let storage = context.backend();
                        commit(storage, pagination_limits(3, 5)).await;
                        storage.refresh_runtime_settings().await.unwrap();
                        let after = page_limits().unwrap();
                        HttpResponse::Ok().json(json!([
                            [before.default_limit(), before.maximum_limit()],
                            [after.default_limit(), after.maximum_limit()]
                        ]))
                    }),
                ),
        ),
    )
    .await;
    let in_flight: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri("/probe").to_request())
            .await;
    let deployment = deployment_page_limits().unwrap();
    let limits = [deployment.default_limit(), deployment.maximum_limit()];
    assert_eq!(in_flight, json!([limits, limits]));
}

#[actix_web::test]
async fn client_configuration_reports_the_applied_default() {
    let backend = StorageHandle::memory();
    commit(&backend, pagination(4)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let response = send(
        &backend,
        None,
        test::TestRequest::get().uri("/api/v1/config"),
    )
    .await;
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["pagination"]["default_page_limit"], 4);
}

#[rstest::rstest]
#[case::client("/api/v1/config")]
#[case::admin("/api/v1/admin/config")]
#[actix_web::test]
async fn configuration_reports_the_effective_runtime_maximum(#[case] path: &str) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 8)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::get().uri(path),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(body["pagination"]["max_page_limit"], 8);
}

#[actix_web::test]
async fn runtime_maximum_clamps_explicit_requests_after_replica_refresh() {
    let storage = MemoryStorage::new();
    let writer = replica(&storage, false);
    let reader = replica(&storage, false);
    reader.refresh_runtime_settings().await.unwrap();
    let user = admin(&writer).await;
    let (status, _) = send_json(
        &writer,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 1, "default_page_limit": 2, "max_page_limit": 3}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    let writer_limit = page_limit_header(&writer, &user.raw_token, "?limit=9").await;
    let reader_before = page_limit_header(&reader, &user.raw_token, "?limit=9").await;
    reader.refresh_runtime_settings().await.unwrap();
    let reader_after = page_limit_header(&reader, &user.raw_token, "?limit=9").await;
    assert_eq!(
        (writer_limit, reader_before, reader_after),
        (Some("3".into()), Some("9".into()), Some("3".into()))
    );
}

#[actix_web::test]
async fn runtime_maximum_bounds_returned_rows() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let scope = prefix("pagination");
    let mut expected = Vec::new();
    for index in 0..5 {
        let collection = backend
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(format!("{scope}_{index}"), "", group_id(1), None),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        expected.push(collection.id().id());
    }
    commit(&backend, pagination_limits(1, 2)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::get().uri(&format!(
            "/api/v1/collections?limit=9&sort=id.asc&name__startswith={scope}"
        )),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let has_next = response
        .headers()
        .contains_key(crate::pagination::NEXT_CURSOR_HEADER);
    let body: Value = test::read_body_json(response).await;
    let ids = body
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_i64().unwrap() as i32)
        .collect::<Vec<_>>();
    assert_eq!((ids, has_next), (expected[..2].to_vec(), true));
}

#[rstest::rstest]
#[case::default_only(json!({"default_page_limit": 3}), 3, 10)]
#[case::maximum_only(json!({"max_page_limit": 8}), 4, 8)]
#[case::both(json!({"default_page_limit": 2, "max_page_limit": 3}), 2, 3)]
#[actix_web::test]
async fn pagination_patch_preserves_omitted_limits(
    #[case] changes: Value,
    #[case] default: u64,
    #[case] maximum: u64,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 10)).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/pagination")
            .set_json(json!({"expected_revision": 2, "changes": changes})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        backend.load_runtime_settings().await.unwrap().pagination(),
        Some(StoragePaginationPolicy::try_with_maximum(default, maximum).unwrap())
    );
}

#[rstest::rstest]
#[case::default_above_maximum(json!({"default_page_limit": 11}))]
#[case::maximum_below_default(json!({"max_page_limit": 3}))]
#[actix_web::test]
async fn pagination_patch_validates_the_combined_limits(#[case] changes: Value) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 10)).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::patch()
            .uri("/api/v1/admin/runtime-settings/pagination")
            .set_json(json!({"expected_revision": 2, "changes": changes})),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(
        history_len(&backend, StorageRuntimeSettingsGroup::Pagination).await,
        1
    );
}

#[actix_web::test]
async fn releasing_pagination_restores_the_deployment_maximum() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 8)).await;
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::delete()
            .uri("/api/v1/admin/runtime-settings/pagination?expected_revision=2"),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(
        body["settings"]["pagination"]["effective"]["max_page_limit"],
        deployment_page_limits().unwrap().maximum_limit()
    );
}

#[actix_web::test]
async fn default_only_put_restores_deployment_maximum_inheritance() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    commit(&backend, pagination_limits(4, 8)).await;
    let (status, body) = send_json(
        &backend,
        &user.raw_token,
        put(
            "/api/v1/admin/runtime-settings/pagination",
            json!({"expected_revision": 2, "default_page_limit": 5}),
        ),
    )
    .await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(
        body["settings"]["pagination"]["effective"]["max_page_limit"],
        deployment_page_limits().unwrap().maximum_limit()
    );
}

#[rstest::rstest]
#[case::runtime_default(None, false, 2)]
#[case::updated_before_body(None, true, 2)]
#[case::explicit_limit(Some(1), true, 1)]
#[actix_web::test]
async fn structured_search_stream_keeps_request_page_limits(
    #[case] requested_limit: Option<usize>,
    #[case] update_before_body: bool,
    #[case] expected_limit: usize,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    for index in 0..4 {
        backend
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(
                    format!("stream_collection_{index}"),
                    "runtime pagination stream fixture",
                    group_id(1),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
    commit(&backend, pagination(2)).await;
    backend.refresh_runtime_settings().await.unwrap();
    let mut request = json!({"version": 1, "target": {"kind": "collection"}});
    if let Some(limit) = requested_limit {
        request["limit"] = json!(limit);
    }
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::post()
            .uri("/api/v1/search/stream")
            .set_json(request),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::OK);

    // The handler has returned, but the search runs when its body is polled.
    if update_before_body {
        commit(&backend, pagination(3)).await;
        backend.refresh_runtime_settings().await.unwrap();
    }
    let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
    let done = body
        .split("\n\n")
        .find_map(|event| event.strip_prefix("event: done\ndata: "))
        .expect("stream should finish with pagination metadata");
    let done: Value = serde_json::from_str(done).unwrap();
    assert_eq!(
        (
            body.matches("event: result\n").count(),
            done["page_limit"].as_u64(),
            done["next"].is_string(),
        ),
        (expected_limit, Some(expected_limit as u64), true),
    );
}
