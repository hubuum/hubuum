use crate::api as prod_api;
use crate::backups::BackupSettings;
use crate::config::{
    DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER, DEFAULT_BACKUP_MAX_CAPTURE_ROWS,
    DEFAULT_BACKUP_MAX_OUTPUT_BYTES, DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS,
    DEFAULT_RESTORE_MAX_UPLOAD_BYTES, DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
};
use crate::middlewares::tracing::TracingMiddleware;
use crate::permissions::{AppContext, LocalPermissionBackend, PermissionBackend};
use crate::restores::RestoreSettings;
use crate::storage::StorageHandle;
use actix_web::{App, http, test, web::Data};
use hubuum_storage_core::StorageBackupBudget;
use hubuum_storage_postgres::{PostgresPool, PostgresPoolSettings};
use serde::Serialize;
use std::sync::Arc;

pub fn app_context(pool: &PostgresPool) -> Data<AppContext> {
    let config = crate::tests::integration_test_config()
        .expect("integration test configuration must be valid");
    let permissions = LocalPermissionBackend::new(
        test_storage_handle(pool.clone()),
        config.admin_groupname.clone(),
    );
    Data::new(app_context_with_permission_backend(
        pool.clone(),
        Arc::new(permissions),
    ))
}

pub fn app_context_with_permission_backend(
    pool: PostgresPool,
    permissions: Arc<dyn PermissionBackend>,
) -> AppContext {
    AppContext::new(test_storage_handle(pool), permissions)
}

fn test_storage_handle(pool: PostgresPool) -> StorageHandle {
    let config = crate::tests::integration_test_config()
        .expect("integration test configuration must be valid");
    let operational_pool_settings = PostgresPoolSettings::builder(config.database_url.clone())
        .max_size(1)
        .statement_timeout_ms(config.db_statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .expect("test notification listener settings must be valid");
    StorageHandle::postgres_with_operational_pool_settings(pool, operational_pool_settings)
}

fn create_token_header(token: &str) -> (http::header::HeaderName, String) {
    (http::header::AUTHORIZATION, format!("Bearer {token}"))
}

fn backup_settings() -> BackupSettings {
    BackupSettings::new(
        DEFAULT_BACKUP_OUTPUT_RETENTION_HOURS,
        DEFAULT_BACKUP_MAX_ACTIVE_TASKS_PER_USER,
        StorageBackupBudget::new(
            DEFAULT_BACKUP_MAX_OUTPUT_BYTES,
            DEFAULT_BACKUP_MAX_CAPTURE_ROWS,
        )
        .unwrap(),
    )
    .expect("default backup settings must be valid")
}

fn restore_settings() -> RestoreSettings {
    RestoreSettings::new(
        DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
        DEFAULT_RESTORE_MAX_UPLOAD_BYTES,
    )
    .expect("default restore settings must be valid")
}

pub async fn get_request_with_correlation(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    correlation_id: Option<&str>,
) -> actix_web::dev::ServiceResponse {
    let headers = correlation_id
        .map(|value| {
            vec![(
                http::header::HeaderName::from_static("x-correlation-id"),
                value.to_string(),
            )]
        })
        .unwrap_or_default();
    get_request_with_headers(pool, token, endpoint, headers).await
}

pub async fn get_request_with_headers(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    headers: Vec<(http::header::HeaderName, String)>,
) -> actix_web::dev::ServiceResponse {
    get_request_with_headers_and_context(pool, token, endpoint, headers, app_context(pool)).await
}

async fn get_request_with_headers_and_context(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    headers: Vec<(http::header::HeaderName, String)>,
    context: Data<AppContext>,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(backup_settings()))
            .app_data(Data::new(restore_settings()))
            .app_data(Data::new(pool.clone()))
            .app_data(context)
            .configure(prod_api::config),
    )
    .await;

    let mut request = test::TestRequest::get()
        .insert_header(create_token_header(token))
        .uri(endpoint);
    for (name, value) in headers {
        request = request.insert_header((name, value));
    }
    request.send_request(&app).await.map_into_boxed_body()
}

pub async fn get_request(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    get_request_with_correlation(pool, token, endpoint, None).await
}

pub async fn get_request_with_permission_backend(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    permissions: Arc<dyn PermissionBackend>,
) -> actix_web::dev::ServiceResponse {
    let context = Data::new(app_context_with_permission_backend(
        pool.clone(),
        permissions,
    ));
    get_request_with_headers_and_context(pool, token, endpoint, Vec::new(), context).await
}

pub async fn post_request_with_headers<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
    headers: Vec<(http::header::HeaderName, String)>,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(backup_settings()))
            .app_data(Data::new(restore_settings()))
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    let mut req = test::TestRequest::post()
        .insert_header(create_token_header(token))
        .uri(endpoint);

    for (name, value) in headers {
        req = req.insert_header((name, value));
    }

    req.set_json(&content)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn post_request<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    post_request_with_headers(pool, token, endpoint, content, vec![]).await
}

pub async fn post_request_with_permission_backend<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
    permissions: Arc<dyn PermissionBackend>,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(backup_settings()))
            .app_data(Data::new(restore_settings()))
            .app_data(Data::new(pool.clone()))
            .app_data(Data::new(app_context_with_permission_backend(
                pool.clone(),
                permissions,
            )))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::post()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn delete_request(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::delete()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn patch_request<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::patch()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content) // Make sure to reference content
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn patch_request_with_headers<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
    headers: Vec<(http::header::HeaderName, String)>,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    let mut request = test::TestRequest::patch()
        .insert_header(create_token_header(token))
        .uri(endpoint);
    for (name, value) in headers {
        request = request.insert_header((name, value));
    }
    request
        .set_json(&content)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn patch_request_with_content_type<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
    content_type: &str,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let body = serde_json::to_vec(&content).expect("request content must serialize as JSON");
    patch_request_with_raw_body(pool, token, endpoint, body, content_type).await
}

pub async fn patch_request_with_raw_body(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    body: impl Into<actix_web::web::Bytes>,
    content_type: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::patch()
        .insert_header(create_token_header(token))
        .insert_header((http::header::CONTENT_TYPE, content_type))
        .uri(endpoint)
        .set_payload(body)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

pub async fn put_request<T>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse
where
    T: Serialize,
{
    let app = test::init_service(
        App::new()
            .wrap(actix_web::middleware::from_fn(
                crate::middlewares::actor_context,
            ))
            .wrap(TracingMiddleware::new())
            .app_data(Data::new(pool.clone()))
            .app_data(app_context(pool))
            .configure(prod_api::config),
    )
    .await;

    test::TestRequest::put()
        .insert_header(create_token_header(token))
        .uri(endpoint)
        .set_json(&content)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

/// Explicit two-step client helper for integration tests using standard fixtures.
/// Raw request helpers remain available for missing/invalid approval tests.
pub async fn credential_post_request<T: Serialize>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse {
    credential_post_request_with_headers(pool, token, endpoint, content, vec![]).await
}
pub async fn credential_post_request_with_headers<T: Serialize>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
    mut headers: Vec<(http::header::HeaderName, String)>,
) -> actix_web::dev::ServiceResponse {
    let mut body = serde_json::to_value(content).unwrap();
    if let Some(operation) = credential_operation_for_request("POST", endpoint, &body)
        && let Some(result) = fixture_credential_approval(pool, token, operation).await
    {
        let approval = match result {
            Ok(approval) => approval,
            Err(response) => return response,
        };
        if !approval["token_expires_at"].is_null() {
            body["expires_at"] = approval["token_expires_at"].clone();
        }
        headers.push((
            http::header::HeaderName::from_static("x-hubuum-credential-approval"),
            approval["approval"].as_str().unwrap().to_string(),
        ));
    }
    post_request_with_headers(pool, token, endpoint, body, headers).await
}
pub async fn credential_patch_request<T: Serialize>(
    pool: &PostgresPool,
    token: &str,
    endpoint: &str,
    content: T,
) -> actix_web::dev::ServiceResponse {
    let body = serde_json::to_value(content).unwrap();
    let mut headers = vec![];
    if let Some(operation) = credential_operation_for_request("PATCH", endpoint, &body)
        && let Some(result) = fixture_credential_approval(pool, token, operation).await
    {
        let approval = match result {
            Ok(approval) => approval,
            Err(response) => return response,
        };
        headers.push((
            http::header::HeaderName::from_static("x-hubuum-credential-approval"),
            approval["approval"].as_str().unwrap().to_string(),
        ));
    }
    patch_request_with_headers(pool, token, endpoint, body, headers).await
}
fn credential_operation_for_request(
    method: &str,
    endpoint: &str,
    body: &serde_json::Value,
) -> Option<serde_json::Value> {
    use serde_json::json;
    let segments = endpoint
        .trim_end_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    match (method, segments.as_slice()) {
        ("POST", ["", "api", "v1", "iam", "principals", principal, "tokens"]) => Some(
            json!({"kind":"create_token", "principal_id":principal.parse::<i32>().ok()?, "token":body}),
        ),
        (
            "POST",
            [
                "",
                "api",
                "v1",
                "iam",
                "principals",
                principal,
                "tokens",
                token_id,
                "renew",
            ],
        ) => Some(
            json!({"kind":"renew_token", "principal_id":principal.parse::<i32>().ok()?, "token_id":token_id.parse::<i32>().ok()?, "token":body}),
        ),
        ("POST", ["", "api", "v1", "iam", "users"]) => {
            Some(json!({"kind":"create_user", "user":body}))
        }
        ("PATCH", ["", "api", "v1", "iam", "users", user_id])
            if body.get("password").is_some_and(|v| !v.is_null()) =>
        {
            Some(json!({"kind":"update_user", "user_id":user_id.parse::<i32>().ok()?, "user":body}))
        }
        ("POST", ["", "api", "v1", "imports"]) => {
            let request =
                serde_json::from_value::<crate::models::ImportRequest>(body.clone()).ok()?;
            request
                .contains_credentials()
                .then(|| json!({"kind":"import_credentials","import":body}))
        }
        ("POST", ["", "api", "v1", "restores", restore_id, "confirm"]) => Some(
            json!({"kind":"confirm_restore", "restore_id":restore_id.parse::<i64>().ok()?, "confirmation":body}),
        ),
        _ => None,
    }
}
async fn fixture_credential_approval(
    pool: &PostgresPool,
    token: &str,
    operation: serde_json::Value,
) -> Option<Result<serde_json::Value, actix_web::dev::ServiceResponse>> {
    use hubuum_storage_postgres::diesel_async_prelude::*;
    let digest = crate::models::Token::storage_hash_from_raw(token);
    let password_hash = hubuum_storage_postgres::with_connection(pool, async |connection| {
        crate::schema::tokens::table
            .inner_join(
                crate::schema::users::table
                    .on(crate::schema::users::id.eq(crate::schema::tokens::principal_id)),
            )
            .filter(crate::schema::tokens::token.eq(digest))
            .select(crate::schema::users::password)
            .first::<Option<String>>(connection)
            .await
            .optional()
    })
    .await
    .ok()???;
    let password = if password_hash == *super::TEST_ADMIN_PASSWORD_HASH {
        "testadminpassword"
    } else if password_hash == *super::TEST_USER_PASSWORD_HASH {
        "testpassword"
    } else {
        return None;
    };
    let response = post_request(
        pool,
        token,
        "/api/v1/iam/credential-approvals",
        serde_json::json!({"password":password,"operation":operation}),
    )
    .await;
    if response.status() == http::StatusCode::CREATED {
        Some(Ok(test::read_body_json(response).await))
    } else {
        Some(Err(response))
    }
}
