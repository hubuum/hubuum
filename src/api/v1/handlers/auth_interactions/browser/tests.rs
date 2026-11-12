use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

use actix_web::{
    App,
    cookie::Cookie,
    dev::ServiceResponse,
    http::{StatusCode, header},
    test,
};
use hubuum_storage_postgres::{PostgresStorageError, diesel_async_prelude::*, with_connection};
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::tests::{TestContext, app_context};

const ENTRY: &str = "/api/v1/auth/interactions/browser";

struct Fixture {
    context: TestContext,
    config: Arc<crate::config::AppConfig>,
    client: String,
    begun: Value,
    peer: SocketAddr,
}
impl Fixture {
    async fn new(bff: bool) -> Self {
        let context = TestContext::new().await;
        let client = context.scoped_name("browser");
        let mut config = crate::config::get_config().unwrap().clone();
        config.auth_browser = json!({"origin":"https://auth.example","return_targets":[{"id":"frontend","client_id":client,"uri":"https://frontend.example/auth/callback"}]}).to_string().parse().unwrap();
        let mut f = Self {
            context,
            config: Arc::new(config),
            client,
            begun: Value::Null,
            peer: SocketAddr::new(IpAddr::V6(Ipv6Addr::from(*Uuid::new_v4().as_bytes())), 1234),
        };
        let mut body = json!({"client_id":f.client,"provider_id":"local","purpose":"login","delivery":"poll","interaction_mode":"browser"});
        if bff {
            body["delivery"] = json!("bff");
            body["return_target_id"] = json!("frontend");
            body["return_nonce"] = json!("test-return-nonce-01234567890123456789");
        }
        let response = f
            .call(
                test::TestRequest::post()
                    .uri("/api/v1/auth/interactions")
                    .set_json(body),
            )
            .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        f.begun = test::read_body_json(response).await;
        f
    }
    async fn call(&self, request: test::TestRequest) -> ServiceResponse {
        let app = test::init_service(
            App::new()
                .app_data(app_context(&self.context.pool))
                .configure(crate::api::config),
        )
        .await;
        crate::config::runtime::scope(self.config.clone(), async {
            request
                .insert_header((header::HOST, "auth.example"))
                .peer_addr(self.peer)
                .send_request(&app)
                .await
                .map_into_boxed_body()
        })
        .await
    }
    fn form(path: &str, body: Value, cookie: &str) -> test::TestRequest {
        test::TestRequest::post()
            .uri(path)
            .set_form(body)
            .insert_header((header::COOKIE, cookie))
            .insert_header((header::ORIGIN, "https://auth.example"))
            .insert_header(("sec-fetch-site", "same-origin"))
    }
    fn path(&self) -> String {
        format!("{ENTRY}/{}", self.begun["id"].as_str().unwrap())
    }
    async fn entry(&self) -> (String, String) {
        let response = self.call(test::TestRequest::get().uri(ENTRY)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = cookie(&response);
        let html = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        (cookie, csrf(&html))
    }
    async fn attach(&self) -> (String, String) {
        let (cookie, csrf) = self.entry().await;
        let response = self
            .call(Self::form(
                ENTRY,
                json!({"csrf":csrf,"user_code":self.begun["next"]["user_code"]}),
                &cookie,
            ))
            .await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        (
            crate_cookie(&response, self.begun["id"].as_str().unwrap()),
            csrf,
        )
    }
    async fn authenticate(&self) -> (String, String) {
        let (cookie, csrf) = self.attach().await;
        let name = self
            .context
            .normal_user
            .name(&self.context.pool)
            .await
            .unwrap();
        let response = self
            .call(Self::form(
                &format!("{}/credentials", self.path()),
                json!({"csrf":csrf,"name":name,"password":"testpassword"}),
                &cookie,
            ))
            .await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let cookie = crate_cookie(&response, self.begun["id"].as_str().unwrap());
        let response = self
            .call(
                test::TestRequest::get()
                    .uri(&self.path())
                    .insert_header((header::COOKIE, cookie.as_str())),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let html = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(html.contains("Approve this request?"));
        (cookie, self::csrf(&html))
    }
    async fn decide(&self, cookie: &str, csrf: &str, decision: &str) -> ServiceResponse {
        self.call(Self::form(
            &format!("{}/decision", self.path()),
            json!({"csrf":csrf,"decision":decision}),
            cookie,
        ))
        .await
    }
    async fn complete(&self) -> ServiceResponse {
        self.call(
            test::TestRequest::post()
                .uri(&format!(
                    "/api/v1/auth/interactions/{}/complete",
                    self.begun["id"].as_str().unwrap()
                ))
                .insert_header((
                    header::AUTHORIZATION,
                    format!(
                        "Hubuum-Interaction {}",
                        self.begun["redemption_secret"].as_str().unwrap()
                    ),
                ))
                .set_json(json!({"client_id":self.client})),
        )
        .await
    }
    async fn cleanup(self) {
        with_connection(&self.context.pool, async |connection| {
            diesel::sql_query("DELETE FROM auth_interactions WHERE client_id=$1")
                .bind::<diesel::sql_types::Text, _>(self.client)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
    }
}
fn crate_cookie(response: &ServiceResponse, suffix: &str) -> String {
    response
        .headers()
        .get_all(header::SET_COOKIE)
        .filter_map(|value| Cookie::parse(value.to_str().unwrap()).ok())
        .find(|cookie| cookie.name().ends_with(suffix))
        .map(|cookie| format!("{}={}", cookie.name(), cookie.value()))
        .unwrap()
}
fn cookie(response: &ServiceResponse) -> String {
    crate_cookie(response, "entry")
}
fn csrf(html: &str) -> String {
    html.split("name=\"csrf\" value=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .into()
}

#[actix_web::test]
async fn hosted_login_requires_explicit_approval_and_completes_once() {
    let f = Fixture::new(false).await;
    let (cookie, csrf) = f.authenticate().await;
    assert_eq!(f.complete().await.status(), StatusCode::CONFLICT);
    assert_eq!(
        f.decide(&cookie, &csrf, "approve").await.status(),
        StatusCode::OK
    );
    let response = f.complete().await;
    assert_eq!(response.status(), StatusCode::OK);
    let session: Value = test::read_body_json(response).await;
    assert!(session["access_token"].is_string());
    assert_eq!(f.complete().await.status(), StatusCode::CONFLICT);
    f.cleanup().await;
}

#[rstest]
#[case::wrong_cookie(true, false)]
#[case::wrong_csrf(false, true)]
#[actix_web::test]
async fn approval_rejects_wrong_browser_or_challenge(
    #[case] wrong_cookie: bool,
    #[case] wrong_csrf: bool,
) {
    let f = Fixture::new(false).await;
    let (mut cookie, mut csrf) = f.authenticate().await;
    if wrong_cookie {
        cookie.push('0');
    }
    if wrong_csrf {
        csrf.push('0');
    }
    assert_eq!(
        f.decide(&cookie, &csrf, "approve").await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(f.complete().await.status(), StatusCode::CONFLICT);
    f.cleanup().await;
}

#[actix_web::test]
async fn denial_never_issues_a_session() {
    let f = Fixture::new(false).await;
    let (cookie, csrf) = f.authenticate().await;
    assert_eq!(
        f.decide(&cookie, &csrf, "deny").await.status(),
        StatusCode::OK
    );
    assert_eq!(f.complete().await.status(), StatusCode::FORBIDDEN);
    f.cleanup().await;
}

#[actix_web::test]
async fn stale_credentials_form_cannot_be_used_for_approval() {
    let f = Fixture::new(false).await;
    let (cookie, old_csrf) = f.attach().await;
    let name = f.context.normal_user.name(&f.context.pool).await.unwrap();
    let response = f
        .call(Fixture::form(
            &format!("{}/credentials", f.path()),
            json!({"csrf":old_csrf,"name":name,"password":"testpassword"}),
            &cookie,
        ))
        .await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let new_cookie = crate_cookie(&response, f.begun["id"].as_str().unwrap());
    assert_eq!(
        f.decide(&new_cookie, &old_csrf, "approve").await.status(),
        StatusCode::UNAUTHORIZED
    );
    f.cleanup().await;
}

#[rstest]
#[case::missing(None)]
#[case::cross_origin(Some("https://attacker.example"))]
#[case::null(Some("null"))]
#[actix_web::test]
async fn code_attachment_requires_exact_origin(#[case] origin: Option<&str>) {
    let f = Fixture::new(false).await;
    let (cookie, csrf) = f.entry().await;
    let mut request = test::TestRequest::post()
        .uri(ENTRY)
        .set_form(json!({"csrf":csrf,"user_code":f.begun["next"]["user_code"]}))
        .insert_header((header::COOKIE, cookie));
    if let Some(origin) = origin {
        request = request.insert_header((header::ORIGIN, origin));
    }
    assert_eq!(f.call(request).await.status(), StatusCode::UNAUTHORIZED);
    f.cleanup().await;
}

#[actix_web::test]
async fn another_browser_cannot_replace_the_first_attachment() {
    let f = Fixture::new(false).await;
    f.attach().await;
    let (cookie, csrf) = f.entry().await;
    assert_eq!(
        f.call(Fixture::form(
            ENTRY,
            json!({"csrf":csrf,"user_code":f.begun["next"]["user_code"]}),
            &cookie
        ))
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    f.cleanup().await;
}

#[actix_web::test]
async fn bff_redirect_contains_only_bound_id_and_nonce() {
    let f = Fixture::new(true).await;
    let (cookie, csrf) = f.authenticate().await;
    let response = f.decide(&cookie, &csrf, "approve").await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let url = reqwest::Url::parse(
        response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        url.origin().ascii_serialization(),
        "https://frontend.example"
    );
    let pairs: Vec<_> = url.query_pairs().collect();
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].0, "interaction_id");
    assert_eq!(pairs[0].1, f.begun["id"].as_str().unwrap());
    assert_eq!(pairs[1].0, "nonce");
    assert_eq!(pairs[1].1, "test-return-nonce-01234567890123456789");
    f.cleanup().await;
}

#[actix_web::test]
async fn entry_uses_secure_host_only_cookie_and_script_free_html() {
    let f = Fixture::new(false).await;
    let response = f.call(test::TestRequest::get().uri(ENTRY)).await;
    let raw = response
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    let cookie = Cookie::parse(raw).unwrap();
    assert!(cookie.name().starts_with("__Host-"));
    assert_eq!(cookie.http_only(), Some(true));
    assert_eq!(cookie.secure(), Some(true));
    assert_eq!(cookie.same_site(), Some(actix_web::cookie::SameSite::Lax));
    assert_eq!(cookie.path(), Some("/"));
    assert!(cookie.domain().is_none());
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    assert_eq!(
        response.headers().get(header::REFERRER_POLICY).unwrap(),
        "same-origin"
    );
    let html = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
    assert!(!html.contains("<script"));
    assert!(!html.contains(f.begun["redemption_secret"].as_str().unwrap()));
    f.cleanup().await;
}

#[rstest]
#[case::approved(false)]
#[case::tampered(true)]
#[actix_web::test]
async fn credential_approval_is_bound_to_original_operation(#[case] tampered: bool) {
    let mut f = Fixture::new(false).await;
    let response = f.call(test::TestRequest::post().uri("/api/v1/auth/interactions")
        .insert_header((header::AUTHORIZATION, format!("Bearer {}", f.context.normal_token)))
        .set_json(json!({"client_id":f.client, "provider_id":"local", "purpose":"credential_approval", "delivery":"poll", "interaction_mode":"browser", "operation":{"kind":"create_token","principal_id":f.context.normal_user.id, "token":{"name":"browser-approved-token"}}}))).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    f.begun = test::read_body_json(response).await;
    let (cookie, csrf) = f.authenticate().await;
    assert_eq!(
        f.decide(&cookie, &csrf, "approve").await.status(),
        StatusCode::OK
    );
    let mut operation = json!({"kind":"create_token","principal_id":f.context.normal_user.id,"token":{"name":"browser-approved-token"}});
    if tampered {
        operation["token"]["name"] = json!("different-token");
    }
    let response = f
        .call(
            test::TestRequest::post()
                .uri(&format!(
                    "/api/v1/auth/interactions/{}/complete",
                    f.begun["id"].as_str().unwrap()
                ))
                .insert_header((
                    header::AUTHORIZATION,
                    format!(
                        "Hubuum-Interaction {}",
                        f.begun["redemption_secret"].as_str().unwrap()
                    ),
                ))
                .set_json(if tampered {
                    json!({"client_id":f.client,"operation":operation})
                } else {
                    json!({"client_id":f.client})
                }),
        )
        .await;
    assert_eq!(
        response.status(),
        if tampered {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::OK
        }
    );
    if !tampered {
        let approval: Value = test::read_body_json(response).await;
        assert!(approval["approval"].is_string());
        assert!(approval.get("access_token").is_none());
    }
    f.cleanup().await;
}

#[actix_web::test]
async fn changed_return_registration_prevents_browser_approval() {
    let mut f = Fixture::new(true).await;
    let (cookie, csrf) = f.authenticate().await;
    Arc::make_mut(&mut f.config).auth_browser = json!({"origin":"https://auth.example","return_targets":[{"id":"frontend","client_id":f.client,"uri":"https://other.example/callback"}]}).to_string().parse().unwrap();
    assert_eq!(
        f.decide(&cookie, &csrf, "approve").await.status(),
        StatusCode::CONFLICT
    );
    f.cleanup().await;
}
