#[cfg(test)]
mod tests {
    use actix_web::{test, web, App, HttpResponse};
    use std::str::FromStr;

    use crate::config::ClientAllowlist;
    use crate::middlewares::ClientAllowlistMiddleware;

    async fn ok_handler() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

#[actix_web::test]
async fn test_allows_whitelisted_ipv4() {
    let app = test::init_service(
        App::new()
            .wrap(ClientAllowlistMiddleware::new_with_trust(
                ClientAllowlist::from_str("10.0.0.0/24").unwrap(),
                true,
            ))
            .route("/", web::get().to(ok_handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/")
        .insert_header(("x-forwarded-for", "10.0.0.42"))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
}

#[actix_web::test]
async fn test_rejects_non_whitelisted_ipv4() {
    let app = test::init_service(
        App::new()
            .wrap(ClientAllowlistMiddleware::new_with_trust(
                ClientAllowlist::from_str("10.0.0.0/24").unwrap(),
                true,
            ))
            .route("/", web::get().to(ok_handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/")
        .insert_header(("x-forwarded-for", "192.168.1.10"))
        .to_request();

    let resp = test::try_call_service(&app, req).await;
    assert!(resp.is_err());
    let err = resp.unwrap_err();
    assert_eq!(
        err.error_response().status(),
        actix_web::http::StatusCode::FORBIDDEN
    );
}

#[actix_web::test]
async fn test_allows_ipv6_in_range() {
    let app = test::init_service(
        App::new()
            .wrap(ClientAllowlistMiddleware::new_with_trust(
                ClientAllowlist::from_str("2001:db8::/32").unwrap(),
                true,
            ))
            .route("/", web::get().to(ok_handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/")
        .insert_header(("x-forwarded-for", "2001:db8::1"))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
}

#[actix_web::test]
async fn test_ignores_headers_when_trust_disabled() {
    let app = test::init_service(
        App::new()
            .wrap(ClientAllowlistMiddleware::new_with_trust(
                ClientAllowlist::from_str("10.0.0.0/24").unwrap(),
                false,
            ))
            .route("/", web::get().to(ok_handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/")
        .insert_header(("x-forwarded-for", "10.0.0.42"))
        .to_request();

    let resp = test::try_call_service(&app, req).await;
    assert!(resp.is_err());
}

#[actix_web::test]
async fn test_allows_all_when_wildcard() {
    let app = test::init_service(
        App::new()
            .wrap(ClientAllowlistMiddleware::new_with_trust(
                ClientAllowlist::from_str("*").unwrap(),
                true,
            ))
            .route("/", web::get().to(ok_handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/")
        .insert_header(("x-forwarded-for", "192.168.1.100"))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
}

// Unit tests for ClientAllowlist logic (no actix-web involved)
#[cfg(test)]
mod allowlist_unit_tests {
    use super::ClientAllowlist;
    use std::str::FromStr;

    #[::core::prelude::v1::test]
    fn test_parses_any() {
        let allowlist = ClientAllowlist::from_str("*").unwrap();
        assert!(allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)));
    }

    #[::core::prelude::v1::test]
    fn test_parses_default_hosts() {
        let allowlist = ClientAllowlist::from_str("127.0.0.1,::1").unwrap();
        assert!(allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)));
        assert!(allowlist.allows(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)));
    }

    #[::core::prelude::v1::test]
    fn test_rejects_outside_network() {
        let allowlist = ClientAllowlist::from_str("10.0.0.0/24").unwrap();
        assert!(!allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)));
    }

    #[::core::prelude::v1::test]
    fn test_errors_on_empty() {
        assert!(ClientAllowlist::from_str("").is_err());
        assert!(ClientAllowlist::from_str(",,,").is_err());
    }

    #[::core::prelude::v1::test]
    fn test_errors_on_invalid_ip() {
        assert!(ClientAllowlist::from_str("not-an-ip").is_err());
    }

    #[::core::prelude::v1::test]
    fn test_allows_multiple_cidrs() {
        let allowlist = ClientAllowlist::from_str("10.0.0.0/24,192.168.1.0/24").unwrap();
        assert!(allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 5))));
        assert!(allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 5))));
        assert!(!allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 16, 0, 1))));
    }

    #[::core::prelude::v1::test]
    fn test_allows_single_ip() {
        let allowlist = ClientAllowlist::from_str("192.168.1.100").unwrap();
        assert!(allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 100))));
        assert!(!allowlist.allows(std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 101))));
    }
    }
}
