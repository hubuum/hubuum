use std::sync::LazyLock;

use actix_web::{
    HttpResponse,
    http::{StatusCode, header},
};
use hubuum_domain::InteractionPurpose;
use hubuum_storage_core::StorageInteractionView;
use minijinja::{Environment, context, value::Serde};
use serde::Serialize;

static TEMPLATES: LazyLock<Environment<'static>> = LazyLock::new(|| {
    let mut environment = Environment::new();
    environment.set_fuel(Some(10_000));
    environment
        .add_template("browser.html", include_str!("browser.html"))
        .expect("compiled browser template");
    environment
});

#[derive(Serialize)]
pub(super) struct RequestDisplay {
    id: String,
    client: String,
    provider: String,
    purpose: &'static str,
    details: Option<String>,
    #[serde(skip)]
    return_origin: Option<String>,
}
impl RequestDisplay {
    pub(super) fn new(view: &StorageInteractionView) -> Self {
        Self {
            id: view.binding().id().value().to_string(),
            client: view.binding().client().as_str().to_owned(),
            provider: view
                .binding()
                .prompt()
                .map(|p| p.provider().as_str().to_owned())
                .unwrap_or_else(|| view.binding().provider().into()),
            details: view.binding().prompt().map(|p| p.details().to_owned()),
            return_origin: view
                .binding()
                .return_target()
                .and_then(|t| reqwest::Url::parse(t.uri()).ok())
                .map(|u| u.origin().ascii_serialization()),
            purpose: match view.binding().authority().purpose() {
                InteractionPurpose::Login => "Sign in",
                InteractionPurpose::CredentialApproval => "Approve a credential operation",
                InteractionPurpose::ProviderVerification => {
                    "Test an authentication provider (no session will be issued)"
                }
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub(super) enum BrowserPage<'a> {
    Entry {
        csrf: &'a str,
    },
    Credentials {
        request: RequestDisplay,
        csrf: &'a str,
    },
    Approval {
        request: RequestDisplay,
        csrf: &'a str,
        name: &'a str,
    },
    Complete {
        approved: bool,
    },
    Error {
        message: &'a str,
    },
}
impl BrowserPage<'_> {
    fn title(&self) -> &'static str {
        match self {
            Self::Entry { .. } => "Connect your application",
            Self::Credentials { .. } => "Sign in",
            Self::Approval { .. } => "Approve this request?",
            Self::Complete { approved: true } => "Request approved",
            Self::Complete { approved: false } => "Request denied",
            Self::Error { .. } => "Authentication stopped",
        }
    }
}

pub(super) fn response(status: StatusCode, page: BrowserPage<'_>) -> HttpResponse {
    // Browsers apply form-action to the POST's redirect chain as well. Only the
    // registered target origin is allowed; the service rechecks the exact URI.
    let return_origin = match &page {
        BrowserPage::Approval { request, .. } => request.return_origin.as_deref().unwrap_or(""),
        _ => "",
    };
    let policy = format!(
        "default-src 'none'; style-src 'unsafe-inline'; form-action 'self' {return_origin}; frame-ancestors 'none'; base-uri 'none'"
    );
    let html = TEMPLATES
        .get_template("browser.html")
        .expect("compiled browser template")
        .render(context! { title => page.title(), page => Serde(&page) });
    let (status, body) = match html {
        Ok(body) => (status, body),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Authentication page unavailable".into(),
        ),
    };
    HttpResponse::build(status)
        .content_type("text/html; charset=utf-8")
        .insert_header((header::CACHE_CONTROL, "no-store"))
        // Native POST forms suppress Origin under no-referrer. Same-origin
        // preserves the strict Origin check without sending cross-origin referrers.
        .insert_header((header::REFERRER_POLICY, "same-origin"))
        .insert_header(("Content-Security-Policy", policy))
        .insert_header(("X-Content-Type-Options", "nosniff"))
        .body(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn approval_details_are_html_escaped() {
        let response = response(
            StatusCode::OK,
            BrowserPage::Approval {
                request: RequestDisplay {
                    id: "test".into(),
                    client: "client".into(),
                    provider: "local".into(),
                    purpose: "Approve a credential operation",
                    details: Some("<script>alert(1)</script>".into()),
                    return_origin: None,
                },
                csrf: "challenge",
                name: "<img src=x onerror=alert(1)>",
            },
        );
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let html = std::str::from_utf8(&body).unwrap();
        assert!(!html.contains("<script>"));
        assert!(!html.contains("<img"));
        assert!(html.contains("&lt;script&gt;"));
    }
}
