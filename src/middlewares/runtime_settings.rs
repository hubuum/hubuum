//! Apply runtime-managed settings at the start of each HTTP request.
use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{Error, HttpMessage, web::Data};
use hubuum_storage_core::StorageRuntimeSettingsGroup;

use crate::errors::ApiError;
use crate::pagination::{PageLimits, with_request_page_limits};
use crate::permissions::AppContext;
use crate::storage::runtime_settings::AppliedRuntimeSettings;

/// Capture policy before tracing resolves client attribution and metrics labels.
/// Rejection belongs to `apply_runtime_settings`, inside HTTP instrumentation.
pub async fn resolve_runtime_settings(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    with_runtime_settings(req, next, false).await
}

/// Fix the request's page limits from the settings this replica applies now.
///
/// Changes committed while the request runs take effect on the next request.
/// Without an application context, deployment configuration applies.
pub async fn apply_runtime_settings(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    with_runtime_settings(req, next, true).await
}

async fn with_runtime_settings(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
    reject_stale: bool,
) -> Result<ServiceResponse<BoxBody>, Error> {
    if matches!(req.path(), "/healthz" | "/readyz") {
        return Ok(next.call(req).await?.map_into_boxed_body());
    }
    let applied = req
        .extensions()
        .get::<AppliedRuntimeSettings>()
        .cloned()
        .or_else(|| {
            req.app_data::<Data<AppContext>>()
                .map(|context| context.backend().applied_runtime_settings())
        });
    let config = match crate::config::runtime::scoped() {
        Some(config) => config,
        None => match applied.as_ref() {
            Some(applied) => crate::config::runtime::for_snapshot(applied.snapshot())?,
            None => crate::config::runtime::current()?,
        },
    };
    // Scrapes use the last applied policy so process metrics and cached gauges
    // remain available during reconciliation failures. The scoped network policy
    // still reaches the proxy and client-allowlist middleware below.
    let is_metrics = config.metrics_enabled && req.path() == config.metrics_path.as_str();
    if let Some(applied) = &applied
        && !is_metrics
        && reject_stale
    {
        applied.ensure_security_current().map_err(ApiError::from)?;
    }
    let network_managed = config.runtime_manages(StorageRuntimeSettingsGroup::Network);
    let deployment_proxy = (!network_managed)
        .then(|| req.app_data::<Data<super::ProxyTrust>>())
        .flatten();
    let proxy_trust = deployment_proxy.map_or_else(
        || {
            super::ProxyTrust::new(
                config.trust_ip_headers,
                config.trusted_proxies.nets().to_vec(),
                config.trusted_proxy_hops,
            )
        },
        |policy| policy.get_ref().clone(),
    );
    req.extensions_mut().insert(proxy_trust);
    if let Some(applied) = applied {
        req.extensions_mut().insert(applied);
    }
    let limits = PageLimits::new(config.default_page_limit, config.max_page_limit)?;
    Ok(crate::config::runtime::scope(config, async {
        with_request_page_limits(limits, next.call(req)).await
    })
    .await?
    .map_into_boxed_body())
}
