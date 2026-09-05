use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actix_web::http::header::HeaderMap;
use hubuum_events_core::TraceLink;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{
    Link, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState,
    TracerProvider as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, IdGenerator, RandomIdGenerator, Sampler,
    SamplingResult, SdkTracerProvider, ShouldSample, Span as SdkSpan, SpanData, SpanExporter,
    SpanProcessor,
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::{AppConfig, TracingSamplingMode};
use crate::utilities::bounded_file::{
    MAX_CERTIFICATE_BUNDLE_BYTES, MAX_PRIVATE_KEY_BYTES, read_bounded_regular_file,
};

const MAX_OTLP_HEADERS: usize = 16;
const MAX_OTLP_HEADERS_BYTES: usize = 8 * 1024;
const MAX_TRACE_HEADER_BYTES: usize = 512;
const MAX_TRACESTATE_MEMBERS: usize = 32;
pub(crate) const MAX_QUEUE_CAPACITY: usize = 65_536;
pub(crate) const MAX_BATCH_SIZE: usize = 8_192;
pub(crate) const MAX_TIMEOUT_MS: u64 = 60_000;
pub(crate) const SAMPLE_RATIO_BOUNDS: std::ops::RangeInclusive<f64> = 0.0..=1.0;
const MAX_RESOURCE_VALUE_BYTES: usize = 128;

const HTTP_SERVER_ATTRIBUTES: &[&str] = &[
    "http.request.method",
    "http.route",
    "http.response.status_code",
    "client.network.category",
    "auth.principal.kind",
    "request_id",
    "correlation_id",
    "error.type",
];
const TASK_ATTRIBUTES: &[&str] = &["task.kind", "task.outcome", "task.attempt", "error.type"];
const EVENT_ATTRIBUTES: &[&str] = &[
    "sink.kind",
    "delivery.attempt",
    "delivery.outcome",
    "fanout.outcome",
    "error.type",
];
const HTTP_CLIENT_ATTRIBUTES: &[&str] = &[
    "http.request.method",
    "http.response.status_code",
    "http.response.body.size",
    "server.address.category",
    "error.type",
];
const DATABASE_ATTRIBUTES: &[&str] = &[
    "db.operation.category",
    "db.connection.mode",
    "db.caller",
    "db.duration_ms",
    "db.result",
];
const STORAGE_ATTRIBUTES: &[&str] = &[
    "backend",
    "capability",
    "operation",
    "storage.result",
    "storage.duration_ms",
    "error.type",
];
const AUTHORIZATION_ATTRIBUTES: &[&str] = &[
    "authorization.backend",
    "authorization.operation",
    "authorization.request.count",
    "authorization.result",
    "authorization.duration_ms",
];
const AUTHENTICATION_ATTRIBUTES: &[&str] = &[
    "auth.token.format",
    "auth.token.key_state",
    "auth.provider.kind",
    "auth.operation",
    "auth.result",
];

/// Final data-classification boundary for exported spans.
///
/// Application logs intentionally carry richer operational identifiers, but
/// only closed catalog spans and explicitly allowlisted attributes may cross
/// the OTLP boundary. Span events are excluded because arbitrary log fields can
/// contain resource names or user-controlled strings.
#[derive(Debug)]
struct ClassifiedSpanExporter<E> {
    inner: E,
    pending: Option<Arc<AtomicUsize>>,
}

impl<E> ClassifiedSpanExporter<E> {
    #[cfg(test)]
    const fn new(inner: E) -> Self {
        Self {
            inner,
            pending: None,
        }
    }

    fn with_pending(inner: E, pending: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            pending: Some(pending),
        }
    }
}

impl<E> SpanExporter for ClassifiedSpanExporter<E>
where
    E: SpanExporter,
{
    fn export(
        &self,
        mut batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let received = batch.len();
        if let Some(pending) = &self.pending {
            let previous = pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(received))
                })
                .expect("saturating queue-depth update always succeeds");
            crate::observability::metrics::trace_queue_utilization(
                previous.saturating_sub(received),
            );
        }
        batch.retain_mut(|span| {
            let Some((_, allowed)) = classified_span(&span.name) else {
                return false;
            };
            span.attributes
                .retain(|attribute| allowed.contains(&attribute.key.as_str()));
            span.events.events.clear();
            for link in &mut span.links.links {
                link.attributes.clear();
            }
            true
        });
        let dropped = received.saturating_sub(batch.len());
        if dropped > 0 {
            crate::observability::metrics::trace_spans_dropped("classification", dropped);
        }
        async move {
            if batch.is_empty() {
                Ok(())
            } else {
                let span_count = batch.len();
                let result = self.inner.export(batch).await.map_err(|_| {
                    OTelSdkError::InternalFailure("OTLP trace export failed".to_string())
                });
                crate::observability::metrics::trace_export_batch(
                    if result.is_ok() { "success" } else { "failure" },
                    span_count,
                );
                result
            }
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// Adds observable, fail-open admission in front of the SDK batch processor.
///
/// The SDK owns the actual bounded channel but does not expose its queue depth
/// or dropped-span counter. Reserving the same capacity here gives Hubuum exact
/// low-cardinality saturation metrics without blocking application threads.
#[derive(Debug)]
struct MeteredBatchSpanProcessor {
    inner: BatchSpanProcessor,
    pending: Arc<AtomicUsize>,
    capacity: usize,
}

impl SpanProcessor for MeteredBatchSpanProcessor {
    fn on_start(&self, span: &mut SdkSpan, context: &Context) {
        if let Some(data) = span.exported_data()
            && let Some((category, _)) = classified_span(&data.name)
        {
            crate::observability::metrics::trace_span_lifecycle(category, "started", 1);
        }
        self.inner.on_start(span, context);
    }

    fn on_end(&self, span: SpanData) {
        let Some((category, _)) = classified_span(&span.name) else {
            crate::observability::metrics::trace_spans_dropped("classification", 1);
            return;
        };
        crate::observability::metrics::trace_span_lifecycle(category, "ended", 1);
        let reservation =
            self.pending
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current < self.capacity).then_some(current + 1)
                });
        let Ok(previous) = reservation else {
            crate::observability::metrics::trace_spans_dropped("queue_saturation", 1);
            return;
        };
        crate::observability::metrics::trace_queue_utilization(previous + 1);
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

fn classified_span(name: &str) -> Option<(&'static str, &'static [&'static str])> {
    match name {
        "http.server.request" => Some(("http", HTTP_SERVER_ATTRIBUTES)),
        "task.admission" | "task.execute" => Some(("task", TASK_ATTRIBUTES)),
        "event.fanout" | "event.delivery" => Some(("event", EVENT_ATTRIBUTES)),
        "http.client.request" => Some(("outbound", HTTP_CLIENT_ATTRIBUTES)),
        "db.connection" | "db.operation" => Some(("database", DATABASE_ATTRIBUTES)),
        "storage_operation" => Some(("storage", STORAGE_ATTRIBUTES)),
        "authz.permission_backend" | "authz.scope_intersection" => {
            Some(("authorization", AUTHORIZATION_ATTRIBUTES))
        }
        "auth.token_validation" | "auth.provider" | "auth.identity_refresh" => {
            Some(("authentication", AUTHENTICATION_ATTRIBUTES))
        }
        _ => None,
    }
}

#[must_use]
pub(crate) fn is_catalog_span(name: &str) -> bool {
    classified_span(name).is_some()
}

#[derive(Clone)]
pub struct TracingSettings {
    enabled: bool,
    endpoint: Option<String>,
    headers: HashMap<String, String>,
    ca_cert_path: Option<String>,
    client_identity_paths: Option<(String, String)>,
    connect_timeout: Duration,
    export_timeout: Duration,
    flush_timeout: Duration,
    queue_capacity: usize,
    batch_size: usize,
    sampling_mode: TracingSamplingMode,
    sample_ratio: f64,
    service_name: String,
    service_namespace: String,
    deployment_environment: String,
    runtime_role: String,
    trust_incoming_sampling: bool,
    propagate_outbound: bool,
}

impl fmt::Debug for TracingSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TracingSettings")
            .field("enabled", &self.enabled)
            .field("endpoint_configured", &self.endpoint.is_some())
            .field("header_count", &self.headers.len())
            .field("ca_cert_configured", &self.ca_cert_path.is_some())
            .field(
                "client_cert_configured",
                &self.client_identity_paths.is_some(),
            )
            .field(
                "client_key_configured",
                &self.client_identity_paths.is_some(),
            )
            .field("connect_timeout", &self.connect_timeout)
            .field("export_timeout", &self.export_timeout)
            .field("flush_timeout", &self.flush_timeout)
            .field("queue_capacity", &self.queue_capacity)
            .field("batch_size", &self.batch_size)
            .field("sampling_mode", &self.sampling_mode)
            .field("sample_ratio", &self.sample_ratio)
            .field("service_name", &self.service_name)
            .field("service_namespace", &self.service_namespace)
            .field("deployment_environment", &self.deployment_environment)
            .field("runtime_role", &self.runtime_role)
            .field("trust_incoming_sampling", &self.trust_incoming_sampling)
            .field("propagate_outbound", &self.propagate_outbound)
            .finish()
    }
}

impl TracingSettings {
    pub fn from_config(config: &AppConfig) -> Result<Self, String> {
        use crate::config::environment::constraints;

        validate_timeout(
            "tracing_connect_timeout_ms",
            config.tracing_connect_timeout_ms,
        )?;
        validate_timeout(
            "tracing_export_timeout_ms",
            config.tracing_export_timeout_ms,
        )?;
        validate_timeout("tracing_flush_timeout_ms", config.tracing_flush_timeout_ms)?;
        if !(1..=MAX_QUEUE_CAPACITY).contains(&config.tracing_queue_capacity) {
            return Err(format!(
                "tracing_queue_capacity must be between 1 and {MAX_QUEUE_CAPACITY}"
            ));
        }
        if !(1..=MAX_BATCH_SIZE).contains(&config.tracing_batch_size)
            || !constraints::TRACING_BATCH_SIZE
                .ordered_values_satisfy(config.tracing_batch_size, config.tracing_queue_capacity)
        {
            return Err(format!(
                "tracing_batch_size must be between 1 and {MAX_BATCH_SIZE} and no larger than tracing_queue_capacity"
            ));
        }
        if !config.tracing_sample_ratio.is_finite()
            || !SAMPLE_RATIO_BOUNDS.contains(&config.tracing_sample_ratio)
        {
            return Err("tracing_sample_ratio must be a finite value between 0 and 1".to_string());
        }
        validate_resource_value("tracing_service_name", &config.tracing_service_name)?;
        validate_resource_value(
            "tracing_service_namespace",
            &config.tracing_service_namespace,
        )?;
        validate_resource_value(
            "tracing_deployment_environment",
            &config.tracing_deployment_environment,
        )?;

        let endpoint = config
            .tracing_otlp_endpoint
            .as_deref()
            .map(validate_endpoint)
            .transpose()?;
        if !constraints::TRACING_ENDPOINT
            .requirement_is_satisfied(config.tracing_enabled, endpoint.is_some())
        {
            return Err("tracing_otlp_endpoint is required when tracing is enabled".to_string());
        }
        let client_identity_paths = constraints::TRACING_KEY_PAIR
            .resolve(
                config.tracing_otlp_client_cert.clone(),
                config.tracing_otlp_client_key.clone(),
            )
            .map_err(|_| {
                "tracing_otlp_client_cert and tracing_otlp_client_key must be configured together"
                    .to_string()
            })?;

        Ok(Self {
            enabled: config.tracing_enabled,
            endpoint,
            headers: parse_headers(config.tracing_otlp_headers.as_deref())?,
            ca_cert_path: config.tracing_otlp_ca_cert.clone(),
            client_identity_paths,
            connect_timeout: Duration::from_millis(config.tracing_connect_timeout_ms),
            export_timeout: Duration::from_millis(config.tracing_export_timeout_ms),
            flush_timeout: Duration::from_millis(config.tracing_flush_timeout_ms),
            queue_capacity: config.tracing_queue_capacity,
            batch_size: config.tracing_batch_size,
            sampling_mode: config.tracing_sampling_mode,
            sample_ratio: config.tracing_sample_ratio,
            service_name: config.tracing_service_name.clone(),
            service_namespace: config.tracing_service_namespace.clone(),
            deployment_environment: config.tracing_deployment_environment.clone(),
            runtime_role: config.runtime_role.as_str().to_string(),
            trust_incoming_sampling: config.tracing_trust_incoming_sampling,
            propagate_outbound: config.tracing_propagate_outbound,
        })
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn sampling_mode(&self) -> TracingSamplingMode {
        self.sampling_mode
    }

    #[must_use]
    pub const fn sample_ratio(&self) -> f64 {
        self.sample_ratio
    }

    #[must_use]
    pub const fn trust_incoming_sampling(&self) -> bool {
        self.trust_incoming_sampling
    }

    #[must_use]
    pub const fn propagate_outbound(&self) -> bool {
        self.propagate_outbound
    }
}

fn validate_timeout(name: &str, value: u64) -> Result<(), String> {
    if !(1..=MAX_TIMEOUT_MS).contains(&value) {
        return Err(format!("{name} must be between 1 and {MAX_TIMEOUT_MS}"));
    }
    Ok(())
}

fn validate_resource_value(name: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_RESOURCE_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(format!(
            "{name} must contain 1 to {MAX_RESOURCE_VALUE_BYTES} bytes without control characters"
        ));
    }
    Ok(())
}

fn validate_endpoint(value: &str) -> Result<String, String> {
    let mut endpoint = reqwest::Url::parse(value)
        .map_err(|_| "tracing_otlp_endpoint must be a valid HTTPS URL".to_string())?;
    if endpoint.scheme() != "https" {
        return Err("tracing_otlp_endpoint must use HTTPS".to_string());
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err("tracing_otlp_endpoint must not contain embedded credentials".to_string());
    }
    if endpoint.host_str().is_none() || endpoint.query().is_some() || endpoint.fragment().is_some()
    {
        return Err(
            "tracing_otlp_endpoint must include a host and must not include a query or fragment"
                .to_string(),
        );
    }
    let base_path = endpoint.path().trim_end_matches('/');
    let traces_path = if base_path.is_empty() {
        "/v1/traces".to_string()
    } else {
        format!("{base_path}/v1/traces")
    };
    endpoint.set_path(&traces_path);
    Ok(endpoint.to_string())
}

fn parse_headers(value: Option<&str>) -> Result<HashMap<String, String>, String> {
    let Some(value) = value else {
        return Ok(HashMap::new());
    };
    if value.len() > MAX_OTLP_HEADERS_BYTES {
        return Err(format!(
            "tracing_otlp_headers exceeds the {MAX_OTLP_HEADERS_BYTES}-byte limit"
        ));
    }
    let mut headers = HashMap::new();
    for entry in value.split(',') {
        let (name, header_value) = entry
            .split_once('=')
            .ok_or_else(|| "tracing_otlp_headers entries must use name=value".to_string())?;
        let name = name.trim().to_ascii_lowercase();
        let header_value = header_value.trim();
        let parsed_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| "tracing_otlp_headers contains an invalid header name".to_string())?;
        reqwest::header::HeaderValue::from_str(header_value)
            .map_err(|_| "tracing_otlp_headers contains an invalid header value".to_string())?;
        if matches!(
            parsed_name.as_str(),
            "traceparent" | "tracestate" | "baggage" | "content-length" | "content-type" | "host"
        ) {
            return Err(format!(
                "tracing_otlp_headers cannot override transport-owned header {name}"
            ));
        }
        if headers.insert(name, header_value.to_string()).is_some() {
            return Err("tracing_otlp_headers contains a duplicate header name".to_string());
        }
        if headers.len() > MAX_OTLP_HEADERS {
            return Err(format!(
                "tracing_otlp_headers supports at most {MAX_OTLP_HEADERS} headers"
            ));
        }
    }
    Ok(headers)
}

pub struct TraceRuntime {
    provider: Option<SdkTracerProvider>,
    tracer: Option<opentelemetry_sdk::trace::SdkTracer>,
    flush_timeout: Duration,
}

impl TraceRuntime {
    #[must_use]
    pub fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        self.tracer.clone()
    }

    pub async fn shutdown(&mut self) -> Result<(), String> {
        self.tracer.take();
        let Some(provider) = self.provider.take() else {
            return Ok(());
        };
        let flush_timeout = self.flush_timeout;
        // The blocking reqwest client owns an internal runtime and must also be
        // dropped outside an async executor context. The SDK timeout remains
        // the authoritative bound for exporter flush and thread shutdown.
        let result = tokio::task::spawn_blocking(move || {
            provider
                .shutdown_with_timeout(flush_timeout)
                .map_err(|error| format!("trace flush failed or timed out: {error}"))
        })
        .await
        .map_err(|_| "trace shutdown task failed".to_string())
        .and_then(|result| result);
        crate::observability::metrics::trace_flush(if result.is_ok() {
            "success"
        } else {
            "failure"
        });
        result
    }
}

impl Drop for TraceRuntime {
    fn drop(&mut self) {
        self.tracer.take();
        let Some(provider) = self.provider.take() else {
            return;
        };
        let flush_timeout = self.flush_timeout;
        let _ = std::thread::spawn(move || provider.shutdown_with_timeout(flush_timeout)).join();
    }
}

pub async fn initialize(settings: &TracingSettings) -> Result<TraceRuntime, String> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    hubuum_outbound_http::set_trace_propagation_enabled(
        settings.enabled && settings.propagate_outbound,
    );
    if !settings.enabled {
        return Ok(TraceRuntime {
            provider: None,
            tracer: None,
            flush_timeout: settings.flush_timeout,
        });
    }

    // reqwest's blocking client deliberately rejects construction from inside
    // an async runtime. Build the complete provider on a blocking worker; its
    // BatchSpanProcessor then owns export on a separate SDK thread.
    let settings = settings.clone();
    tokio::task::spawn_blocking(move || initialize_enabled(&settings))
        .await
        .map_err(|_| "trace initialization task failed".to_string())?
}

fn initialize_enabled(settings: &TracingSettings) -> Result<TraceRuntime, String> {
    let exporter = build_otlp_exporter(settings)?;
    let pending = Arc::new(AtomicUsize::new(0));
    let batch = BatchSpanProcessor::builder(ClassifiedSpanExporter::with_pending(
        exporter,
        pending.clone(),
    ))
    .with_batch_config(
        BatchConfigBuilder::default()
            .with_max_queue_size(settings.queue_capacity)
            .with_max_export_batch_size(settings.batch_size)
            .build(),
    )
    .build();
    let batch = MeteredBatchSpanProcessor {
        inner: batch,
        pending,
        capacity: settings.queue_capacity,
    };
    let sampler = configured_sampler(settings);
    let resource = Resource::builder_empty()
        .with_service_name(settings.service_name.clone())
        .with_attributes([
            KeyValue::new("service.namespace", settings.service_namespace.clone()),
            KeyValue::new(
                "deployment.environment.name",
                settings.deployment_environment.clone(),
            ),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("service.instance.role", settings.runtime_role.clone()),
        ])
        .build();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(batch)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();
    let tracer = provider.tracer("hubuum");
    global::set_tracer_provider(provider.clone());
    Ok(TraceRuntime {
        provider: Some(provider),
        tracer: Some(tracer),
        flush_timeout: settings.flush_timeout,
    })
}

fn build_otlp_exporter(
    settings: &TracingSettings,
) -> Result<opentelemetry_otlp::SpanExporter, String> {
    // BatchSpanProcessor 0.32 drives exporter futures on its own non-Tokio
    // thread. An async reqwest client has no reactor there, so use the SDK's
    // supported blocking transport; request handlers only enqueue spans and do
    // not execute this network request themselves.
    let mut client = reqwest::blocking::Client::builder()
        .connect_timeout(settings.connect_timeout)
        .timeout(settings.export_timeout)
        .redirect(reqwest::redirect::Policy::none());
    if let Some(path) = settings.ca_cert_path.as_deref() {
        let pem = read_bounded_regular_file(
            Path::new(path),
            "OTLP CA certificate bundle",
            MAX_CERTIFICATE_BUNDLE_BYTES,
        )
        .map_err(|_| "failed to read bounded OTLP CA certificate bundle".to_string())?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|_| "OTLP CA certificate bundle is not valid PEM".to_string())?;
        // A configured private bundle is an explicit trust policy, so use it
        // as the complete root set instead of depending on platform-specific
        // semantics for merging private roots into the native store.
        client = client.tls_certs_only(certificates);
    }
    if let Some((cert_path, key_path)) = &settings.client_identity_paths {
        let mut pem = read_bounded_regular_file(
            Path::new(cert_path),
            "OTLP client certificate",
            MAX_CERTIFICATE_BUNDLE_BYTES,
        )
        .map_err(|_| "failed to read bounded OTLP client certificate".to_string())?;
        pem.extend_from_slice(b"\n");
        pem.extend(
            read_bounded_regular_file(
                Path::new(key_path),
                "OTLP client private key",
                MAX_PRIVATE_KEY_BYTES,
            )
            .map_err(|_| "failed to read bounded OTLP client private key".to_string())?,
        );
        let identity = reqwest::Identity::from_pem(&pem)
            .map_err(|_| "OTLP client certificate or key is not valid PEM".to_string())?;
        client = client.identity(identity);
    }
    let client = client
        .build()
        .map_err(|_| "failed to build the OTLP HTTPS client".to_string())?;
    opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_http_client(client)
        .with_endpoint(
            settings
                .endpoint
                .as_deref()
                .expect("enabled tracing settings require an endpoint"),
        )
        .with_timeout(settings.export_timeout)
        .with_headers(settings.headers.clone())
        .build()
        .map_err(|_| "failed to initialize the OTLP trace exporter".to_string())
}

fn configured_sampler(settings: &TracingSettings) -> Box<dyn ShouldSample> {
    match settings.sampling_mode {
        TracingSamplingMode::Off => Box::new(Sampler::AlwaysOff),
        TracingSamplingMode::AlwaysOn => Box::new(Sampler::AlwaysOn),
        TracingSamplingMode::ParentBasedRatio if settings.trust_incoming_sampling => Box::new(
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(settings.sample_ratio))),
        ),
        TracingSamplingMode::ParentBasedRatio => Box::new(
            LocallyControlledParentRatioSampler::new(settings.sample_ratio),
        ),
    }
}

#[derive(Clone, Debug)]
struct LocallyControlledParentRatioSampler {
    ratio: f64,
    trace_id_generator: fn() -> TraceId,
}

impl LocallyControlledParentRatioSampler {
    fn new(ratio: f64) -> Self {
        Self {
            ratio,
            trace_id_generator: locally_generated_trace_id,
        }
    }

    #[cfg(test)]
    fn with_trace_id_generator(ratio: f64, trace_id_generator: fn() -> TraceId) -> Self {
        Self {
            ratio,
            trace_id_generator,
        }
    }
}

impl ShouldSample for LocallyControlledParentRatioSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        span_kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        let has_untrusted_remote_parent = parent_context
            .filter(|context| context.has_active_span())
            .is_some_and(|context| context.span().span_context().is_remote());
        if has_untrusted_remote_parent {
            return Sampler::TraceIdRatioBased(self.ratio).should_sample(
                parent_context,
                (self.trace_id_generator)(),
                name,
                span_kind,
                attributes,
                links,
            );
        }
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.ratio))).should_sample(
            parent_context,
            trace_id,
            name,
            span_kind,
            attributes,
            links,
        )
    }
}

fn locally_generated_trace_id() -> TraceId {
    RandomIdGenerator::default().new_trace_id()
}

struct RequestHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for RequestHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

pub fn extract_remote_parent(headers: &HeaderMap) -> Result<Option<Context>, &'static str> {
    let traceparent = single_bounded_header(headers, "traceparent")?;
    let Some(_) = traceparent else {
        return Ok(None);
    };
    let tracestate = single_bounded_header(headers, "tracestate")?;
    if tracestate.is_some_and(|value| value.split(',').count() > MAX_TRACESTATE_MEMBERS) {
        return Err("tracestate has too many members");
    }
    let context = TraceContextPropagator::new().extract(&RequestHeaderExtractor(headers));
    if !context.span().span_context().is_valid() {
        return Err("trace context is invalid");
    }
    Ok(Some(context))
}

fn single_bounded_header<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<Option<&'a str>, &'static str> {
    let mut values = headers.get_all(name);
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err("trace header must not be repeated");
    }
    let value = value.to_str().map_err(|_| "trace header is not ASCII")?;
    if value.is_empty() || value.len() > MAX_TRACE_HEADER_BYTES {
        return Err("trace header is empty or too large");
    }
    Ok(Some(value))
}

pub fn set_remote_parent(span: &Span, parent: Option<Context>) {
    if let Some(parent) = parent {
        let _ = span.set_parent(parent);
    }
}

#[must_use]
pub fn current_trace_link() -> Option<TraceLink> {
    trace_link_from_context(&Context::current())
}

#[must_use]
pub fn trace_link_from_span(span: &Span) -> Option<TraceLink> {
    trace_link_from_context(&span.context())
}

fn trace_link_from_context(context: &Context) -> Option<TraceLink> {
    let span_context = context.span().span_context().clone();
    if !span_context.is_valid() {
        return None;
    }
    TraceLink::new(
        span_context.trace_id().to_string(),
        span_context.span_id().to_string(),
        span_context.trace_flags().to_u8(),
        0,
    )
    .ok()
}

pub fn add_link(span: &Span, link: Option<&TraceLink>) {
    let Some(link) = link.and_then(span_context_from_link) else {
        return;
    };
    span.add_link(link);
}

fn span_context_from_link(link: &TraceLink) -> Option<SpanContext> {
    let trace_id = TraceId::from_hex(link.trace_id()).ok()?;
    let span_id = SpanId::from_hex(link.span_id()).ok()?;
    Some(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(link.trace_flags()),
        true,
        TraceState::default(),
    ))
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    use opentelemetry_sdk::trace::SdkTracer;
    use tracing::Dispatch;
    use tracing_subscriber::{EnvFilter, Layer, filter::filter_fn, layer::SubscriberExt};

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub(super) struct CapturingExporter(pub(super) Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for CapturingExporter {
        fn export(
            &self,
            batch: Vec<SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            let spans = self.0.clone();
            async move {
                spans.lock().unwrap().extend(batch);
                Ok(())
            }
        }
    }

    pub(crate) struct TraceCapture {
        provider: SdkTracerProvider,
        dispatch: Dispatch,
        exporter: CapturingExporter,
    }

    impl TraceCapture {
        pub(crate) fn new(log_level: &str) -> Self {
            let exporter = CapturingExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(ClassifiedSpanExporter::new(exporter.clone()))
                .with_sampler(Sampler::AlwaysOn)
                .build();
            let subscriber = tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::sink)
                        .with_filter(EnvFilter::new(log_level)),
                )
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer("trace-regression-test"))
                        .with_filter(filter_fn(|metadata| {
                            metadata.is_span() && is_catalog_span(metadata.name())
                        })),
                );
            Self {
                provider,
                dispatch: Dispatch::new(subscriber),
                exporter,
            }
        }

        pub(crate) fn tracer(&self) -> SdkTracer {
            self.provider.tracer("trace-regression-test")
        }

        pub(crate) fn dispatch(&self) -> Dispatch {
            self.dispatch.clone()
        }

        pub(crate) fn spans(&self) -> Vec<SpanData> {
            self.provider.force_flush().unwrap();
            self.exporter.0.lock().unwrap().clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use actix_web::http::header::{HeaderName, HeaderValue};
    use base64::Engine;
    use opentelemetry::baggage::BaggageExt;
    use opentelemetry::trace::{Span as _, SpanKind, Tracer as _};
    use opentelemetry_sdk::trace::{SamplingDecision, ShouldSample};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use uuid::Uuid;

    use super::test_support::{CapturingExporter, TraceCapture};
    use super::*;

    // Pinned, test-only localhost CA/server certificate, valid 2026-09-03 to
    // 2036-08-31. The private key is not used outside this loopback fixture.
    const LOCALHOST_CA_DER_B64: &str = "MIIDLTCCAhWgAwIBAgIUckd1Z/EFKIubmsZ3ybIyKZErUFswDQYJKoZIhvcNAQELBQAwHjEcMBoGA1UEAwwTSHVidXVtIE9UTFAgVGVzdCBDQTAeFw0yNjA5MDMxNzQ1NTFaFw0zNjA4MzExNzQ1NTFaMB4xHDAaBgNVBAMME0h1YnV1bSBPVExQIFRlc3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC3/gDJbCqt8vlbb/lVngqBzHyubRlKRi7orj2v9fKrlEZbtml+sAFEZfdVQKiMQz2IOZe+JlScOJEtYGSlCQkDDiJyZnMChazqNQOm2MUM8wT7PaTAT5aJ4DxrrKXvUNhTtq/oePoiBvpU4U0s4m7IUOff/tTSqfeXjZc3UMBs+EPLvuCrVC1NHAL7SBO6c/DRfgZp41ktQXNPjEZiQbU2+UD91E5VqDPpyR+KUmcNbOCbh5jFVkxfOV0JT7XId+vWnqhMFeC4f663bOUJr14JY63yvcC9hrxi480WeSzbZtLdaU/ZgAwe/IjgYmxWdqAldJ6VnSr8KDMXYydlx15DAgMBAAGjYzBhMB0GA1UdDgQWBBQhOx4g0Fv6z9o/DwVx2jXOwlWOEDAfBgNVHSMEGDAWgBQhOx4g0Fv6z9o/DwVx2jXOwlWOEDAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjANBgkqhkiG9w0BAQsFAAOCAQEAW6eDC0L+iPvzs6Q9oi+PNV03ooeqgbPYOpavdqdFpalsRbgzaUeyjltJtAa5YasnHFGs1jv6iwlsrQhvLjdsca7Kgdteff3iBqGRxBMyUyJWc7lh7zaJld6FXlFh33Y0us552fqxi6bx6QFQYruBreylv4Ude26R7KsR6zabxxL02CuB1vufwYwkFbstL3Rx6IKv4If8xlhgu+0DyrS7yaVivZBmm8C0k3X5Hs0bPtoi8o9KX4Apv1O5+OrCVpB4dApuoWVbSdhdsy++xUXa5IjjBh7dnTADcZ10/Ec0TyrR5Wz2CuYhsG/QSZI6DDERIVerPM/xYMD6KaWTpH6Cbg==";
    const LOCALHOST_SERVER_CERT_DER_B64: &str = "MIIDTTCCAjWgAwIBAgIUWfhECmPc9LV6B+p82qYdESJSBZYwDQYJKoZIhvcNAQELBQAwHjEcMBoGA1UEAwwTSHVidXVtIE9UTFAgVGVzdCBDQTAeFw0yNjA5MDMxNzQ1NTFaFw0zNjA4MzExNzQ1NTFaMBQxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL5Fsk+QJMC98kpDcg9oBYJLI0IHHEgBNc6ucKEFnZO63r6320hyPf51AR912jVR0c8FoBj6NIZXQ858YgSjhJ4fgWWhbGFY9l8rHUWSVZi8E2et8Abp/j/d3Jb9ocon4e1F6ZpviO5tAsRROdv9rOQ/mUfEMI4/hhX6oMgsWQonpFv9ZdrQ9m4ZVy9YW8S8GTPYH/BehDpOADse3oGLwCtLw424IJGyd0hYSnuNtIRNkAHS9TDiuFEK6ObxJZa00XMOmTYYQUC0swY1sfEPftDjq6hA/QT/Vttht8i2hM4onq8zrlSxxeyKHeDmxHBMnB/Kco8aN5sVfLfnSIAD2DMCAwEAAaOBjDCBiTAUBgNVHREEDTALgglsb2NhbGhvc3QwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwHQYDVR0OBBYEFH2yY217/QvXvgXlkaRYes0o8jhwMB8GA1UdIwQYMBaAFCE7HiDQW/rP2j8PBXHaNc7CVY4QMA0GCSqGSIb3DQEBCwUAA4IBAQAiCwT/F9Otec4WXQk5azXqjFaktGzc9tBczSAOqMcfLBDf4IDFql1MzEGsLSwp5qBvX7iKBnd3B3J4+8RCqcalWF19tIYINC6dU/DppDxrir0aUa5+EtpAY9VNXMDeHbu29DNv0jlWyDU6HG6NaTRHOKeI89OaSBZmK2ai6il0iiPkG71aL3KYw1XL9PX1IEihMh2JUE4wDglF7QFtLDIXc/ucYdPXy7V9L7FGfDyVyWZ+gvUp1qbvWlzxPCuWjoG2GVFqpJSgDugfFoy3tYcsNmyK787CJPh1hbAYteXijFweHVJT8CK+WdI+GmeM74edSZ5ORcH4JekGG3F4uWQU";
    const LOCALHOST_SERVER_KEY_DER_B64: &str = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC+RbJPkCTAvfJKQ3IPaAWCSyNCBxxIATXOrnChBZ2Tut6+t9tIcj3+dQEfddo1UdHPBaAY+jSGV0POfGIEo4SeH4FloWxhWPZfKx1FklWYvBNnrfAG6f4/3dyW/aHKJ+HtRemab4jubQLEUTnb/azkP5lHxDCOP4YV+qDILFkKJ6Rb/WXa0PZuGVcvWFvEvBkz2B/wXoQ6TgA7Ht6Bi8ArS8ONuCCRsndIWEp7jbSETZAB0vUw4rhRCujm8SWWtNFzDpk2GEFAtLMGNbHxD37Q46uoQP0E/1bbYbfItoTOKJ6vM65UscXsih3g5sRwTJwfynKPGjebFXy350iAA9gzAgMBAAECggEAEBC3cQNVLxb6pa2TLFzWlj084V7TUvsTLXvKE7ZzKx2MoCLK9495z4nWie77+SOK6QVrEqDRnYQxu/YBmq2pzWYQmWGrUn7d8oQj6RNlectrggYMLbFFH+ReMzuAFR8P8uZDxp/jOmpm4OeQ0JPXLMB38zjL9r7DVgi+2Zfw/qvb579xnpZ99mXh7MRO3WoXsNBipsEI2ZWMYqygSUba1nUGt53NW7020szFYIu7sRX5cPIYRfNs8tyKmp1rbx3TVwdnMajyMJBK+E/ayX7UdM1qEtRAwULWqDj3C/a+AFVScuLj74ouDjCfzGKAbQM63gQL549MYOiHNAvU0lEMjQKBgQDk/FCthf9JSXlygud04a2QWAufUKbFaa1pB2z/E1ArQNYiK6vsgrR6oPZwStotpQcDIRslKT9NKlhwHom+76ORZ6Qq4HfxD8PuBsJZ3PczEiA6nS4Lw7dxYeG8fLNNoe2taf0zCww3fXerZ/Vpza5G0eMcSdbEnU4SoXHvsNiXvQKBgQDUuC8ezcsaxRjncUg65DcgSK1GrBWtzrw2IWWhz9excjQRFHxY/4lmS9hfIyn/QjPnoK/74rvfoToYaItkUIrVRlgrby4wD4QMYLpgmRMMFIrSEe3VxhONhklDK7r0Cmtv1o683qIsuyT0AqH5b0I0ZkP15f9IU6Ahoqio0L92rwKBgGnw9p6hvS+6B69cMxvXgcajhZUK6m2xa+KI5fvJgrDQSYH7tIozGq8Vo47mgrTVgj4HZhi2Uaww6EPPTSmCk6mlSsXvnm5wPdT1WZvb6J6/Rxv3NqOIxGl3wMnE4+wJ+/3caKHh/Z/6s5AhA+EUoQnw92NSIkRLByEFgsJDjqH9AoGAOHUR/Ij+KQK602KbmxXLE8R2SNWPg2vlRDCk+sdhJfV7oDThs/VOkFn8+XMpyFfX0tgxHRdWacou0x+cL2m5D1X2PMDrb1IO6AIcNVsrVP92wL6Fc0F5GwzTEQLgQbkqPqqhg7tLK7gX3LN+Lee7mJCz5OXAVX/sdkDnpraQun0CgYEA5O6IoAIdjgZbmw0NfC1CfK20ddCvrvqc2fbx4kEjoBlgmqoNeX560eqN/KvDpmkMOo9+u8TeFaqj1A9pDtSCZKXuitAf2oNlCvUYNr4b6GJ0O0lKJgqPunAA37lS/NEO3rk2C/bEubP9x79T3MEMILrXdVdEdMaqyKPiVUP8wgc=";

    async fn spawn_otlp_https_endpoint() -> (u16, oneshot::Receiver<Vec<u8>>, Vec<u8>) {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let ca_certificate = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_CA_DER_B64)
            .unwrap();
        let server_certificate = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_SERVER_CERT_DER_B64)
            .unwrap();
        let private_key = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_SERVER_KEY_DER_B64)
            .unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![
                    CertificateDer::from(server_certificate),
                    CertificateDer::from(ca_certificate.clone()),
                ],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(private_key)),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (request_tx, request_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "exporter closed before sending request headers");
                request.extend_from_slice(&chunk[..read]);
                if let Some(index) = request.windows(4).position(|value| value == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while request.len() < header_end + content_length {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "exporter closed before sending request body");
                request.extend_from_slice(&chunk[..read]);
            }
            request_tx.send(request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\n\r\n",
                )
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        });

        (port, request_rx, ca_certificate)
    }

    fn certificate_pem(certificate: &[u8]) -> String {
        let encoded = base64::engine::general_purpose::STANDARD.encode(certificate);
        let body = encoded
            .as_bytes()
            .chunks(64)
            .map(|line| std::str::from_utf8(line).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        format!("-----BEGIN CERTIFICATE-----\n{body}\n-----END CERTIFICATE-----\n")
    }

    struct TemporaryCertificate(std::path::PathBuf);

    impl TemporaryCertificate {
        fn new(certificate: &[u8]) -> Self {
            let path =
                std::env::temp_dir().join(format!("hubuum-otel-test-ca-{}.pem", Uuid::new_v4()));
            fs::write(&path, certificate_pem(certificate)).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TemporaryCertificate {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn current_trace_link_survives_a_span_excluded_from_telemetry() {
        let capture = TraceCapture::new("debug");
        tracing::dispatcher::with_default(&capture.dispatch(), || {
            let request = tracing::info_span!("http.server.request");
            let expected = trace_link_from_span(&request).unwrap();
            let _request = request.enter();
            let internal = tracing::info_span!("import_planning");
            assert!(!internal.is_disabled());
            let _internal = internal.enter();

            assert_eq!(current_trace_link(), Some(expected));
        });
    }

    #[test]
    fn rejects_invalid_and_oversized_trace_headers_without_returning_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("secret-invalid-value"),
        );
        assert!(matches!(
            extract_remote_parent(&headers),
            Err("trace context is invalid")
        ));

        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_str(&"a".repeat(MAX_TRACE_HEADER_BYTES + 1)).unwrap(),
        );
        assert!(matches!(
            extract_remote_parent(&headers),
            Err("trace header is empty or too large")
        ));
    }

    #[test]
    fn rejects_ambiguous_and_overwide_trace_state() {
        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        headers.append(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        assert!(matches!(
            extract_remote_parent(&headers),
            Err("trace header must not be repeated")
        ));

        headers.remove("traceparent");
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        let state = (0..=MAX_TRACESTATE_MEMBERS)
            .map(|index| format!("v{index}=1"))
            .collect::<Vec<_>>()
            .join(",");
        headers.insert(
            HeaderName::from_static("tracestate"),
            HeaderValue::from_str(&state).unwrap(),
        );
        assert!(matches!(
            extract_remote_parent(&headers),
            Err("tracestate has too many members")
        ));
    }

    #[test]
    fn accepts_a_valid_w3c_parent_and_ignores_baggage() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        headers.insert(
            HeaderName::from_static("baggage"),
            HeaderValue::from_static("secret=value"),
        );
        let context = extract_remote_parent(&headers).unwrap().unwrap();
        let span = context.span();
        assert_eq!(
            span.span_context().trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert!(context.baggage().is_empty());
    }

    #[test]
    fn settings_debug_redacts_endpoint_headers_and_key_paths() {
        let mut config = crate::config::get_config().unwrap();
        config.tracing_enabled = true;
        config.tracing_otlp_endpoint = Some("https://collector.example/v1".to_string());
        config.tracing_otlp_headers = Some("authorization=secret".to_string());
        config.tracing_otlp_client_key = Some("/secret/key.pem".to_string());
        config.tracing_otlp_client_cert = Some("/cert.pem".to_string());
        let debug = format!("{:?}", TracingSettings::from_config(&config).unwrap());
        assert!(!debug.contains("collector.example"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("key.pem"));
    }

    #[rstest::rstest]
    #[case(-0.01, false)]
    #[case(0.0, true)]
    #[case(0.25, true)]
    #[case(1.0, true)]
    #[case(1.01, false)]
    #[case(f64::NAN, false)]
    #[case(f64::INFINITY, false)]
    fn sampling_ratio_bounds_preserve_fractional_values(
        #[case] ratio: f64,
        #[case] accepted: bool,
    ) {
        let mut config = crate::config::get_config().unwrap();
        config.tracing_sample_ratio = ratio;
        assert_eq!(TracingSettings::from_config(&config).is_ok(), accepted);
    }

    #[rstest::rstest]
    #[case(None, None, true)]
    #[case(Some("cert.pem"), Some("key.pem"), true)]
    #[case(Some("cert.pem"), None, false)]
    #[case(None, Some("key.pem"), false)]
    fn client_identity_paths_are_preserved_as_a_pair(
        #[case] cert: Option<&str>,
        #[case] key: Option<&str>,
        #[case] accepted: bool,
    ) {
        let mut config = crate::config::get_config().unwrap();
        config.tracing_otlp_client_cert = cert.map(str::to_string);
        config.tracing_otlp_client_key = key.map(str::to_string);
        let result = TracingSettings::from_config(&config);
        assert_eq!(result.is_ok(), accepted);
        if let Ok(settings) = result {
            assert_eq!(
                settings.client_identity_paths,
                cert.zip(key)
                    .map(|(cert, key)| (cert.to_string(), key.to_string()))
            );
        }
    }

    #[test]
    fn tracing_settings_reject_unsafe_or_unbounded_configuration() {
        let mut config = crate::config::get_config().unwrap();
        config.tracing_enabled = true;
        config.tracing_otlp_endpoint = Some("http://collector.invalid".to_string());
        assert!(TracingSettings::from_config(&config).is_err());

        config.tracing_otlp_endpoint = Some("https://collector.invalid".to_string());
        config.tracing_otlp_headers = Some("traceparent=not-operator-metadata".to_string());
        assert!(TracingSettings::from_config(&config).is_err());

        config.tracing_otlp_headers = None;
        config.tracing_batch_size = config.tracing_queue_capacity + 1;
        assert!(TracingSettings::from_config(&config).is_err());
    }

    #[test]
    fn initialization_errors_do_not_reveal_certificate_paths() {
        let mut config = crate::config::get_config().unwrap();
        config.tracing_enabled = true;
        config.tracing_otlp_endpoint = Some("https://collector.invalid".to_string());
        let sensitive_path = "/private/collector-tenant/secret-ca.pem";
        config.tracing_otlp_ca_cert = Some(sensitive_path.to_string());
        let settings = TracingSettings::from_config(&config).unwrap();

        let error = build_otlp_exporter(&settings).unwrap_err();

        assert!(!error.contains(sensitive_path));
        assert!(!error.contains("collector.invalid"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_export_uses_private_ca_flushes_and_applies_the_final_redaction_boundary() {
        let (port, request_rx, certificate) = spawn_otlp_https_endpoint().await;
        let certificate = TemporaryCertificate::new(&certificate);

        let mut config = crate::config::get_config().unwrap();
        config.tracing_enabled = true;
        config.tracing_otlp_endpoint = Some(format!("https://localhost:{port}"));
        config.tracing_otlp_headers = Some("x-collector-auth=bounded-test-value".to_string());
        config.tracing_otlp_ca_cert = Some(certificate.path().to_string_lossy().into_owned());
        config.tracing_sampling_mode = TracingSamplingMode::AlwaysOn;
        config.tracing_queue_capacity = 8;
        config.tracing_batch_size = 8;
        let settings = TracingSettings::from_config(&config).unwrap();
        let exporter = tokio::task::spawn_blocking(move || build_otlp_exporter(&settings))
            .await
            .unwrap()
            .unwrap();
        let processor = BatchSpanProcessor::builder(ClassifiedSpanExporter::new(exporter))
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(8)
                    .with_max_export_batch_size(8)
                    .build(),
            )
            .build();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("otlp-private-ca-test");
        let mut span = tracer.start("http.server.request");
        span.set_attribute(KeyValue::new("http.route", "/api/v1/classes/{id}"));
        span.set_attribute(KeyValue::new("unsafe.attribute", "secret-payload"));
        span.end();

        tokio::task::spawn_blocking(move || provider.shutdown_with_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let request = tokio::time::timeout(Duration::from_secs(1), request_rx)
            .await
            .unwrap()
            .unwrap();
        let header_end = request
            .windows(4)
            .position(|value| value == b"\r\n\r\n")
            .unwrap()
            + 4;
        let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
        assert!(
            headers.starts_with("post /v1/traces http/1.1\r\n"),
            "unexpected OTLP request headers: {headers}"
        );
        assert!(headers.contains("x-collector-auth: bounded-test-value\r\n"));
        assert!(
            !request[header_end..]
                .windows(b"secret-payload".len())
                .any(|value| value == b"secret-payload")
        );
    }

    #[test]
    fn remote_sampling_flag_is_honored_only_when_explicitly_trusted() {
        let mut config = crate::config::get_config().unwrap();
        config.tracing_sample_ratio = 0.0;
        config.tracing_sampling_mode = TracingSamplingMode::ParentBasedRatio;
        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let remote_context = Context::new().with_remote_span_context(SpanContext::new(
            trace_id,
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ));

        config.tracing_trust_incoming_sampling = true;
        let trusted = TracingSettings::from_config(&config).unwrap();
        assert_eq!(
            configured_sampler(&trusted)
                .should_sample(
                    Some(&remote_context),
                    trace_id,
                    "http.server.request",
                    &SpanKind::Server,
                    &[],
                    &[],
                )
                .decision,
            SamplingDecision::RecordAndSample
        );

        config.tracing_trust_incoming_sampling = false;
        let untrusted = TracingSettings::from_config(&config).unwrap();
        assert_eq!(
            configured_sampler(&untrusted)
                .should_sample(
                    Some(&remote_context),
                    trace_id,
                    "http.server.request",
                    &SpanKind::Server,
                    &[],
                    &[],
                )
                .decision,
            SamplingDecision::Drop
        );
    }

    #[test]
    fn untrusted_remote_trace_id_cannot_select_the_ratio_sampling_decision() {
        fn locally_selected_drop_trace_id() -> TraceId {
            TraceId::from_hex("ffffffffffffffffffffffffffffffff").unwrap()
        }

        let attacker_selected_trace_id =
            TraceId::from_hex("00000000000000000000000000000001").unwrap();
        assert_eq!(
            Sampler::TraceIdRatioBased(0.5)
                .should_sample(
                    None,
                    attacker_selected_trace_id,
                    "http.server.request",
                    &SpanKind::Server,
                    &[],
                    &[],
                )
                .decision,
            SamplingDecision::RecordAndSample
        );
        let remote_context = Context::new().with_remote_span_context(SpanContext::new(
            attacker_selected_trace_id,
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ));
        let sampler = LocallyControlledParentRatioSampler::with_trace_id_generator(
            0.5,
            locally_selected_drop_trace_id,
        );

        assert_eq!(
            sampler
                .should_sample(
                    Some(&remote_context),
                    attacker_selected_trace_id,
                    "http.server.request",
                    &SpanKind::Server,
                    &[],
                    &[],
                )
                .decision,
            SamplingDecision::Drop
        );
    }

    #[test]
    fn exporter_allows_only_catalog_spans_and_classified_attributes() {
        let capture = CapturingExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(ClassifiedSpanExporter::new(capture.clone()))
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("classification-test");

        let mut request = tracer.start("http.server.request");
        request.set_attribute(KeyValue::new("http.route", "/api/v1/classes/{id}"));
        request.set_attribute(KeyValue::new("resource.name", "secret-object-name"));
        request.add_event(
            "unsafe log event",
            vec![KeyValue::new("payload", "secret-payload")],
        );
        request.end();

        let mut unknown = tracer.start("user-controlled-span-name");
        unknown.set_attribute(KeyValue::new("payload", "secret-payload"));
        unknown.end();
        provider.force_flush().unwrap();

        let spans = capture.0.lock().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "http.server.request");
        assert_eq!(spans[0].attributes.len(), 1);
        assert_eq!(spans[0].attributes[0].key.as_str(), "http.route");
        assert!(spans[0].events.is_empty());
        assert!(!format!("{:?}", spans[0]).contains("secret"));
    }

    #[test]
    fn metered_batch_processor_drops_at_capacity_without_blocking_the_producer() {
        let capture = CapturingExporter::default();
        let batch =
            BatchSpanProcessor::builder(ClassifiedSpanExporter::new(capture.clone())).build();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(MeteredBatchSpanProcessor {
                inner: batch,
                pending: Arc::new(AtomicUsize::new(1)),
                capacity: 1,
            })
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("saturation-test");

        tracer.start("http.server.request").end();
        provider.shutdown().unwrap();

        assert!(capture.0.lock().unwrap().is_empty());
    }
}
