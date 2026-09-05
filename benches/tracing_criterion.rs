use criterion::{Criterion, criterion_group, criterion_main};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use std::hint::black_box;
use tracing::{info_span, subscriber};
use tracing_subscriber::layer::SubscriberExt as _;

fn request_span() {
    let span = info_span!(
        "http.server.request",
        otel.kind = "server",
        http.request.method = "GET",
        http.route = "/api/v1/classes/{class_id}",
        http.response.status_code = 200_u16,
        client.network.category = "public",
        auth.principal.kind = "authenticated",
        request_id = "00000000-0000-0000-0000-000000000000",
        correlation_id = tracing::field::Empty,
        error.type = tracing::field::Empty,
    );
    black_box(span);
}

fn benchmark_tracing_overhead(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("request_trace_overhead");
    group.bench_function("disabled", |bencher| bencher.iter(request_span));

    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer().with_tracer(provider.tracer("hubuum-tracing-benchmark")),
    );
    subscriber::with_default(subscriber, || {
        group.bench_function("sampled", |bencher| bencher.iter(request_span));
    });
    group.finish();
    provider.shutdown().unwrap();
}

criterion_group!(benches, benchmark_tracing_overhead);
criterion_main!(benches);
