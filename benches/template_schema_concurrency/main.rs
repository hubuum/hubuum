//! End-to-end execution evidence, including child startup and schema contention.
use hubuum_templates::{TemplateBatch, TemplateExecution, TemplateLimits};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;

const TEMPLATE: &str = "{% macro item(x) %}{{ x.id }}:{{ x.name }}:{{ x.payload }}\n{% endmacro %}{% for x in items %}{{ item(x) }}{% endfor %}";

fn workload(worker: usize) -> (Value, Value) {
    let context = json!({"items": (0..256).map(|id| json!({"id":id, "name":"item", "payload":"x".repeat(1024)})).collect::<Vec<_>>()});
    let schema = json!({"type":"object", "properties": {"items": {"type":"array", "items": {"type":"object", "required":["id","name","payload"]}}}, "title":format!("worker-{worker}")});
    (context, schema)
}

fn validate_workload(schema: &Value, context: &Value) {
    // Keep the full template workload while each schema evaluation fits the
    // admission budget. Measurements include all batches and their preparation.
    for items in context["items"].as_array().unwrap().chunks(4) {
        hubuum_domain::validate_json_value(schema, &json!({"items": items})).unwrap();
    }
}

#[tokio::main]
async fn main() {
    let (context, schema) = workload(0);
    let started = Instant::now();
    validate_workload(&schema, &context);
    TemplateExecution::new("cold", TEMPLATE, TemplateLimits::new(64, 500_000))
        .render(&context)
        .await
        .unwrap();
    println!(
        "{}",
        json!({"scenario":"template_schema_cold_start", "elapsed_us":started.elapsed().as_micros()})
    );
    // Keep initial meta-schema and per-schema compilation out of comparisons.
    for worker in 0..8 {
        let (context, schema) = workload(worker);
        validate_workload(&schema, &context);
    }
    for concurrency in [1, 4, 8] {
        let barrier = Arc::new(Barrier::new(concurrency));
        let started = Instant::now();
        let workers = (0..concurrency)
            .map(|worker| {
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    let (context, schema) = workload(worker);
                    barrier.wait().await;
                    let mut peak_heap = 0;
                    let mut samples = Vec::new();
                    for _ in 0..10 {
                        let started = Instant::now();
                        validate_workload(&schema, &context);
                        let rendered = TemplateExecution::new(
                            "concurrent",
                            TEMPLATE,
                            TemplateLimits::new(64, 500_000),
                        )
                        .render(&context)
                        .await
                        .unwrap();
                        samples.push(started.elapsed().as_micros());
                        peak_heap = peak_heap.max(rendered.peak_heap_bytes());
                    }
                    (peak_heap, samples)
                })
            })
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        for worker in workers {
            results.push(worker.await.unwrap());
        }
        let peak_heap = results.iter().map(|(peak, _)| *peak).max().unwrap();
        let mut samples = results
            .into_iter()
            .flat_map(|(_, samples)| samples)
            .collect::<Vec<_>>();
        samples.sort_unstable();
        println!(
            "{}",
            json!({"scenario":"template_schema_concurrency", "concurrency":concurrency, "renders":concurrency*10, "elapsed_ms":started.elapsed().as_millis(), "p50_us":samples[samples.len()/2], "p95_us":samples[(samples.len()-1)*95/100], "peak_worker_rust_heap_bytes":peak_heap})
        );
    }
    benchmark_batches().await;
}

async fn benchmark_batches() {
    // Paired samples on the same process/worker build. The email baseline also
    // compiles both entries separately, matching the former delivery path.
    let context = json!({"name": "example", "id": 42, "payload": "x".repeat(4096)});
    for (scenario, entries, validate_first) in
        [("email", 2, true), ("remote_ten_headers", 12, false)]
    {
        let mut individual = Vec::new();
        let mut batched = Vec::new();
        for iteration in 0..21 {
            // Reverse the order each round to reduce systematic order effects.
            for use_batch in if iteration % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let started = Instant::now();
                let limits = TemplateLimits::new(64, 50_000);
                let execution = || TemplateExecution::new("entry", "{{ name }}:{{ id }}", limits);
                let outputs = if use_batch {
                    let mut batch = TemplateBatch::new(entries * 1024);
                    for _ in 0..entries {
                        batch.push(execution()).unwrap();
                    }
                    batch.render(&context).await.unwrap()
                } else {
                    if validate_first {
                        for _ in 0..entries {
                            hubuum_templates::prepare_template("{{ name }}:{{ id }}")
                                .limits(limits)
                                .validate()
                                .await
                                .unwrap();
                        }
                    }
                    let mut outputs = Vec::new();
                    for _ in 0..entries {
                        outputs.push(execution().render(&context).await.unwrap());
                    }
                    outputs
                };
                let elapsed = started.elapsed().as_micros();
                assert_eq!(outputs.len(), entries);
                for output in outputs {
                    assert_eq!(output.into_parts().0, "example:42");
                }
                if iteration > 0 {
                    if use_batch {
                        batched.push(elapsed);
                    } else {
                        individual.push(elapsed);
                    }
                }
            }
        }
        for (mode, mut samples, worker_starts) in [
            (
                "individual",
                individual,
                entries * if validate_first { 2 } else { 1 },
            ),
            ("batch", batched, 1),
        ] {
            samples.sort_unstable();
            println!(
                "{}",
                json!({"scenario": scenario, "mode": mode, "operations": samples.len(),
                "templates_per_operation": entries, "worker_starts_per_operation": worker_starts,
                "p50_us": samples[samples.len()/2], "p95_us": samples[(samples.len()-1)*95/100]})
            );
        }
    }
}
