//! End-to-end execution evidence, including child startup and schema contention.
use hubuum_templates::{TemplateExecution, TemplateLimits};
use serde_json::{Value, json};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const TEMPLATE: &str = "{% macro item(x) %}{{ x.id }}:{{ x.name }}:{{ x.payload }}\n{% endmacro %}{% for x in items %}{{ item(x) }}{% endfor %}";

fn workload(worker: usize) -> (Value, Value) {
    let context = json!({"items": (0..256).map(|id| json!({"id":id, "name":"item", "payload":"x".repeat(1024)})).collect::<Vec<_>>()});
    let schema = json!({"type":"object", "properties": {"items": {"type":"array", "items": {"type":"object", "required":["id","name","payload"]}}}, "title":format!("worker-{worker}")});
    (context, schema)
}

fn main() {
    let (context, schema) = workload(0);
    let started = Instant::now();
    hubuum_domain::validate_json_value(&schema, &context).unwrap();
    TemplateExecution::new("cold", TEMPLATE, TemplateLimits::new(64, 500_000))
        .render(&context)
        .unwrap();
    println!(
        "{}",
        json!({"scenario":"template_schema_cold_start", "elapsed_us":started.elapsed().as_micros()})
    );
    // Keep initial meta-schema and per-schema compilation out of comparisons.
    for worker in 0..8 {
        let (context, schema) = workload(worker);
        hubuum_domain::validate_json_value(&schema, &context).unwrap();
    }
    for concurrency in [1, 4, 8] {
        let barrier = Arc::new(Barrier::new(concurrency));
        let started = Instant::now();
        let workers = (0..concurrency)
            .map(|worker| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let (context, schema) = workload(worker);
                    barrier.wait();
                    let mut peak_heap = 0;
                    let mut samples = Vec::new();
                    for _ in 0..10 {
                        let started = Instant::now();
                        hubuum_domain::validate_json_value(&schema, &context).unwrap();
                        let rendered = TemplateExecution::new(
                            "concurrent",
                            TEMPLATE,
                            TemplateLimits::new(64, 500_000),
                        )
                        .render(&context)
                        .unwrap();
                        samples.push(started.elapsed().as_micros());
                        peak_heap = peak_heap.max(rendered.peak_heap_bytes());
                    }
                    (peak_heap, samples)
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
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
}
