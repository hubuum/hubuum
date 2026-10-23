use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hubuum_templates::{SizeLimitedWriter, json_value_to_template_value, register_curated_helpers};
use minijinja::Environment;
use serde_json::json;
use std::hint::black_box;

fn templates(c: &mut Criterion) {
    let context = json!({"items": (0..128).map(|id| json!({"name": format!("host-{id}"), "id": id})).collect::<Vec<_>>()});
    let source = "{% for item in items %}{{ item.name }}:{{ item.id }}\n{% endfor %}";
    let mut environment = Environment::new();
    environment.set_recursion_limit(32);
    environment.set_fuel(Some(100_000));
    register_curated_helpers(&mut environment, None);
    environment.add_template("report", source).unwrap();
    let template = environment.get_template("report").unwrap();
    let context = json_value_to_template_value(&context);
    let mut group = c.benchmark_group("release_template");
    group.throughput(Throughput::Elements(128));
    group.bench_function("render", |b| {
        b.iter(|| {
            let mut writer = SizeLimitedWriter::new(64 * 1024);
            template
                .render_captured_to(black_box(context.clone()), &mut writer)
                .unwrap();
            black_box(writer.into_string().unwrap())
        })
    });
    group.finish();
}
criterion_group!(benches, templates);
criterion_main!(benches);
