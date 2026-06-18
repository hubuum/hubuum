use criterion::{criterion_group, criterion_main, Criterion};
use hubuum::utilities::auth::{hash_password, verify_password};
use std::hint::black_box;
use std::time::Duration;

// Argon2 is deliberately expensive, so this is a Criterion (wall-clock) bench
// with a small sample size. Its purpose is a regression tripwire: it surfaces
// accidental changes to the Argon2 cost parameters (which would weaken password
// security or blow up login latency), not micro-level timing noise.
const PASSWORD: &str = "correct horse battery staple";

fn benchmark_password_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("password_hashing");
    // Argon2 hashing takes tens of milliseconds; keep the run bounded.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("hash_password", |b| {
        b.iter(|| hash_password(black_box(PASSWORD)).expect("password should hash"));
    });

    let stored_hash = hash_password(PASSWORD).expect("password should hash for verify benchmark");

    group.bench_function("verify_password", |b| {
        b.iter(|| {
            verify_password(black_box(PASSWORD), black_box(&stored_hash))
                .expect("verification should not error")
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_password_hashing);
criterion_main!(benches);
