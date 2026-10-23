use std::hint::black_box;
use std::io::{self, Write};

use criterion::{Criterion, criterion_group, criterion_main};
use hubuum_budget::{ByteLimit, LimitedWriter};
use hubuum_templates::SizeLimitedWriter;

// Keep the pre-extraction implementations as measurement baselines. Each case
// accepts the same 128 fragments, with identical destination/preallocation.
struct LegacyBackupWriter<W> {
    inner: W,
    remaining: usize,
    written: usize,
    exceeded: bool,
}

impl<W: Write> Write for LegacyBackupWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = true;
            return Err(io::Error::other("backup byte limit exceeded"));
        }
        let count = self.inner.write(bytes)?;
        self.remaining -= count;
        self.written += count;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct LegacyTemplateWriter {
    max_bytes: usize,
    buffer: Vec<u8>,
    exceeded: bool,
}

impl Write for LegacyTemplateWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buffer.len().saturating_add(buf.len()) > self.max_bytes {
            self.exceeded = true;
            return Err(io::Error::other("template output limit exceeded"));
        }
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }
    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write(buf).map(|_| ())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn legacy_backup() -> Vec<u8> {
    let mut writer = LegacyBackupWriter {
        inner: Vec::new(),
        remaining: black_box(8192),
        written: 0,
        exceeded: false,
    };
    for _ in 0..128 {
        writer
            .write_all(black_box(b"template output line\n"))
            .unwrap();
    }
    writer.inner
}

fn shared_backup() -> Vec<u8> {
    let mut writer = LimitedWriter::new(Vec::new(), ByteLimit::new(black_box(8192)));
    for _ in 0..128 {
        writer
            .write_all(black_box(b"template output line\n"))
            .unwrap();
    }
    writer.into_inner()
}

fn legacy_template() -> String {
    let max_bytes: usize = black_box(8192);
    let mut writer = LegacyTemplateWriter {
        max_bytes,
        buffer: Vec::with_capacity(max_bytes.min(1024)),
        exceeded: false,
    };
    for _ in 0..128 {
        writer
            .write_all(black_box(b"template output line\n"))
            .unwrap();
    }
    String::from_utf8(writer.buffer).unwrap()
}

fn shared_template() -> String {
    let mut writer = SizeLimitedWriter::new(black_box(8192));
    for _ in 0..128 {
        writer
            .write_all(black_box(b"template output line\n"))
            .unwrap();
    }
    writer.into_string().unwrap()
}

fn legacy_batch() -> Vec<Vec<u8>> {
    let mut remaining: usize = black_box(8192);
    let mut outputs = Vec::with_capacity(4);
    for _ in 0..4 {
        let max_bytes = remaining.min(black_box(2048));
        let mut writer = LegacyTemplateWriter {
            max_bytes,
            buffer: Vec::with_capacity(max_bytes.min(1024)),
            exceeded: false,
        };
        for _ in 0..32 {
            writer
                .write_all(black_box(b"template output line\n"))
                .unwrap();
        }
        remaining -= writer.buffer.len();
        outputs.push(writer.buffer);
    }
    outputs
}

fn shared_batch() -> Vec<Vec<u8>> {
    let mut parent = ByteLimit::new(black_box(8192)).start();
    let mut outputs = Vec::with_capacity(4);
    for _ in 0..4 {
        let limit = ByteLimit::new(black_box(2048));
        let buffer = Vec::with_capacity(limit.bytes().min(parent.remaining()).min(1024));
        let mut writer = parent.writer(buffer, limit);
        for _ in 0..32 {
            writer
                .write_all(black_box(b"template output line\n"))
                .unwrap();
        }
        outputs.push(writer.into_inner());
    }
    outputs
}

fn budgets(c: &mut Criterion) {
    assert_eq!(legacy_backup(), shared_backup());
    assert_eq!(legacy_backup().capacity(), shared_backup().capacity());
    assert_eq!(legacy_template(), shared_template());
    assert_eq!(legacy_template().capacity(), shared_template().capacity());
    assert_eq!(legacy_batch(), shared_batch());
    assert_eq!(
        legacy_batch().iter().map(Vec::capacity).collect::<Vec<_>>(),
        shared_batch().iter().map(Vec::capacity).collect::<Vec<_>>()
    );
    c.bench_function("budget/backup_legacy", |b| {
        b.iter(|| black_box(legacy_backup()))
    });
    c.bench_function("budget/backup_shared", |b| {
        b.iter(|| black_box(shared_backup()))
    });
    c.bench_function("budget/template_legacy", |b| {
        b.iter(|| black_box(legacy_template()))
    });
    c.bench_function("budget/template_shared", |b| {
        b.iter(|| black_box(shared_template()))
    });
    c.bench_function("budget/batch_legacy", |b| {
        b.iter(|| black_box(legacy_batch()))
    });
    c.bench_function("budget/batch_shared", |b| {
        b.iter(|| black_box(shared_batch()))
    });
    c.bench_function("budget/count_without_buffer", |b| {
        b.iter(|| {
            let mut writer = LimitedWriter::new(io::sink(), ByteLimit::new(black_box(8192)));
            for _ in 0..128 {
                writer
                    .write_all(black_box(b"template output line\n"))
                    .unwrap();
            }
            black_box(writer.budget().used())
        });
    });
}

criterion_group!(benches, budgets);
criterion_main!(benches);
