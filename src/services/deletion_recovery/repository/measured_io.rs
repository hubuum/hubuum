use std::fs::File;
use std::io::{self, Read, Write};

use crate::observability::metrics::{
    CheckpointOperation, CheckpointTimer, checkpoint_read_bytes, checkpoint_write_bytes,
};

/// Count successful syscall-sized chunks, even when read_to_end/write_all later
/// fails. The observer is injectable so failure regressions avoid global races.
struct ObservedIo<T, F> {
    inner: T,
    observe: F,
}

impl<R: Read, F: FnMut(usize)> Read for ObservedIo<R, F> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        (self.observe)(count);
        Ok(count)
    }
}

impl<W: Write, F: FnMut(usize)> Write for ObservedIo<W, F> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        (self.observe)(count);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(super) fn read(file: File, maximum: u64) -> io::Result<Vec<u8>> {
    let timer = CheckpointTimer::start(CheckpointOperation::Read);
    let mut bytes = Vec::new();
    let result = ObservedIo {
        inner: file,
        observe: checkpoint_read_bytes,
    }
    .take(maximum)
    .read_to_end(&mut bytes);
    timer.finish(result.is_ok());
    result.map(|_| bytes)
}

pub(super) fn write(file: &mut File, bytes: &[u8]) -> io::Result<()> {
    let timer = CheckpointTimer::start(CheckpointOperation::Write);
    let result = ObservedIo {
        inner: file,
        observe: checkpoint_write_bytes,
    }
    .write_all(bytes);
    timer.finish(result.is_ok());
    result
}

pub(super) fn sync_file(file: &File) -> io::Result<()> {
    sync(file, CheckpointOperation::FsyncFile)
}
pub(super) fn sync_directory(file: &File) -> io::Result<()> {
    sync(file, CheckpointOperation::FsyncDirectory)
}

fn sync(file: &File, operation: CheckpointOperation) -> io::Result<()> {
    let timer = CheckpointTimer::start(operation);
    let result = file.sync_all();
    timer.finish(result.is_ok());
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct InterruptedWrite {
        bytes: Vec<u8>,
        remaining: usize,
    }
    impl Write for InterruptedWrite {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                return Err(io::Error::other("injected interrupted write"));
            }
            let count = bytes.len().min(self.remaining);
            self.bytes.extend_from_slice(&bytes[..count]);
            self.remaining -= count;
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn partial_failure_and_retry_count_actual_written_bytes() {
        let expected = b"authenticated checkpoint bytes";
        let mut file = InterruptedWrite {
            bytes: Vec::new(),
            remaining: 7,
        };
        let mut counted = 0;
        assert!(
            ObservedIo {
                inner: &mut file,
                observe: |n| counted += n
            }
            .write_all(expected)
            .is_err()
        );
        assert_eq!(counted, 7);
        let prefix = file.bytes.len();
        file.remaining = usize::MAX;
        ObservedIo {
            inner: &mut file,
            observe: |n| counted += n,
        }
        .write_all(&expected[prefix..])
        .unwrap();
        assert_eq!(file.bytes, expected);
        assert_eq!(counted, expected.len());
    }

    struct InterruptedRead {
        bytes: Cursor<Vec<u8>>,
        remaining: usize,
    }
    impl Read for InterruptedRead {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                return Err(io::Error::other("injected interrupted read"));
            }
            let limit = bytes.len().min(self.remaining);
            let count = self.bytes.read(&mut bytes[..limit])?;
            self.remaining -= count;
            Ok(count)
        }
    }

    #[test]
    fn failed_readback_counts_bytes_before_failure_and_a_full_retry() {
        let expected = b"signed HEAD reference";
        let mut file = InterruptedRead {
            bytes: Cursor::new(expected.to_vec()),
            remaining: 5,
        };
        let mut counted = 0;
        assert!(
            ObservedIo {
                inner: &mut file,
                observe: |n| counted += n
            }
            .read_to_end(&mut Vec::new())
            .is_err()
        );
        assert_eq!(counted, 5);
        let mut result = Vec::new();
        ObservedIo {
            inner: expected.as_slice(),
            observe: |n| counted += n,
        }
        .read_to_end(&mut result)
        .unwrap();
        assert_eq!(result, expected);
        assert_eq!(counted, 5 + expected.len());
    }
}
