use std::io::{self, Write};

use serde::Serialize;

use crate::errors::ApiError;

#[cfg(not(feature = "integration-test-support"))]
const MAX_OBJECT_AGGREGATE_CANDIDATE_BATCH_BYTES: usize = 8 * 1024 * 1024;
#[cfg(feature = "integration-test-support")]
const MAX_OBJECT_AGGREGATE_CANDIDATE_BATCH_BYTES: usize = 4 * 1024;
pub(super) const MAX_OBJECT_AGGREGATE_ACCUMULATOR_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(super) enum ObjectAggregateJsonBound {
    CandidateBatch,
    Accumulator,
}

impl ObjectAggregateJsonBound {
    pub(super) const fn max_bytes(self) -> usize {
        match self {
            Self::CandidateBatch => MAX_OBJECT_AGGREGATE_CANDIDATE_BATCH_BYTES,
            Self::Accumulator => MAX_OBJECT_AGGREGATE_ACCUMULATOR_BYTES,
        }
    }

    pub(super) fn measure<T>(self, value: &T) -> Result<usize, ApiError>
    where
        T: Serialize,
    {
        let mut writer = BoundedSizeWriter::new(self.max_bytes());
        match serde_json::to_writer(&mut writer, value) {
            Ok(()) => Ok(writer.bytes),
            Err(_) if writer.exceeded => Err(self.overflow_error()),
            Err(error) => Err(ApiError::InternalServerError(format!(
                "Failed to measure {}: {error}",
                self.subject()
            ))),
        }
    }

    pub(super) fn overflow_error(self) -> ApiError {
        match self {
            Self::CandidateBatch => ApiError::PayloadTooLarge(format!(
                "An object snapshot exceeds the {}-byte grouped-query source batch limit",
                self.max_bytes()
            )),
            Self::Accumulator => ApiError::PayloadTooLarge(format!(
                "Object aggregate cardinality and values exceed the {}-byte intermediate storage limit; narrow the source filters or grouping dimensions",
                self.max_bytes()
            )),
        }
    }

    const fn subject(self) -> &'static str {
        match self {
            Self::CandidateBatch => "object aggregate candidate",
            Self::Accumulator => "aggregated object row",
        }
    }
}

struct BoundedSizeWriter {
    bytes: usize,
    max_bytes: usize,
    exceeded: bool,
}

impl BoundedSizeWriter {
    const fn new(max_bytes: usize) -> Self {
        Self {
            bytes: 0,
            max_bytes,
            exceeded: false,
        }
    }
}

impl Write for BoundedSizeWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(bytes) = self.bytes.checked_add(buffer.len()) else {
            self.exceeded = true;
            return Err(io::Error::other(
                "object aggregate serialized size overflowed",
            ));
        };
        if bytes > self.max_bytes {
            self.exceeded = true;
            return Err(io::Error::other(
                "object aggregate value exceeds its serialized size bound",
            ));
        }
        self.bytes = bytes;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
