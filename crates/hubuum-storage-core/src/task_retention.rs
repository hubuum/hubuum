//! Independently protected task admission receipts and scoped detail retention.
//!
//! Receipt revisions are separate from application/deletion fences. Ordinary
//! task retention must not invalidate unrelated producers.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageValidationError;

mod receipt;
pub use receipt::*;
mod policy;
pub use policy::*;
mod record;
pub use record::*;
mod purge;
pub use purge::*;
mod authority;
pub use authority::*;
mod capability;
pub use capability::*;
#[cfg(test)]
mod tests;

pub const TASK_CONTROL_MAX_RECEIPTS: usize = 16_384;
pub const TASK_CONTROL_MAX_RECEIPT_BYTES: usize = 1024;
pub const TASK_CONTROL_MAX_RECEIPTS_BYTES: usize = 12 * 1024 * 1024;
pub const TASK_CONTROL_MAX_RETENTION_BYTES: usize = 4 * 1024 * 1024;
pub const TASK_CONTROL_MAX_PURGE_BATCHES: usize = 1024;
pub const TASK_CONTROL_MAX_PURGE_TASKS: usize = 64;
pub const TASK_CONTROL_PURGE_COMPLETION_RESERVE: usize = 512;
pub const TASK_CONTROL_MAX_POLICIES: usize = 64;
pub const TASK_CONTROL_MAX_HOLDS: usize = 1024;

fn invalid(message: impl Into<String>) -> StorageValidationError {
    StorageValidationError::invalid(message)
}

macro_rules! revision {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(try_from = "i64", into = "i64")]
        pub struct $name(i64);

        impl $name {
            pub fn new(value: i64) -> Result<Self, StorageValidationError> {
                if value < 0 {
                    return Err(invalid("Task control revision must not be negative"));
                }
                Ok(Self(value))
            }
            pub const fn value(self) -> i64 {
                self.0
            }
            pub fn next(self) -> Result<Self, StorageValidationError> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or_else(|| invalid("Task control revision exhausted"))
            }
        }
        impl TryFrom<i64> for $name {
            type Error = StorageValidationError;
            fn try_from(value: i64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for i64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}
revision!(TaskControlRevision);
revision!(TaskRetentionPolicyRevision);

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(try_from = "Uuid", into = "Uuid")]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Result<Self, StorageValidationError> {
                if value.is_nil() {
                    return Err(invalid("Task control identity must not be nil"));
                }
                Ok(Self(value))
            }
            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }
            pub const fn value(self) -> Uuid {
                self.0
            }
        }
        impl TryFrom<Uuid> for $name {
            type Error = StorageValidationError;
            fn try_from(value: Uuid) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}
identifier!(TaskReceiptGeneration);
identifier!(TaskPurgeBatchId);
identifier!(TaskHoldId);

macro_rules! digest {
    ($name:ident) => {
        #[derive(Clone, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: String) -> Result<Self, StorageValidationError> {
                if value.len() != 64
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(invalid(
                        "Task control digest must contain 64 lowercase hexadecimal characters",
                    ));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([redacted])"))
            }
        }
        impl TryFrom<String> for $name {
            type Error = StorageValidationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}
digest!(ControlKeyFingerprint);
digest!(ProtectedTaskKey);
digest!(TaskRequestFingerprint);

#[cfg(test)]
mod size_tests;
