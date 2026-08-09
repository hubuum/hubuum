use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageErrorKind {
    BadRequest,
    Conflict,
    Database,
    Internal,
    NotFound,
    NotAcceptable,
    PayloadTooLarge,
    PreconditionFailed,
    Unavailable,
    Validation,
}

impl StorageErrorKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::Conflict => "conflict",
            Self::Database => "database",
            Self::Internal => "internal",
            Self::NotFound => "not_found",
            Self::NotAcceptable => "not_acceptable",
            Self::PayloadTooLarge => "payload_too_large",
            Self::PreconditionFailed => "precondition_failed",
            Self::Unavailable => "unavailable",
            Self::Validation => "validation",
        }
    }

    pub(crate) const fn is_backend_failure(self) -> bool {
        matches!(self, Self::Database | Self::Internal | Self::Unavailable)
    }
}

/// Backend-neutral failure returned by storage capabilities.
///
/// The representation deliberately carries no Diesel, Actix, or application
/// error types. The application error layer owns transport-facing translation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StorageError {
    kind: StorageErrorKind,
    message: String,
    current_etag: Option<String>,
}

impl StorageError {
    #[cfg(test)]
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::BadRequest, message, None)
    }

    #[cfg(test)]
    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    #[cfg(test)]
    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::NotFound, message, None)
    }

    pub(crate) fn new(
        kind: StorageErrorKind,
        message: impl Into<String>,
        current_etag: Option<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            current_etag,
        }
    }

    pub(crate) fn into_parts(self) -> (StorageErrorKind, String, Option<String>) {
        (self.kind, self.message, self.current_etag)
    }

    pub(crate) const fn kind(&self) -> StorageErrorKind {
        self.kind
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StorageError {}
