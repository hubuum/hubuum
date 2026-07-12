use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TlsBackend {
    Rustls,
    Openssl,
}

impl TlsBackend {
    #[cfg(any(
        not(any(feature = "tls-rustls", feature = "tls-openssl")),
        all(feature = "tls-rustls", not(feature = "tls-openssl")),
        all(feature = "tls-openssl", not(feature = "tls-rustls"))
    ))]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rustls => "rustls",
            Self::Openssl => "openssl",
        }
    }
}
