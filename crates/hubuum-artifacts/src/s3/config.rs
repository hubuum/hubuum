use std::fmt;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
};
use aws_smithy_http_client::{
    Builder,
    tls::{Provider, TlsContext, TrustStore, rustls_provider::CryptoMode},
};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use url::Url;

use crate::{ArtifactError, ArtifactResult, Destination};

/// Secrets are never serialized or included in Debug. Credentials may rotate
/// without changing the destination. Endpoint/bucket replacement requires a new ID.
pub struct S3Config {
    pub(super) destination: Destination,
    pub(super) bucket: String,
    pub(super) region: String,
    pub(super) endpoint: Option<String>,
    credentials: Credentials,
    ca_pem: Option<Vec<u8>>,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("destination", &self.destination)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

pub struct S3ConfigBuilder {
    destination: Destination,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    credentials: Option<Credentials>,
    ca_pem: Option<Vec<u8>>,
}

impl S3Config {
    pub fn builder(destination: Destination) -> S3ConfigBuilder {
        S3ConfigBuilder {
            destination,
            bucket: String::new(),
            region: String::new(),
            endpoint: None,
            credentials: None,
            ca_pem: None,
        }
    }

    pub(super) fn client(&self) -> ArtifactResult<Client> {
        let trust = self
            .ca_pem
            .as_ref()
            .map_or_else(TrustStore::default, |pem| {
                TrustStore::default().with_pem_certificate(pem.clone())
            });
        let tls = TlsContext::builder()
            .with_trust_store(trust)
            .build()
            .map_err(|_| ArtifactError::Invalid("Invalid S3 TLS trust configuration".into()))?;
        let http = Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
            .tls_context(tls)
            .build_https();
        let mut builder = aws_sdk_s3::config::Builder::new();
        builder.set_endpoint_url(self.endpoint.clone());
        let config = builder
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(self.region.clone()))
            .credentials_provider(self.credentials.clone())
            .force_path_style(true)
            .http_client(http)
            .retry_config(RetryConfig::standard().with_max_attempts(2))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(30))
                    .operation_attempt_timeout(Duration::from_secs(15))
                    .connect_timeout(Duration::from_secs(5))
                    .build(),
            )
            .build();
        Ok(Client::from_conf(config))
    }
}

impl S3ConfigBuilder {
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
    pub fn credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        self.credentials = Some(Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            "hubuum-artifacts",
        ));
        self
    }
    pub fn private_ca_pem(mut self, pem: Vec<u8>) -> Self {
        self.ca_pem = Some(pem);
        self
    }
    pub fn build(self) -> ArtifactResult<S3Config> {
        if self.bucket.len() < 3
            || self.bucket.len() > 63
            || !self
                .bucket
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            || !self
                .bucket
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
            || self.bucket.contains("..")
            || self.bucket.contains(".-")
            || self.bucket.contains("-.")
            || self.bucket.parse::<std::net::Ipv4Addr>().is_ok()
            || !self.bucket.bytes().all(|value| {
                value.is_ascii_lowercase() || value.is_ascii_digit() || b".-".contains(&value)
            })
            || self.region.is_empty()
            || self.region.len() > 64
            || !self
                .region
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || value == b'-')
        {
            return Err(ArtifactError::Invalid("Invalid S3 bucket or region".into()));
        }
        if let Some(endpoint) = &self.endpoint {
            let url = Url::parse(endpoint)
                .map_err(|_| ArtifactError::Invalid("Invalid S3 endpoint".into()))?;
            if url.scheme() != "https"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
            {
                return Err(ArtifactError::Invalid(
                    "S3 endpoint must be an HTTPS origin without credentials, path or query".into(),
                ));
            }
        }
        if self
            .ca_pem
            .as_ref()
            .is_some_and(|pem| pem.is_empty() || pem.len() > 1024 * 1024)
        {
            return Err(ArtifactError::Invalid(
                "S3 private CA must contain 1–1,048,576 PEM bytes".into(),
            ));
        }
        if let Some(pem) = &self.ca_pem {
            let certificates = CertificateDer::pem_slice_iter(pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ArtifactError::Invalid("Invalid S3 private CA PEM".into()))?;
            if certificates.is_empty() {
                return Err(ArtifactError::Invalid(
                    "S3 private CA PEM contains no certificates".into(),
                ));
            }
            let mut roots = rustls::RootCertStore::empty();
            for certificate in certificates {
                roots.add(certificate).map_err(|_| {
                    ArtifactError::Invalid("Invalid S3 private CA certificate".into())
                })?;
            }
        }
        let credentials = self
            .credentials
            .filter(|credentials| {
                !credentials.access_key_id().is_empty()
                    && !credentials.secret_access_key().is_empty()
            })
            .ok_or_else(|| ArtifactError::Invalid("S3 credentials are required".into()))?;
        Ok(S3Config {
            destination: self.destination,
            bucket: self.bucket,
            region: self.region,
            endpoint: self.endpoint,
            credentials,
            ca_pem: self.ca_pem,
        })
    }
}
