use actix_http::{Request, Response};
use actix_service::{IntoServiceFactory, ServiceFactory};
use actix_web::{Error, HttpServer, body::MessageBody, dev::AppConfig};
#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
use std::path::Path;

use crate::config::TlsBackend;
#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
use crate::utilities::bounded_file::{
    MAX_CERTIFICATE_BUNDLE_BYTES, MAX_PRIVATE_KEY_BYTES, read_bounded_regular_file,
};

type ServerResult<F, I, S, B> = std::io::Result<HttpServer<F, I, S, B>>;

pub fn install_default_crypto_provider() -> std::io::Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    if rustls::crypto::CryptoProvider::get_default().is_some() {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "Failed to install the process-wide rustls crypto provider",
        ))
    }
}

#[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
fn no_tls_backend_error(requested_backend: Option<TlsBackend>) -> std::io::Error {
    let message = match requested_backend {
        Some(backend) => format!(
            "TLS backend `{}` was requested, but no TLS backend was enabled during build. Please enable either `tls-rustls` or `tls-openssl` during build to use TLS",
            backend.as_str()
        ),
        None => "TLS certificate and key offered, but no TLS backend was enabled during build. Please enable either `tls-rustls` or `tls-openssl` during build to use TLS".to_string(),
    };

    std::io::Error::other(message)
}

#[cfg(any(
    all(feature = "tls-rustls", not(feature = "tls-openssl")),
    all(feature = "tls-openssl", not(feature = "tls-rustls"))
))]
fn unavailable_backend_error(
    requested_backend: TlsBackend,
    available_feature: &'static str,
) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "TLS backend `{}` was requested, but this build only includes `{}`",
            requested_backend.as_str(),
            available_feature
        ),
    )
}

fn resolve_backend(requested_backend: Option<TlsBackend>) -> std::io::Result<TlsBackend> {
    #[cfg(all(feature = "tls-rustls", feature = "tls-openssl"))]
    {
        Ok(requested_backend.unwrap_or(TlsBackend::Rustls))
    }

    #[cfg(all(feature = "tls-rustls", not(feature = "tls-openssl")))]
    {
        match requested_backend {
            Some(TlsBackend::Openssl) => {
                Err(unavailable_backend_error(TlsBackend::Openssl, "tls-rustls"))
            }
            _ => Ok(TlsBackend::Rustls),
        }
    }

    #[cfg(all(feature = "tls-openssl", not(feature = "tls-rustls")))]
    {
        match requested_backend {
            Some(TlsBackend::Rustls) => {
                Err(unavailable_backend_error(TlsBackend::Rustls, "tls-openssl"))
            }
            _ => Ok(TlsBackend::Openssl),
        }
    }

    #[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
    {
        Err(no_tls_backend_error(requested_backend))
    }
}

#[cfg(feature = "tls-openssl")]
fn validate_openssl_key_pair(builder: &openssl::ssl::SslAcceptorBuilder) -> std::io::Result<()> {
    builder.check_private_key().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("TLS private key does not match the certificate: {error}"),
        )
    })
}

pub fn configure_server<F, I, S, B>(
    server: HttpServer<F, I, S, B>,
    bind_address: &str,
    cert: &str,
    key: &str,
    pass: Option<&str>,
    backend: Option<TlsBackend>,
) -> ServerResult<F, I, S, B>
where
    F: Fn() -> I + Send + Clone + 'static,
    I: IntoServiceFactory<S, Request>,
    S: ServiceFactory<Request, Config = AppConfig> + 'static,
    S::Error: Into<Error>,
    S::InitError: std::fmt::Debug,
    S::Response: Into<Response<B>>,
    B: MessageBody + 'static,
{
    let selected_backend = resolve_backend(backend)?;

    #[cfg(feature = "tls-rustls")]
    if selected_backend == TlsBackend::Rustls {
        return tls_rustls::configure_server(server, bind_address, cert, key, pass);
    }

    #[cfg(feature = "tls-openssl")]
    if selected_backend == TlsBackend::Openssl {
        return tls_openssl::configure_server(server, bind_address, cert, key, pass);
    }

    let _ = (server, bind_address, cert, key, pass, selected_backend);
    unreachable!("resolved TLS backend without a compiled implementation")
}

#[cfg(feature = "tls-rustls")]
mod tls_rustls {
    use super::*;
    use rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    };
    use tracing::info;

    fn parse_certificate_chain(
        certificate_bytes: &[u8],
    ) -> std::io::Result<Vec<CertificateDer<'static>>> {
        let cert_chain = CertificateDer::pem_slice_iter(certificate_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to parse TLS certificate chain: {e}"),
                )
            })?;
        if cert_chain.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "TLS certificate chain contains no certificates",
            ));
        }
        Ok(cert_chain)
    }

    pub fn configure_server<F, I, S, B>(
        server: HttpServer<F, I, S, B>,
        bind_address: &str,
        cert: &str,
        key: &str,
        pass: Option<&str>,
    ) -> ServerResult<F, I, S, B>
    where
        F: Fn() -> I + Send + Clone + 'static,
        I: IntoServiceFactory<S, Request>,
        S: ServiceFactory<Request, Config = AppConfig> + 'static,
        S::Error: Into<Error>,
        S::InitError: std::fmt::Debug,
        S::Response: Into<Response<B>>,
        B: MessageBody + 'static,
    {
        if pass.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Using encrypted TLS key with passphrase is not supported with rustls feature",
            ));
        }

        let certificate_bytes = read_bounded_regular_file(
            Path::new(cert),
            "TLS certificate chain",
            MAX_CERTIFICATE_BUNDLE_BYTES,
        )?;
        let cert_chain = parse_certificate_chain(&certificate_bytes)?;

        let key_bytes =
            read_bounded_regular_file(Path::new(key), "TLS private key", MAX_PRIVATE_KEY_BYTES)?;
        let key_der = PrivateKeyDer::from_pem_slice(&key_bytes).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse TLS private key: {e}"),
            )
        })?;

        let rustls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to configure TLS: {e}"),
                )
            })?;

        info!("Server binding with rustls to https://{}", bind_address);
        server
            .bind_rustls_0_23(bind_address, rustls_config)
            .map_err(|e| std::io::Error::other(format!("Failed to bind server: {e}")))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn rustls_rejects_a_malformed_entry_after_a_certificate() {
            let pem = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n\
                -----BEGIN CERTIFICATE-----\nnot-base64!\n-----END CERTIFICATE-----\n";

            let error = parse_certificate_chain(pem).unwrap_err();

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            assert!(
                error
                    .to_string()
                    .contains("Failed to parse TLS certificate chain")
            );
        }

        #[test]
        fn rustls_rejects_a_bundle_without_certificates() {
            let error = parse_certificate_chain(b"").unwrap_err();

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "TLS certificate chain contains no certificates"
            );
        }
    }
}

#[cfg(feature = "tls-openssl")]
mod tls_openssl {
    use super::*;
    use openssl::{
        pkey::PKey,
        ssl::{SslAcceptor, SslAcceptorBuilder, SslMethod},
        x509::X509,
    };
    use tracing::info;

    pub(super) fn build_acceptor(
        certificate_bytes: &[u8],
        key_bytes: &[u8],
        pass: Option<&str>,
    ) -> std::io::Result<SslAcceptorBuilder> {
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())
            .map_err(|e| std::io::Error::other(format!("unable to create SSL acceptor: {e}")))?;

        let pkey = match pass {
            Some(pass) => PKey::private_key_from_pem_passphrase(key_bytes, pass.as_bytes())
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unable to decrypt private key: {e}"),
                    )
                })?,
            None => PKey::private_key_from_pem(key_bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unable to parse private key: {e}"),
                )
            })?,
        };
        builder.set_private_key(&pkey).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unable to set private key: {e}"),
            )
        })?;

        let certificates = X509::stack_from_pem(certificate_bytes).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unable to parse certificate chain: {e}"),
            )
        })?;
        let mut certificates = certificates.into_iter();
        let certificate = certificates.next().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "TLS certificate chain contains no certificates",
            )
        })?;
        builder.set_certificate(&certificate).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unable to set TLS certificate: {e}"),
            )
        })?;
        for certificate in certificates {
            builder.add_extra_chain_cert(certificate).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unable to set TLS certificate chain: {e}"),
                )
            })?;
        }
        validate_openssl_key_pair(&builder)?;
        Ok(builder)
    }

    pub fn configure_server<F, I, S, B>(
        server: HttpServer<F, I, S, B>,
        bind_address: &str,
        cert: &str,
        key: &str,
        pass: Option<&str>,
    ) -> ServerResult<F, I, S, B>
    where
        F: Fn() -> I + Send + Clone + 'static,
        I: IntoServiceFactory<S, Request>,
        S: ServiceFactory<Request, Config = AppConfig> + 'static,
        S::Error: Into<Error>,
        S::InitError: std::fmt::Debug,
        S::Response: Into<Response<B>>,
        B: MessageBody + 'static,
    {
        let key_bytes =
            read_bounded_regular_file(Path::new(key), "TLS private key", MAX_PRIVATE_KEY_BYTES)?;
        let certificate_bytes = read_bounded_regular_file(
            Path::new(cert),
            "TLS certificate chain",
            MAX_CERTIFICATE_BUNDLE_BYTES,
        )?;
        let builder = build_acceptor(&certificate_bytes, &key_bytes, pass)?;

        info!("Server binding with openssl to https://{}", bind_address);
        server
            .bind_openssl(bind_address, builder)
            .map_err(|e| std::io::Error::other(format!("Failed to bind server: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::{install_default_crypto_provider, resolve_backend};
    use crate::config::TlsBackend;

    #[test]
    fn installs_a_process_wide_crypto_provider() {
        install_default_crypto_provider().unwrap();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[cfg(all(feature = "tls-rustls", feature = "tls-openssl"))]
    #[test]
    fn backend_selection_defaults_to_rustls_when_both_backends_are_enabled() {
        assert_eq!(resolve_backend(None).unwrap(), TlsBackend::Rustls);
    }

    #[cfg(all(feature = "tls-rustls", feature = "tls-openssl"))]
    #[test]
    fn backend_selection_honors_explicit_backend_when_both_backends_are_enabled() {
        assert_eq!(
            resolve_backend(Some(TlsBackend::Rustls)).unwrap(),
            TlsBackend::Rustls
        );
        assert_eq!(
            resolve_backend(Some(TlsBackend::Openssl)).unwrap(),
            TlsBackend::Openssl
        );
    }

    #[cfg(all(feature = "tls-rustls", not(feature = "tls-openssl")))]
    #[test]
    fn backend_selection_defaults_to_rustls_when_only_rustls_is_enabled() {
        assert_eq!(resolve_backend(None).unwrap(), TlsBackend::Rustls);
        assert_eq!(
            resolve_backend(Some(TlsBackend::Rustls)).unwrap(),
            TlsBackend::Rustls
        );
    }

    #[cfg(all(feature = "tls-rustls", not(feature = "tls-openssl")))]
    #[test]
    fn backend_selection_rejects_openssl_when_only_rustls_is_enabled() {
        let error = resolve_backend(Some(TlsBackend::Openssl)).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(
            "TLS backend `openssl` was requested, but this build only includes `tls-rustls`"
        ));
    }

    #[cfg(all(feature = "tls-openssl", not(feature = "tls-rustls")))]
    #[test]
    fn backend_selection_defaults_to_openssl_when_only_openssl_is_enabled() {
        assert_eq!(resolve_backend(None).unwrap(), TlsBackend::Openssl);
        assert_eq!(
            resolve_backend(Some(TlsBackend::Openssl)).unwrap(),
            TlsBackend::Openssl
        );
    }

    #[cfg(all(feature = "tls-openssl", not(feature = "tls-rustls")))]
    #[test]
    fn backend_selection_rejects_rustls_when_only_openssl_is_enabled() {
        let error = resolve_backend(Some(TlsBackend::Rustls)).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(
            "TLS backend `rustls` was requested, but this build only includes `tls-openssl`"
        ));
    }

    #[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
    #[test]
    fn backend_selection_rejects_tls_requests_when_no_backend_is_enabled() {
        let implicit_backend_error = resolve_backend(None).unwrap_err();
        assert_eq!(implicit_backend_error.kind(), std::io::ErrorKind::Other);
        assert!(
            implicit_backend_error
                .to_string()
                .contains("no TLS backend was enabled during build")
        );

        let explicit_backend_error = resolve_backend(Some(TlsBackend::Rustls)).unwrap_err();
        assert_eq!(explicit_backend_error.kind(), std::io::ErrorKind::Other);
        assert!(
            explicit_backend_error
                .to_string()
                .contains("TLS backend `rustls` was requested")
        );
    }

    #[cfg(feature = "tls-openssl")]
    mod openssl {
        use openssl::{
            asn1::Asn1Time,
            hash::MessageDigest,
            pkey::{PKey, Private},
            rsa::Rsa,
            ssl::{SslAcceptor, SslAcceptorBuilder, SslMethod},
            symm::Cipher,
            x509::{X509, X509NameBuilder},
        };
        use rstest::rstest;

        use super::super::{tls_openssl::build_acceptor, validate_openssl_key_pair};

        fn certificate_for(key: &PKey<Private>) -> X509 {
            let mut name = X509NameBuilder::new().unwrap();
            name.append_entry_by_text("CN", "localhost").unwrap();
            let name = name.build();

            let mut certificate = X509::builder().unwrap();
            certificate.set_version(2).unwrap();
            certificate.set_subject_name(&name).unwrap();
            certificate.set_issuer_name(&name).unwrap();
            certificate
                .set_not_before(&Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            certificate
                .set_not_after(&Asn1Time::days_from_now(1).unwrap())
                .unwrap();
            certificate.set_pubkey(key).unwrap();
            certificate.sign(key, MessageDigest::sha256()).unwrap();
            certificate.build()
        }

        fn acceptor_with_key(
            certificate_key: &PKey<Private>,
            configured_key: &PKey<Private>,
        ) -> SslAcceptorBuilder {
            let certificate = certificate_for(certificate_key);
            let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
            builder.set_private_key(configured_key).unwrap();
            builder.set_certificate(&certificate).unwrap();
            builder
        }

        #[test]
        fn openssl_accepts_a_matching_certificate_and_private_key() {
            let key = PKey::from_rsa(Rsa::generate(2_048).unwrap()).unwrap();
            let builder = acceptor_with_key(&key, &key);

            validate_openssl_key_pair(&builder).unwrap();
        }

        #[test]
        fn openssl_rejects_a_mismatched_certificate_and_private_key() {
            let certificate_key = PKey::from_rsa(Rsa::generate(2_048).unwrap()).unwrap();
            let configured_key = PKey::from_rsa(Rsa::generate(2_048).unwrap()).unwrap();
            let builder = acceptor_with_key(&certificate_key, &configured_key);

            let error = validate_openssl_key_pair(&builder).unwrap_err();

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                error
                    .to_string()
                    .contains("TLS private key does not match the certificate")
            );
        }

        #[rstest]
        #[case::unencrypted(None)]
        #[case::encrypted(Some("test-passphrase"))]
        fn openssl_builds_an_acceptor_from_pem_material(#[case] passphrase: Option<&str>) {
            let key = PKey::from_rsa(Rsa::generate(2_048).unwrap()).unwrap();
            let certificate = certificate_for(&key).to_pem().unwrap();
            let private_key = match passphrase {
                Some(passphrase) => key
                    .private_key_to_pem_pkcs8_passphrase(
                        Cipher::aes_256_cbc(),
                        passphrase.as_bytes(),
                    )
                    .unwrap(),
                None => key.private_key_to_pem_pkcs8().unwrap(),
            };

            build_acceptor(&certificate, &private_key, passphrase).unwrap();
        }
    }
}
