use actix_http::{Request, Response};
use actix_service::{IntoServiceFactory, ServiceFactory};
use actix_web::{body::MessageBody, dev::AppConfig, Error, HttpServer};

use crate::config::TlsBackend;

type ServerResult<F, I, S, B> = std::io::Result<HttpServer<F, I, S, B>>;

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
        pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
        ServerConfig,
    };
    use tracing::info;

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

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .map_err(|e| {
                std::io::Error::other(format!("Failed to install crypto provider: {e:?}"))
            })?;

        let cert_chain = CertificateDer::pem_file_iter(cert)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to read certificate file: {e}"),
                )
            })?
            .flatten()
            .collect();

        let key_der = PrivateKeyDer::from_pem_file(key).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to read key file: {e}"),
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
}

#[cfg(feature = "tls-openssl")]
mod tls_openssl {
    use super::*;
    use openssl::{
        pkey::PKey,
        ssl::{SslAcceptor, SslFiletype, SslMethod},
    };
    use std::{fs::File, io::Read};
    use tracing::info;

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
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())
            .map_err(|e| std::io::Error::other(format!("unable to create SSL acceptor: {e}")))?;

        if let Some(pass) = pass {
            let mut buf = Vec::new();
            File::open(key)?.read_to_end(&mut buf)?;
            let pkey =
                PKey::private_key_from_pem_passphrase(&buf, pass.as_bytes()).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unable to decrypt private key: {e}"),
                    )
                })?;
            builder.set_private_key(&pkey).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unable to set private key: {e}"),
                )
            })?;
        } else {
            builder
                .set_private_key_file(key, SslFiletype::PEM)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("unable to load private key file: {e}"),
                    )
                })?;
        }

        builder.set_certificate_chain_file(cert).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unable to load certificate chain: {e}"),
            )
        })?;

        info!("Server binding with openssl to https://{}", bind_address);
        server
            .bind_openssl(bind_address, builder)
            .map_err(|e| std::io::Error::other(format!("Failed to bind server: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_backend;
    use crate::config::TlsBackend;

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
        assert!(implicit_backend_error
            .to_string()
            .contains("no TLS backend was enabled during build"));

        let explicit_backend_error = resolve_backend(Some(TlsBackend::Rustls)).unwrap_err();
        assert_eq!(explicit_backend_error.kind(), std::io::ErrorKind::Other);
        assert!(explicit_backend_error
            .to_string()
            .contains("TLS backend `rustls` was requested"));
    }
}
