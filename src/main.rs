mod api;
mod config;
mod db;
mod errors;
mod extractors;
mod logger;
mod macros;
mod middlewares;
mod models;
mod schema;
mod tests;
mod traits;
mod utilities;

use actix_web::{middleware::Logger, web, web::Data, web::JsonConfig, App, HttpServer};
use db::init_pool;
#[cfg(feature = "swagger-ui")]
use utoipa::OpenApi;
#[cfg(feature = "swagger-ui")]
use utoipa_swagger_ui::SwaggerUi;

use tracing::{debug, info};
use tracing_subscriber::{
    filter::EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::config::get_config;
use crate::errors::{
    fatal_error, json_error_handler, EXIT_CODE_CONFIG_ERROR, EXIT_CODE_INIT_ERROR,
    EXIT_CODE_TLS_ERROR,
};
use crate::utilities::is_valid_log_level;
use crate::api::openapi::openapi_json as openapi_json_handler;

#[cfg(all(feature = "tls-openssl", feature = "tls-rustls"))]
compile_error!("Features `tls-openssl` and `tls-rustls` are mutually exclusive");

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Clone the config to prevent the mutex from being locked
    // See https://rust-lang.github.io/rust-clippy/master/index.html#await_holding_lock
    let config = match get_config() {
        Ok(cfg) => cfg.clone(),
        Err(e) => fatal_error(
            &format!("Failed to load configuration: {}", e),
            EXIT_CODE_CONFIG_ERROR,
        ),
    };
    let filter = if is_valid_log_level(&config.log_level) {
        EnvFilter::try_new(&config.log_level).unwrap_or_else(|_e| {
            fatal_error(
                &format!("Error parsing log level: {}", &config.log_level),
                EXIT_CODE_CONFIG_ERROR,
            )
        })
    } else {
        fatal_error(
            &format!("Invalid log level: {}", config.log_level),
            EXIT_CODE_CONFIG_ERROR,
        )
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(FmtSpan::CLOSE)
                .event_format(logger::HubuumLoggingFormat),
        )
        .init();

    debug!(
        message = "Starting server",
        bind_ip = %config.bind_ip,
        port = config.port,
        ssl = config.tls_cert_path.is_some() && config.tls_key_path.is_some(),
        log_level = %config.log_level,
        actix_workers = config.actix_workers,
        db_pool_size = config.db_pool_size,
    );

    let pool = init_pool(&config.database_url, config.db_pool_size);

    if let Err(e) = utilities::init::init(pool.clone()).await {
        fatal_error(
            &format!("Critical database initialization failed: {}", e),
            EXIT_CODE_INIT_ERROR,
        );
    }

    let server = HttpServer::new(move || {
        let app = App::new()
            .wrap(middlewares::tracing::TracingMiddleware)
            .wrap(Logger::default())
            .app_data(Data::new(pool.clone()))
            .app_data(JsonConfig::default().error_handler(json_error_handler))
            .route("/api-doc/openapi.json", web::get().to(openapi_json_handler));

        #[cfg(feature = "swagger-ui")]
        let app = app.service(
            SwaggerUi::new("/swagger-ui/{_:.*}")
                .url("/api-doc/openapi.json", api::openapi::ApiDoc::openapi()),
        );

        app.configure(api::config)
    });

    let bind_address = format!("{}:{}", config.bind_ip, config.port);

    let server = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => match tls::configure_server(
            server,
            &bind_address,
            cert,
            key,
            config.tls_key_passphrase.as_deref(),
        ) {
            Ok(srv) => srv,
            Err(e) => fatal_error(
                &format!("Failed to configure TLS server: {}", e),
                EXIT_CODE_TLS_ERROR,
            ),
        },
        (Some(_), None) => fatal_error(
            "TLS certificate specified but key is missing. Please provide both --tls-cert-path and --tls-key-path",
            EXIT_CODE_TLS_ERROR,
        ),
        (None, Some(_)) => fatal_error(
            "TLS key specified but certificate is missing. Please provide both --tls-cert-path and --tls-key-path",
            EXIT_CODE_TLS_ERROR,
        ),
        _ => {
            info!("Server binding to http://{}", bind_address);
            server.bind(bind_address)?
        }
    };

    server.workers(config.actix_workers).run().await
}

// TLS module if neither tls-rustls or tls-openssl are set.
#[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
mod tls {
    use actix_http::{Request, Response};
    use actix_service::{IntoServiceFactory, ServiceFactory};
    use actix_web::{body::MessageBody, dev::AppConfig, Error, HttpServer};
    pub fn configure_server<F, I, S, B>(
        _: HttpServer<F, I, S, B>,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> std::io::Result<HttpServer<F, I, S, B>>
    where
        F: Fn() -> I + Send + Clone + 'static,
        I: IntoServiceFactory<S, Request>,
        S: ServiceFactory<Request, Config = AppConfig> + 'static,
        S::Error: Into<Error>,
        S::InitError: std::fmt::Debug,
        S::Response: Into<Response<B>>,
        B: MessageBody + 'static,
    {
        Err(std::io::Error::other(
            "TLS certificate and key offered, but no TLS feature enabled during build. Please enable either `tls-rustls` or `tls-openssl` during build to use TLS"
        ))
    }
}

#[cfg(feature = "tls-rustls")]
mod tls {
    use actix_http::{Request, Response};
    use actix_service::{IntoServiceFactory, ServiceFactory};
    use actix_web::{body::MessageBody, dev::AppConfig, Error, HttpServer};
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
    ) -> std::io::Result<HttpServer<F, I, S, B>>
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
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to install crypto provider: {:?}", e),
                )
            })?;

        let cert_chain = CertificateDer::pem_file_iter(cert)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to read certificate file: {}", e),
                )
            })?
            .flatten()
            .collect();

        let key_der = PrivateKeyDer::from_pem_file(key).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to read key file: {}", e),
            )
        })?;

        let rustls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to configure TLS: {}", e),
                )
            })?;

        info!("Server binding with rustls to https://{}", bind_address);
        server
            .bind_rustls_0_23(bind_address, rustls_config)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to bind server: {}", e),
                )
            })
    }
}

#[cfg(feature = "tls-openssl")]
mod tls {
    use actix_http::{Request, Response};
    use actix_service::{IntoServiceFactory, ServiceFactory};
    use actix_web::{body::MessageBody, dev::AppConfig, Error, HttpServer};
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
    ) -> std::io::Result<HttpServer<F, I, S, B>>
    where
        F: Fn() -> I + Send + Clone + 'static,
        I: IntoServiceFactory<S, Request>,
        S: ServiceFactory<Request, Config = AppConfig> + 'static,
        S::Error: Into<Error>,
        S::InitError: std::fmt::Debug,
        S::Response: Into<Response<B>>,
        B: MessageBody + 'static,
    {
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("unable to create SSL acceptor: {}", e),
            )
        })?;

        if let Some(pass) = pass {
            let mut buf = Vec::new();
            File::open(key)?.read_to_end(&mut buf)?;
            let pkey =
                PKey::private_key_from_pem_passphrase(&buf, pass.as_bytes()).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unable to decrypt private key: {}", e),
                    )
                })?;
            builder.set_private_key(&pkey).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unable to set private key: {}", e),
                )
            })?;
        } else {
            builder
                .set_private_key_file(key, SslFiletype::PEM)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("unable to load private key file: {}", e),
                    )
                })?;
        }

        builder.set_certificate_chain_file(cert).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unable to load certificate chain: {}", e),
            )
        })?;

        info!("Server binding with openssl to https://{}", bind_address);
        server.bind_openssl(bind_address, builder).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to bind server: {}", e),
            )
        })
    }
}
