use crate::api::v1::handlers::{event_deliveries, event_sinks, event_subscriptions, events, me};
use actix_web::web;

pub mod backups;
pub mod classes;
pub mod collections;
pub mod export_templates;
pub mod exports;
pub mod groups;
pub mod imports;
pub mod relations;
pub mod remote_targets;
pub mod restores;
pub mod search;
pub mod tasks;
pub mod users;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam/users").configure(users::config))
        .service(web::scope("/iam/groups").configure(groups::config))
        .service(
            web::scope("/iam/service-accounts")
                .configure(crate::api::v1::handlers::service_accounts::config),
        )
        .service(
            web::scope("/iam/principals").configure(crate::api::v1::handlers::principals::config),
        )
        .service(web::scope("/iam/me").configure(me::config))
        .service(web::scope("/imports").configure(imports::config))
        .service(web::scope("/backups").configure(backups::config))
        .service(web::scope("/restores").configure(restores::config))
        .service(
            web::scope("/collections")
                .configure(collections::config)
                .configure(event_subscriptions::config),
        )
        .service(web::scope("/classes").configure(classes::config))
        .service(web::scope("/search").configure(search::config))
        .service(web::scope("/exports").configure(exports::config))
        .service(web::scope("/event-deliveries").configure(event_deliveries::config))
        .service(web::scope("/event-sinks").configure(event_sinks::config))
        .service(web::scope("/tasks").configure(tasks::config))
        .service(web::scope("/events").configure(events::config))
        .service(web::scope("/admin").configure(crate::api::v1::handlers::runtime_config::config))
        .service(web::scope("/export-templates").configure(export_templates::config))
        .service(web::scope("/remote-targets").configure(remote_targets::config))
        .service(web::scope("/relations").configure(relations::config));
}
