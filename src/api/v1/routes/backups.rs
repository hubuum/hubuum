use actix_web::web;

use crate::api::v1::handlers::backups;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(backups::create_backup)
        .service(backups::get_backup)
        .service(backups::get_backup_output);
}
