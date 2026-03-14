use actix_web::web;

use crate::api::v1::handlers::search;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(search::get_search)
        .service(search::stream_search);
}
