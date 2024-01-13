use actix_web::web;

pub mod groups;
pub mod namespaces;
pub mod users;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam/users").configure(users::config))
        .service(web::scope("/iam/groups").configure(groups::config))
        .service(web::scope("/namespaces").configure(namespaces::config));
}
