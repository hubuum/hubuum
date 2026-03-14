use actix_web::web;

pub mod classes;
pub mod groups;
pub mod imports;
pub mod namespaces;
pub mod relations;
pub mod reports;
pub mod search;
pub mod tasks;
pub mod templates;
pub mod users;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam/users").configure(users::config))
        .service(web::scope("/iam/groups").configure(groups::config))
        .service(web::scope("/imports").configure(imports::config))
        .service(web::scope("/namespaces").configure(namespaces::config))
        .service(web::scope("/classes").configure(classes::config))
        .service(web::scope("/search").configure(search::config))
        .service(web::scope("/reports").configure(reports::config))
        .service(web::scope("/tasks").configure(tasks::config))
        .service(web::scope("/templates").configure(templates::config))
        .service(web::scope("/relations").configure(relations::config));
}
