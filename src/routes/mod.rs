use actix_web::web;

mod auth;
mod classes;
mod iam;
mod objects;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam").configure(iam::config))
        .service(web::scope("/auth").configure(auth::config));
    //        .service(web::scope("/classes").configure(classes::config))
    //      .service(web::scope("/objects").configure(objects::config));
    // Add more routes as needed
}
