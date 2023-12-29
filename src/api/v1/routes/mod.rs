use actix_web::{get, web, Responder};

pub mod iam;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/iam").configure(iam::config))
        .service(foo);
}

#[get("/")]
async fn foo() -> impl Responder {
    "Hello, world!"
}
