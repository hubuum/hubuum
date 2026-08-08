#[cfg(test)]
mod container_build;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    hubuum::run_runtime_from_environment().await
}
