#[tokio::main]
async fn main() -> Result<(), hubuum::ApiError> {
    hubuum::run_admin_from_environment().await
}
