use hubuum::errors::ApiError;

#[tokio::main]
async fn main() -> Result<(), ApiError> {
    hubuum::run_admin_from_environment().await
}
