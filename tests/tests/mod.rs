pub use hubuum::tests::*;

use rstest::fixture;

#[fixture]
pub async fn test_context() -> TestContext {
    TestContext::new().await
}

#[fixture]
pub fn test_scope() -> TestScope {
    TestScope::new()
}
