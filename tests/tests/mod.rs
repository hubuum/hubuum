pub use hubuum::tests::*;

use rstest::fixture;
use std::num::NonZeroUsize;

/// Functional task tests may need more polls on a remote, populated database.
pub fn task_poll_attempts() -> usize {
    std::env::var("HUBUUM_TEST_TASK_POLL_ATTEMPTS").map_or(200, |value| {
        value
            .parse::<NonZeroUsize>()
            .expect("HUBUUM_TEST_TASK_POLL_ATTEMPTS must be a positive integer")
            .get()
    })
}

#[fixture]
pub async fn test_context() -> TestContext {
    TestContext::new().await
}

#[fixture]
pub fn test_scope() -> TestScope {
    TestScope::new()
}
