use super::*;
use rstest::rstest;

#[rstest]
#[case(0, 900)]
#[case(301, 900)]
#[case(300, 299)]
#[case(300, 901)]
#[case(u32::MAX, u32::MAX)]
fn invalid_freshness_bounds_are_rejected(#[case] interval: u32, #[case] stale: u32) {
    assert_eq!(
        ExternalFreshnessPolicy::new(interval, stale),
        Err(ExternalAuthenticationError::InvalidFreshnessPolicy)
    );
}

#[rstest]
#[case(None, ExternalFreshness::Expired)]
#[case(Some(-1), ExternalFreshness::Expired)]
#[case(Some(0), ExternalFreshness::Current)]
#[case(Some(299), ExternalFreshness::Current)]
#[case(Some(300), ExternalFreshness::RefreshRequired)]
#[case(Some(899), ExternalFreshness::RefreshRequired)]
#[case(Some(900), ExternalFreshness::Expired)]
fn default_policy_bounds_upstream_age(
    #[case] age: Option<i64>,
    #[case] expected: ExternalFreshness,
) {
    let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
    let verified = age.map(|age| now - Duration::seconds(age));
    assert_eq!(
        ExternalFreshnessPolicy::default().evaluate(verified, now),
        expected
    );
}

#[test]
fn stricter_policy_expires_even_without_a_stale_grace_period() {
    let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
    let policy = ExternalFreshnessPolicy::new(10, 10).unwrap();
    assert!(
        !policy.allows_existing_authority_during_outage(Some(now - Duration::seconds(10)), now)
    );
}

#[test]
fn session_rotation_cannot_extend_upstream_age() {
    let verified = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
    let policy = ExternalFreshnessPolicy::default();
    // Issuing/rotating a Hubuum session has no input into this policy.
    let after_rotations = verified + Duration::seconds(901);
    assert!(!policy.allows_existing_authority_during_outage(Some(verified), after_rotations));
}
