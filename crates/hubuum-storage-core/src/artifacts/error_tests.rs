use std::error::Error;
use std::fmt;

use rstest::rstest;

use super::ArtifactError;

const PRIVATE: &str = "private-admission-evidence-must-not-be-printed";

// Intentionally neither Clone nor Eq: retaining a cause must not require
// copying protected evidence or inventing equality for arbitrary errors.
#[derive(Debug)]
struct Cause(u8);
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PRIVATE)
    }
}
impl Error for Cause {}

fn failure(compound: bool) -> ArtifactError {
    let primary = ArtifactError::Source(Box::new(Cause(1)));
    if compound {
        ArtifactError::AbortFailed {
            primary: Box::new(primary),
            abort: Box::new(ArtifactError::Source(Box::new(Cause(2)))),
        }
    } else {
        primary
    }
}

#[rstest]
#[case::direct(false)]
#[case::failed_abort(true)]
fn opaque_causality_is_redacted(#[case] compound: bool) {
    let error = failure(compound);
    assert!(!format!("{error}").contains(PRIVATE));
    assert!(!format!("{error:?}").contains(PRIVATE));
    assert!(!format!("{error:?}").contains("Cause("));
}

#[test]
fn source_can_be_consumed_without_cloning_original_evidence() {
    let ArtifactError::Source(source) = failure(false) else {
        panic!("opaque source expected");
    };
    assert_eq!(source.downcast::<Cause>().unwrap().0, 1);
}

#[test]
fn abort_pair_retains_both_causes_and_source_exposes_only_primary() {
    let error = failure(true);
    let primary = error
        .source()
        .unwrap()
        .downcast_ref::<ArtifactError>()
        .unwrap();
    assert_eq!(
        primary.source().unwrap().downcast_ref::<Cause>().unwrap().0,
        1
    );
    let abort = error.abort_error().unwrap();
    assert_eq!(
        abort.source().unwrap().downcast_ref::<Cause>().unwrap().0,
        2
    );
}

#[test]
fn opaque_failure_crosses_supervised_worker_boundaries() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<ArtifactError>();
}
