//! Bounded traversal hints. A finished pass is never a committed-data watermark:
//! the next pass must start again to revisit late commits and newly closed rows.

use super::*;
use crate::ApplicationFence;

/// Adapter-owned positions within one finite pass, bound to the original run,
/// scope, origin and application image. This carries no membership or authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "CursorInput")]
pub struct HistoryExpiryScanCursor {
    run: Uuid,
    origin: HistoryExpiryOrigin,
    key: HistoryPolicyKey,
    application: ApplicationFence,
    after: HistoryExpiryScanPosition,
    through: HistoryExpiryScanPosition,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorInput {
    run: Uuid,
    origin: HistoryExpiryOrigin,
    key: HistoryPolicyKey,
    application: ApplicationFence,
    after: HistoryExpiryScanPosition,
    through: HistoryExpiryScanPosition,
}

impl TryFrom<CursorInput> for HistoryExpiryScanCursor {
    type Error = StorageValidationError;

    fn try_from(value: CursorInput) -> Result<Self, Self::Error> {
        if value.run.get_variant() != uuid::Variant::RFC4122
            || value.run.get_version().is_none()
            || value.after.value() == 0
            || value.after >= value.through
        {
            return Err(invalid("Invalid bounded history scan continuation"));
        }
        Ok(Self {
            run: value.run,
            origin: value.origin,
            key: value.key,
            application: value.application,
            after: value.after,
            through: value.through,
        })
    }
}

impl HistoryExpiryScanCursor {
    pub(super) fn validate_request(
        &self,
        request: &HistoryExpiryAdmissionRequest,
    ) -> Result<(), StorageValidationError> {
        if self.run != request.run
            || self.origin != request.origin
            || self.key != request.scan.key()
            || self.application.incarnation() != request.fence.incarnation()
            || self.application.revision() as u64 != request.fence.deletion_revision().value()
        {
            return Err(invalid(
                "History scan continuation differs from its run, scope or application image",
            ));
        }
        Ok(())
    }
}

/// Restart after every finite pass, including passes that selected no batch.
/// A pass boundary says nothing about rows not yet committed, future closures,
/// changed protections, or the absence of eligible work.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "state",
    content = "cursor",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum HistoryExpiryScanProgress {
    Continue(HistoryExpiryScanCursor),
    PassFinished,
}

#[derive(Clone, Debug)]
pub struct HistoryExpiryAdmissionResult {
    manifest: Option<HistoryExpiryManifest>,
    scanned: u32,
    oversized_source_rows: u32,
    progress: HistoryExpiryScanProgress,
}

impl HistoryExpiryAdmissionResult {
    pub fn manifest(&self) -> Option<&HistoryExpiryManifest> {
        self.manifest.as_ref()
    }

    pub fn into_manifest(self) -> Option<HistoryExpiryManifest> {
        self.manifest
    }

    pub const fn scanned(&self) -> u32 {
        self.scanned
    }

    /// Examined rows that cannot fit even a fresh input/identity budget. They
    /// remain untouched, and are advanced past rather than pinning the scan.
    pub const fn oversized_source_rows(&self) -> u32 {
        self.oversized_source_rows
    }

    pub const fn progress(&self) -> &HistoryExpiryScanProgress {
        &self.progress
    }
}

/// Adapter boundary: observe every examined native source row before filtering.
/// Advance only after deciding that row; leave a deferred byte-budget suffix
/// for the next page. An individually oversized first row must still advance.
pub struct HistoryExpiryScanWindow<'a> {
    request: &'a HistoryExpiryAdmissionRequest,
    start: HistoryExpiryScanPosition,
    after: HistoryExpiryScanPosition,
    through: HistoryExpiryScanPosition,
    observed: HistoryExpiryScanPosition,
    scanned: u32,
    oversized_source_rows: u32,
}

impl<'a> HistoryExpiryScanWindow<'a> {
    pub(super) fn new(
        request: &'a HistoryExpiryAdmissionRequest,
        through: u64,
    ) -> Result<Self, StorageValidationError> {
        let through = HistoryExpiryScanPosition::new(through)?;
        let (after, through) = if let Some(cursor) = &request.continuation {
            cursor.validate_request(request)?;
            (cursor.after, cursor.through)
        } else {
            (HistoryExpiryScanPosition::new(0)?, through)
        };
        Ok(Self {
            request,
            start: after,
            after,
            through,
            observed: after,
            scanned: 0,
            oversized_source_rows: 0,
        })
    }

    pub const fn after(&self) -> u64 {
        self.after.value()
    }

    pub const fn through(&self) -> u64 {
        self.through.value()
    }

    pub fn observe(&mut self, position: u64) -> Result<(), StorageValidationError> {
        let position = HistoryExpiryScanPosition::new(position)?;
        if position <= self.observed
            || position > self.through
            || self.scanned >= self.request.scan.limit()
        {
            return Err(invalid(
                "History scan positions or examined-row budget are invalid",
            ));
        }
        self.observed = position;
        self.scanned += 1;
        Ok(())
    }

    pub fn advance(&mut self) {
        self.after = self.observed;
    }

    pub fn advance_oversized(&mut self) -> Result<(), StorageValidationError> {
        if self.observed <= self.after {
            return Err(invalid(
                "Oversized history outcome requires a newly observed row",
            ));
        }
        self.oversized_source_rows += 1;
        self.advance();
        Ok(())
    }

    pub fn finish(
        self,
        manifest: Option<HistoryExpiryManifest>,
        exhausted: bool,
    ) -> Result<HistoryExpiryAdmissionResult, StorageValidationError> {
        if let Some(manifest) = &manifest
            && (manifest.identity() != self.request.identity
                || manifest.run() != self.request.run
                || manifest.origin() != &self.request.origin
                || manifest.key() != self.request.scan.key()
                || manifest.fence() != self.request.fence
                || manifest.members().len() > self.scanned as usize)
        {
            return Err(invalid(
                "History scan result differs from its admission request",
            ));
        }
        let progress = if exhausted || self.after == self.through {
            HistoryExpiryScanProgress::PassFinished
        } else {
            if self.after == self.start {
                return Err(invalid("Bounded history scan made no progress"));
            }
            HistoryExpiryScanProgress::Continue(HistoryExpiryScanCursor {
                run: self.request.run,
                origin: self.request.origin.clone(),
                key: self.request.scan.key(),
                application: ApplicationFence::new(
                    self.request.fence.incarnation(),
                    self.request.fence.deletion_revision().value() as i64,
                )?,
                after: self.after,
                through: self.through,
            })
        };
        Ok(HistoryExpiryAdmissionResult {
            manifest,
            scanned: self.scanned,
            oversized_source_rows: self.oversized_source_rows,
            progress,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn request(limit: u32) -> HistoryExpiryAdmissionRequest {
        HistoryExpiryAdmissionRequest::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            HistoryExpiryOrigin::Scheduled {
                initiator: None,
                policy: None,
            },
            HistoryLifecycleScan::new(
                HistoryPolicyKey::new(
                    StorageIdentityHistoryKind::Object,
                    HistoryPolicyScope::Deployment,
                )
                .unwrap(),
                limit,
            )
            .unwrap(),
            HistoryReviewFence::new(
                Uuid::new_v4(),
                HistoryAuthorityRevision::new(0).unwrap(),
                HistoryDeletionRevision::new(0).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn cursor(request: &HistoryExpiryAdmissionRequest) -> HistoryExpiryScanCursor {
        let mut window = request.scan_window(10).unwrap();
        window.observe(1).unwrap();
        window.advance();
        match window.finish(None, false).unwrap().progress {
            HistoryExpiryScanProgress::Continue(cursor) => cursor,
            HistoryExpiryScanProgress::PassFinished => panic!("one row cannot finish this pass"),
        }
    }

    #[rstest]
    #[case::run("run")]
    #[case::scope("scope")]
    #[case::origin("origin")]
    #[case::incarnation("incarnation")]
    #[case::deletion("deletion")]
    fn continuation_preserves_original_binding(#[case] changed: &str) {
        let request = request(2);
        let cursor = cursor(&request);
        let mut other = request.clone();
        match changed {
            "run" => other.run = Uuid::new_v4(),
            "scope" => {
                other.scan = HistoryLifecycleScan::new(
                    HistoryPolicyKey::new(
                        StorageIdentityHistoryKind::Class,
                        HistoryPolicyScope::Deployment,
                    )
                    .unwrap(),
                    2,
                )
                .unwrap()
            }
            "origin" => {
                other.origin = HistoryExpiryOrigin::Manual {
                    principal: PrincipalIdentity::generate(),
                }
            }
            "incarnation" => {
                other.fence = HistoryReviewFence::new(
                    Uuid::new_v4(),
                    other.fence.authority_revision(),
                    other.fence.deletion_revision(),
                )
                .unwrap()
            }
            "deletion" => {
                other.fence = HistoryReviewFence::new(
                    other.fence.incarnation(),
                    other.fence.authority_revision(),
                    HistoryDeletionRevision::new(1).unwrap(),
                )
                .unwrap()
            }
            _ => unreachable!(),
        }
        assert!(other.continue_from(cursor).is_err());
    }

    #[test]
    fn policy_changes_and_new_rows_do_not_renew_a_finite_pass() {
        let request = request(2);
        let fence = HistoryReviewFence::new(
            request.fence().incarnation(),
            HistoryAuthorityRevision::new(1).unwrap(),
            request.fence().deletion_revision(),
        )
        .unwrap();
        let next = request
            .next_page(
                Uuid::new_v4(),
                fence,
                HistoryExpiryScanProgress::Continue(cursor(&request)),
            )
            .unwrap();
        let window = next.scan_window(100).unwrap();
        assert_eq!((window.after(), window.through()), (1, 10));
    }

    #[test]
    fn deferred_rows_are_charged_without_advancing_past_them() {
        let request = request(2);
        let mut window = request.scan_window(10).unwrap();
        window.observe(1).unwrap();
        window.advance();
        window.observe(2).unwrap();
        let result = window.finish(None, false).unwrap();
        let HistoryExpiryScanProgress::Continue(cursor) = result.progress() else {
            panic!("deferred row must remain")
        };
        assert_eq!((result.scanned(), cursor.after.value()), (2, 1));
    }

    #[test]
    fn finished_pass_restarts_to_revisit_late_rows() {
        let request = request(2);
        let mut window = request.scan_window(1).unwrap();
        window.observe(1).unwrap();
        window.advance();
        let result = window.finish(None, false).unwrap();
        let next = request
            .next_page(Uuid::new_v4(), request.fence(), result.progress().clone())
            .unwrap();
        let window = next.scan_window(20).unwrap();
        assert_eq!((window.after(), window.through()), (0, 20));
    }

    #[rstest]
    #[case::zero_start("after", serde_json::json!("0"))]
    #[case::reversed("after", serde_json::json!("11"))]
    #[case::finished("after", serde_json::json!("10"))]
    #[case::overflow("through", serde_json::json!(u64::MAX.to_string()))]
    #[case::noncanonical("through", serde_json::json!("010"))]
    #[case::number("through", serde_json::json!(10))]
    #[case::unknown("extra", serde_json::json!(true))]
    fn durable_cursor_rejects_invalid_fields(
        #[case] field: &str,
        #[case] value: serde_json::Value,
    ) {
        let mut encoded = serde_json::to_value(cursor(&request(2))).unwrap();
        encoded[field] = value;
        assert!(serde_json::from_value::<HistoryExpiryScanCursor>(encoded).is_err());
    }

    #[rstest]
    #[case::repeated(1)]
    #[case::outside_pass(11)]
    fn native_positions_must_advance_inside_the_finite_pass(#[case] next: u64) {
        let request = request(2);
        let mut window = request.scan_window(10).unwrap();
        window.observe(1).unwrap();
        assert!(window.observe(next).is_err());
    }

    #[test]
    fn skipped_rows_cannot_replenish_the_native_row_budget() {
        let request = request(1);
        let mut window = request.scan_window(10).unwrap();
        window.observe(1).unwrap();
        window.advance();
        assert!(window.observe(2).is_err());
    }

    #[test]
    fn bounded_pages_cannot_return_a_stalled_continuation() {
        let request = request(1);
        let mut window = request.scan_window(10).unwrap();
        window.observe(1).unwrap();
        assert!(window.finish(None, false).is_err());
    }

    #[test]
    fn oversized_source_outcome_is_explicit_and_advances_once() {
        let request = request(2);
        let mut window = request.scan_window(2).unwrap();
        assert!(window.advance_oversized().is_err());
        window.observe(1).unwrap();
        window.advance_oversized().unwrap();
        assert!(window.advance_oversized().is_err());
        let page = window.finish(None, false).unwrap();
        assert_eq!(page.oversized_source_rows(), 1);
        assert!(matches!(
            page.progress(),
            HistoryExpiryScanProgress::Continue(_)
        ));
    }
}
