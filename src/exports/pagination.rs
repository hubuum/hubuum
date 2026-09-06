use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::pagination::{CursorPaginated, encode_cursor, prepare_db_pagination};

pub(super) const CANDIDATE_PAGE_SIZE: usize = 128;
pub(super) const MAX_AUTHORIZATION_CANDIDATES: usize = 10_000;

/// Scan candidates in storage order, retaining only the requested authorized
/// output. Both the live batch and total candidate work have explicit bounds.
pub(super) async fn authorized_storage_page<T, Fetch, Authorize>(
    query: &QueryOptions,
    fetch: Fetch,
    authorize: Authorize,
) -> Result<Vec<T>, ApiError>
where
    T: CursorPaginated,
    Fetch: AsyncFn(QueryOptions) -> Result<Vec<T>, ApiError>,
    Authorize: AsyncFn(Vec<T>) -> Result<Vec<T>, ApiError>,
{
    let wanted = query
        .limit()
        .ok_or_else(|| ApiError::BadRequest("export queries require an output limit".into()))?;
    let mut options = prepare_db_pagination::<T>(query)?;
    options.set_include_total(false);
    let mut output = Vec::new();
    let mut scanned = 0;
    while output.len() < wanted {
        let page_size = CANDIDATE_PAGE_SIZE.min(MAX_AUTHORIZATION_CANDIDATES - scanned);
        options.set_limit(Some(page_size + 1))?;
        let mut candidates = fetch(options.clone()).await?;
        if candidates.len() > page_size + 1 {
            return Err(ApiError::InternalServerError(
                "storage exceeded the export candidate page limit".into(),
            ));
        }
        let has_more = candidates.len() > page_size;
        candidates.truncate(page_size);
        scanned += candidates.len();
        let cursor = candidates
            .last()
            .map(|row| encode_cursor(row, options.sort()))
            .transpose()?;
        output.extend(
            authorize(candidates)
                .await?
                .into_iter()
                .take(wanted - output.len()),
        );
        if !has_more {
            break;
        }
        if output.len() < wanted && scanned == MAX_AUTHORIZATION_CANDIDATES {
            return Err(ApiError::BadRequest(format!(
                "export authorization exceeded {MAX_AUTHORIZATION_CANDIDATES} candidates; narrow the export query"
            )));
        }
        options.set_cursor(cursor)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::search::{FilterField, SortParam};
    use crate::traits::CursorValue;
    use rstest::rstest;
    use std::cell::Cell;

    struct Candidate(i32);
    impl CursorPaginated for Candidate {
        fn supports_sort(field: &FilterField) -> bool {
            *field == FilterField::Id
        }
        fn cursor_value(&self, _: &FilterField) -> Result<CursorValue, ApiError> {
            Ok(CursorValue::Integer(i64::from(self.0)))
        }
        fn default_sort() -> Vec<SortParam> {
            vec![SortParam {
                field: FilterField::Id,
                descending: false,
            }]
        }
        fn tie_breaker_sort() -> Vec<SortParam> {
            Self::default_sort()
        }
    }

    #[rstest]
    #[case::early_stop(1, 1)]
    #[case::sparse_permissions(257, 3)]
    #[case::last_allowed_candidate(10_000, 79)]
    #[actix_web::test]
    async fn export_scans_only_until_visible_page_is_full(
        #[case] allowed_from: i32,
        #[case] expected_pages: usize,
    ) {
        let pages = Cell::new(0);
        let mut query = QueryOptions::empty();
        query.set_limit(Some(1)).unwrap();
        let result = authorized_storage_page(
            &query,
            async |options| {
                assert!(options.limit().unwrap() <= CANDIDATE_PAGE_SIZE + 1);
                assert_eq!(options.cursor().is_some(), pages.get() > 0);
                let start = pages.get() as i32 * CANDIDATE_PAGE_SIZE as i32 + 1;
                pages.set(pages.get() + 1);
                Ok((start..start + options.limit().unwrap() as i32)
                    .map(Candidate)
                    .collect())
            },
            async |candidates: Vec<Candidate>| {
                Ok(candidates
                    .into_iter()
                    .filter(|candidate| candidate.0 >= allowed_from)
                    .collect())
            },
        )
        .await
        .unwrap();
        assert_eq!(result[0].0, allowed_from);
        assert_eq!(pages.get(), expected_pages);
    }

    #[actix_web::test]
    async fn denied_export_stops_at_candidate_work_budget() {
        let pages = Cell::new(0);
        let authorized_candidates = Cell::new(0);
        let mut query = QueryOptions::empty();
        query.set_limit(Some(1)).unwrap();
        let started = std::time::Instant::now();
        let result = authorized_storage_page(
            &query,
            async |options| {
                let start = pages.get() * CANDIDATE_PAGE_SIZE;
                pages.set(pages.get() + 1);
                Ok((start..start + options.limit().unwrap())
                    .map(|id| Candidate(id as i32 + 1))
                    .collect())
            },
            async |candidates: Vec<Candidate>| {
                authorized_candidates.set(authorized_candidates.get() + candidates.len());
                Ok(vec![])
            },
        )
        .await;
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
        assert_eq!(authorized_candidates.get(), MAX_AUTHORIZATION_CANDIDATES);
        assert_eq!(
            pages.get(),
            MAX_AUTHORIZATION_CANDIDATES / CANDIDATE_PAGE_SIZE + 1
        );
        eprintln!(
            "PERFORMANCE_EVIDENCE {}",
            serde_json::json!({"scenario":"denied_export_candidates", "pages": pages.get(), "max_live_candidates": CANDIDATE_PAGE_SIZE + 1, "elapsed_us": started.elapsed().as_micros()})
        );
    }
}
