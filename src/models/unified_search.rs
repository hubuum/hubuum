use crate::models::token_scope::TokenScope;
use crate::permissions::ClassResourceEndpoint;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::str::FromStr;

use base64::Engine;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::{Collection, HubuumClassExpanded, HubuumObject, Permissions};
use crate::pagination::{PageLimits, page_limits};
use crate::permissions::visibility::authorize_all_candidates;
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, PermissionBackend, PrincipalRef, ResourceRef,
};
use crate::services::unified_search as unified_search_service;
use crate::storage::StorageContext;
use crate::traits::AuthzSubject;
use crate::traits::scope_allows;
use crate::utilities::extensions::CustomStringExtensions;

const MAX_UNIFIED_SEARCH_QUERY_LENGTH: usize = 256;
const UNIFIED_SEARCH_CURSOR_VERSION: u8 = 1;
const UNIFIED_SEARCH_CANDIDATE_PAGE_SIZE: usize = 128;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash,
)]
#[serde(rename_all = "lowercase")]
pub enum UnifiedSearchKind {
    Collection,
    Class,
    Object,
}

impl UnifiedSearchKind {
    pub fn batch_key(self) -> &'static str {
        match self {
            Self::Collection => "collections",
            Self::Class => "classes",
            Self::Object => "objects",
        }
    }
}

impl FromStr for UnifiedSearchKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "collection" => Ok(Self::Collection),
            "class" => Ok(Self::Class),
            "object" => Ok(Self::Object),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid search kind: '{value}'"
            ))),
        }
    }
}

/// Opaque pagination cursor token for unified search. Encoded as a versioned
/// base64url payload by [`encode_cursor`] and recovered by [`decode_cursor`].
/// Fields are public so the codec can be exercised directly (for example, in
/// benchmarks).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnifiedSearchCursorToken {
    pub rank: i32,
    pub name: String,
    pub id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUnifiedSearchQuery {
    pub query: String,
    pub kinds: BTreeSet<UnifiedSearchKind>,
    pub limit_per_kind: usize,
    pub search_class_schema: bool,
    pub search_object_data: bool,
    collection_cursor: Option<UnifiedSearchCursorToken>,
    class_cursor: Option<UnifiedSearchCursorToken>,
    object_cursor: Option<UnifiedSearchCursorToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedSearchSpec {
    pub query: String,
    pub search_class_schema: bool,
    pub search_object_data: bool,
    pub limit_per_kind: usize,
    pub collection_cursor: Option<UnifiedSearchCursorToken>,
    pub class_cursor: Option<UnifiedSearchCursorToken>,
    pub object_cursor: Option<UnifiedSearchCursorToken>,
}

#[derive(Default)]
struct UnifiedSearchQueryParts {
    query: Option<String>,
    kinds: Option<BTreeSet<UnifiedSearchKind>>,
    limit_per_kind: Option<usize>,
    search_class_schema: Option<bool>,
    search_object_data: Option<bool>,
    collection_cursor: Option<UnifiedSearchCursorToken>,
    class_cursor: Option<UnifiedSearchCursorToken>,
    object_cursor: Option<UnifiedSearchCursorToken>,
}

impl StorageUnifiedSearchQuery {
    pub fn includes(&self, kind: UnifiedSearchKind) -> bool {
        self.kinds.contains(&kind)
    }

    pub fn search_spec(&self) -> UnifiedSearchSpec {
        UnifiedSearchSpec::from(self)
    }

    fn cursor_for(&self, kind: UnifiedSearchKind) -> Option<&UnifiedSearchCursorToken> {
        match kind {
            UnifiedSearchKind::Collection => self.collection_cursor.as_ref(),
            UnifiedSearchKind::Class => self.class_cursor.as_ref(),
            UnifiedSearchKind::Object => self.object_cursor.as_ref(),
        }
    }
}

impl From<&StorageUnifiedSearchQuery> for UnifiedSearchSpec {
    fn from(value: &StorageUnifiedSearchQuery) -> Self {
        Self {
            query: value.query.clone(),
            search_class_schema: value.search_class_schema,
            search_object_data: value.search_object_data,
            limit_per_kind: value.limit_per_kind,
            collection_cursor: value.collection_cursor.clone(),
            class_cursor: value.class_cursor.clone(),
            object_cursor: value.object_cursor.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchResults {
    pub collections: Vec<Collection>,
    pub classes: Vec<HubuumClassExpanded>,
    pub objects: Vec<HubuumObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchNext {
    pub collections: Option<String>,
    pub classes: Option<String>,
    pub objects: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchResponse {
    pub query: String,
    pub results: UnifiedSearchResults,
    pub next: UnifiedSearchNext,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchBatchResponse {
    pub kind: String,
    pub collections: Vec<Collection>,
    pub classes: Vec<HubuumClassExpanded>,
    pub objects: Vec<HubuumObject>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchStartedEvent {
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchDoneEvent {
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchErrorEvent {
    pub message: String,
}

fn reject_duplicate<T>(slot: &Option<T>, name: &str) -> Result<(), ApiError> {
    if slot.is_some() {
        return Err(ApiError::BadRequest(format!("duplicate {name}")));
    }
    Ok(())
}

fn parse_kinds(value: &str) -> Result<BTreeSet<UnifiedSearchKind>, ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::BadRequest("kinds must not be empty".to_string()));
    }

    let mut parsed = BTreeSet::new();
    for kind in value.split(',') {
        parsed.insert(UnifiedSearchKind::from_str(kind.trim())?);
    }
    Ok(parsed)
}

fn parse_required_query(value: &str) -> Result<String, ApiError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(ApiError::BadRequest("q must not be empty".to_string()));
    }
    if trimmed.chars().count() > MAX_UNIFIED_SEARCH_QUERY_LENGTH {
        return Err(ApiError::BadRequest(format!(
            "q must be at most {MAX_UNIFIED_SEARCH_QUERY_LENGTH} characters"
        )));
    }
    Ok(trimmed)
}

impl UnifiedSearchQueryParts {
    fn apply(&mut self, key: &str, value: Cow<'_, str>) -> Result<(), ApiError> {
        match key {
            "q" => {
                reject_duplicate(&self.query, "q")?;
                self.query = Some(parse_required_query(value.as_ref())?);
            }
            "kinds" => {
                reject_duplicate(&self.kinds, "kinds")?;
                self.kinds = Some(parse_kinds(value.as_ref())?);
            }
            "limit_per_kind" => {
                reject_duplicate(&self.limit_per_kind, "limit_per_kind")?;
                let parsed_limit = value.as_ref().parse::<usize>().map_err(|error| {
                    ApiError::BadRequest(format!("bad limit_per_kind: {error}"))
                })?;
                // Validation against the max limit is deferred to `build`, which
                // receives the limits explicitly and keeps parsing config-free.
                self.limit_per_kind = Some(parsed_limit);
            }
            "search_class_schema" => {
                reject_duplicate(&self.search_class_schema, "search_class_schema")?;
                self.search_class_schema = Some(value.as_ref().as_boolean()?);
            }
            "search_object_data" => {
                reject_duplicate(&self.search_object_data, "search_object_data")?;
                self.search_object_data = Some(value.as_ref().as_boolean()?);
            }
            "cursor_collections" => {
                reject_duplicate(&self.collection_cursor, "cursor_collections")?;
                self.collection_cursor = Some(decode_cursor(value.as_ref())?);
            }
            "cursor_classes" => {
                reject_duplicate(&self.class_cursor, "cursor_classes")?;
                self.class_cursor = Some(decode_cursor(value.as_ref())?);
            }
            "cursor_objects" => {
                reject_duplicate(&self.object_cursor, "cursor_objects")?;
                self.object_cursor = Some(decode_cursor(value.as_ref())?);
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid query parameter: '{key}'"
                )));
            }
        }

        Ok(())
    }

    fn build(self, page_limits: PageLimits) -> Result<StorageUnifiedSearchQuery, ApiError> {
        let limit_per_kind = page_limits.resolve(self.limit_per_kind)?;

        Ok(StorageUnifiedSearchQuery {
            query: self
                .query
                .ok_or_else(|| ApiError::BadRequest("missing q".to_string()))?,
            kinds: self.kinds.unwrap_or_else(default_kinds),
            limit_per_kind,
            search_class_schema: self.search_class_schema.unwrap_or(false),
            search_object_data: self.search_object_data.unwrap_or(false),
            collection_cursor: self.collection_cursor,
            class_cursor: self.class_cursor,
            object_cursor: self.object_cursor,
        })
    }
}

pub fn parse_unified_search_query(qs: &str) -> Result<StorageUnifiedSearchQuery, ApiError> {
    parse_unified_search_query_with_limits(qs, page_limits()?)
}

/// Config-free variant of [`parse_unified_search_query`]. The page limits are
/// supplied by the caller instead of being read from the global configuration,
/// which keeps the whole parse path free of global state (used by benchmarks and
/// any caller that already holds the limits).
pub fn parse_unified_search_query_with_limits(
    qs: &str,
    page_limits: PageLimits,
) -> Result<StorageUnifiedSearchQuery, ApiError> {
    let mut parts = UnifiedSearchQueryParts::default();

    if !qs.is_empty() {
        for chunk in qs.split('&') {
            let (key, value) = hubuum_query::decode_query_parameter_pair(chunk)?;
            parts.apply(key.as_ref(), value)?;
        }
    }

    parts.build(page_limits)
}

fn default_kinds() -> BTreeSet<UnifiedSearchKind> {
    BTreeSet::from([
        UnifiedSearchKind::Collection,
        UnifiedSearchKind::Class,
        UnifiedSearchKind::Object,
    ])
}

/// Serialize a unified-search cursor token to its base64url wire form.
pub fn encode_cursor(token: &UnifiedSearchCursorToken) -> Result<String, ApiError> {
    let mut bytes = Vec::with_capacity(9 + token.name.len());
    bytes.push(UNIFIED_SEARCH_CURSOR_VERSION);
    bytes.extend_from_slice(&token.rank.to_be_bytes());
    bytes.extend_from_slice(&token.id.to_be_bytes());
    bytes.extend_from_slice(token.name.as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// Recover a unified-search cursor token from its base64url wire form.
pub fn decode_cursor(cursor: &str) -> Result<UnifiedSearchCursorToken, ApiError> {
    let mut bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| ApiError::BadRequest(format!("Invalid search cursor: {error}")))?;
    if bytes.first() == Some(&b'{') {
        return serde_json::from_slice::<UnifiedSearchCursorToken>(&bytes)
            .map_err(|error| ApiError::BadRequest(format!("Invalid search cursor: {error}")));
    }
    if bytes.len() < 9 || bytes[0] != UNIFIED_SEARCH_CURSOR_VERSION {
        return Err(ApiError::BadRequest(
            "Invalid search cursor: unsupported cursor format".to_string(),
        ));
    }

    let rank = i32::from_be_bytes(
        bytes[1..5]
            .try_into()
            .expect("validated unified-search cursor header"),
    );
    let id = i32::from_be_bytes(
        bytes[5..9]
            .try_into()
            .expect("validated unified-search cursor header"),
    );
    bytes.drain(..9);
    let name = String::from_utf8(bytes)
        .map_err(|error| ApiError::BadRequest(format!("Invalid search cursor: {error}")))?;
    Ok(UnifiedSearchCursorToken { rank, name, id })
}

struct SearchPage<T> {
    items: Vec<T>,
    next: Option<String>,
}

fn paginate_scored<T>(
    mut scored: Vec<(UnifiedSearchCursorToken, T)>,
    cursor: Option<&UnifiedSearchCursorToken>,
    limit: usize,
    storage_has_more: bool,
) -> Result<SearchPage<T>, ApiError> {
    scored.sort_by(|left, right| left.0.cmp(&right.0));

    if let Some(cursor) = cursor {
        scored.retain(|(token, _)| token > cursor);
    }

    let has_more = storage_has_more || scored.len() > limit;
    if has_more {
        scored.truncate(limit);
    }

    let next = if has_more {
        scored
            .last()
            .map(|(token, _)| encode_cursor(token))
            .transpose()?
    } else {
        None
    };

    Ok(SearchPage {
        items: scored.into_iter().map(|(_, item)| item).collect(),
        next,
    })
}

async fn search_collections<C, S>(
    user: &S,
    backend: &C,
    params: &StorageUnifiedSearchQuery,
    search_spec: &UnifiedSearchSpec,
    scopes: Option<&TokenScope>,
    is_admin: bool,
    authorization: Option<(&dyn PermissionBackend, &PrincipalRef)>,
) -> Result<SearchPage<Collection>, ApiError>
where
    C: StorageContext,
    S: AuthzSubject + ?Sized,
{
    let (rows, storage_has_more) = if let Some((permission_backend, principal)) = authorization {
        if !scope_allows(scopes, &[Permissions::ReadCollection]) {
            (Vec::new(), false)
        } else {
            let mut candidate_spec = search_spec.clone();
            candidate_spec.limit_per_kind = UNIFIED_SEARCH_CANDIDATE_PAGE_SIZE;
            let mut authorized = Vec::new();
            loop {
                let page = unified_search_service::search_collections(
                    backend,
                    user.principal_id(),
                    true,
                    None,
                    &candidate_spec,
                )
                .await?;
                let next_candidate_cursor =
                    page.items.last().map(|candidate| candidate.cursor.clone());
                let page_has_more = page.has_more;
                let allowed = authorize_all_candidates(
                    permission_backend,
                    principal,
                    page.items,
                    scopes,
                    vec![Permissions::ReadCollection],
                    |candidate| ResourceRef::collection(candidate.item.id),
                )
                .await?;
                authorized.extend(allowed);
                if authorized.len() > params.limit_per_kind || !page_has_more {
                    break;
                }
                candidate_spec.collection_cursor =
                    Some(next_candidate_cursor.ok_or_else(|| {
                        ApiError::InternalServerError(
                            "Collection search candidate page cannot advance its cursor"
                                .to_string(),
                        )
                    })?);
            }
            (authorized, false)
        }
    } else {
        let page = unified_search_service::search_collections(
            backend,
            user.principal_id(),
            is_admin,
            scopes,
            search_spec,
        )
        .await?;
        (page.items, page.has_more)
    };
    if rows.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next: None,
        });
    }

    let scored = rows
        .into_iter()
        .map(|candidate| (candidate.cursor, candidate.item))
        .collect();

    paginate_scored(
        scored,
        params.cursor_for(UnifiedSearchKind::Collection),
        params.limit_per_kind,
        storage_has_more,
    )
}

async fn search_classes<C, S>(
    user: &S,
    backend: &C,
    params: &StorageUnifiedSearchQuery,
    search_spec: &UnifiedSearchSpec,
    scopes: Option<&TokenScope>,
    is_admin: bool,
    authorization: Option<(&dyn PermissionBackend, &PrincipalRef)>,
) -> Result<SearchPage<HubuumClassExpanded>, ApiError>
where
    C: StorageContext,
    S: AuthzSubject + ?Sized,
{
    let (rows, storage_has_more) = if let Some((permission_backend, principal)) = authorization {
        if !scope_allows(scopes, &[Permissions::ReadClass]) {
            (Vec::new(), false)
        } else {
            let mut candidate_spec = search_spec.clone();
            candidate_spec.limit_per_kind = UNIFIED_SEARCH_CANDIDATE_PAGE_SIZE;
            let mut authorized = Vec::new();
            loop {
                let page = unified_search_service::search_classes(
                    backend,
                    user.principal_id(),
                    true,
                    None,
                    &candidate_spec,
                )
                .await?;
                let next_candidate_cursor =
                    page.items.last().map(|candidate| candidate.cursor.clone());
                let page_has_more = page.has_more;
                let allowed = authorize_all_candidates(
                    permission_backend,
                    principal,
                    page.items,
                    scopes,
                    vec![Permissions::ReadClass],
                    |candidate| {
                        ResourceRef::class(
                            candidate.item.id,
                            candidate.item.collection.id,
                            Some(candidate.item.name.clone()),
                        )
                    },
                )
                .await?;
                authorized.extend(allowed);
                if authorized.len() > params.limit_per_kind || !page_has_more {
                    break;
                }
                candidate_spec.class_cursor = Some(next_candidate_cursor.ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Class search candidate page cannot advance its cursor".to_string(),
                    )
                })?);
            }
            (authorized, false)
        }
    } else {
        let page = unified_search_service::search_classes(
            backend,
            user.principal_id(),
            is_admin,
            scopes,
            search_spec,
        )
        .await?;
        (page.items, page.has_more)
    };
    if rows.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next: None,
        });
    }

    let scored = rows
        .into_iter()
        .map(|candidate| (candidate.cursor, candidate.item))
        .collect::<Vec<_>>();

    let page = paginate_scored(
        scored,
        params.cursor_for(UnifiedSearchKind::Class),
        params.limit_per_kind,
        storage_has_more,
    )?;
    Ok(SearchPage {
        items: page.items,
        next: page.next,
    })
}

async fn search_objects<C, S>(
    user: &S,
    backend: &C,
    params: &StorageUnifiedSearchQuery,
    search_spec: &UnifiedSearchSpec,
    scopes: Option<&TokenScope>,
    is_admin: bool,
    authorization: Option<(&dyn PermissionBackend, &PrincipalRef)>,
) -> Result<SearchPage<HubuumObject>, ApiError>
where
    C: StorageContext,
    S: AuthzSubject + ?Sized,
{
    let (rows, storage_has_more) = if let Some((permission_backend, principal)) = authorization {
        if !scope_allows(scopes, &[Permissions::ReadObject]) {
            (Vec::new(), false)
        } else {
            let mut candidate_spec = search_spec.clone();
            candidate_spec.limit_per_kind = UNIFIED_SEARCH_CANDIDATE_PAGE_SIZE;
            let mut authorized = Vec::new();
            loop {
                let page = unified_search_service::search_objects(
                    backend,
                    user.principal_id(),
                    true,
                    None,
                    &candidate_spec,
                )
                .await?;
                let next_candidate_cursor =
                    page.items.last().map(|candidate| candidate.cursor.clone());
                let page_has_more = page.has_more;
                let allowed = authorize_all_candidates(
                    permission_backend,
                    principal,
                    page.items,
                    scopes,
                    vec![Permissions::ReadObject],
                    |candidate| {
                        ResourceRef::object(
                            candidate.item.id,
                            ClassResourceEndpoint::new(
                                candidate.item.collection_id,
                                candidate.item.hubuum_class_id,
                            ),
                            Some(candidate.item.name.clone()),
                        )
                    },
                )
                .await?;
                authorized.extend(allowed);
                if authorized.len() > params.limit_per_kind || !page_has_more {
                    break;
                }
                candidate_spec.object_cursor = Some(next_candidate_cursor.ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Object search candidate page cannot advance its cursor".to_string(),
                    )
                })?);
            }
            (authorized, false)
        }
    } else {
        let page = unified_search_service::search_objects(
            backend,
            user.principal_id(),
            is_admin,
            scopes,
            search_spec,
        )
        .await?;
        (page.items, page.has_more)
    };
    if rows.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next: None,
        });
    }

    let scored = rows
        .into_iter()
        .map(|candidate| (candidate.cursor, candidate.item))
        .collect::<Vec<_>>();

    paginate_scored(
        scored,
        params.cursor_for(UnifiedSearchKind::Object),
        params.limit_per_kind,
        storage_has_more,
    )
}

pub async fn execute_unified_search<C, S>(
    user: &S,
    backend: &C,
    params: &StorageUnifiedSearchQuery,
    scopes: Option<&TokenScope>,
) -> Result<UnifiedSearchResponse, ApiError>
where
    C: AuthorizationContext,
    S: AuthzSubject + ?Sized,
{
    let search_spec = params.search_spec();
    let external_backend = match backend.authorization_mode() {
        AuthorizationMode::Delegated(permission_backend)
            if !permission_backend.supports_storage_visibility_filtering() =>
        {
            Some(permission_backend)
        }
        AuthorizationMode::LocalStorage | AuthorizationMode::Delegated(_) => None,
    };
    let principal = if external_backend.is_some() {
        Some(PrincipalRef::load(backend, user).await?)
    } else {
        None
    };
    let is_admin = if external_backend.is_some() {
        true
    } else {
        crate::traits::AuthzSubject::is_admin(user, backend).await?
    };
    let authorization = external_backend.zip(principal.as_ref());
    let collections_future = async {
        if params.includes(UnifiedSearchKind::Collection) {
            search_collections(
                user,
                backend,
                params,
                &search_spec,
                scopes,
                is_admin,
                authorization,
            )
            .await
        } else {
            Ok(SearchPage {
                items: vec![],
                next: None,
            })
        }
    };
    let classes_future = async {
        if params.includes(UnifiedSearchKind::Class) {
            search_classes(
                user,
                backend,
                params,
                &search_spec,
                scopes,
                is_admin,
                authorization,
            )
            .await
        } else {
            Ok(SearchPage {
                items: vec![],
                next: None,
            })
        }
    };
    let objects_future = async {
        if params.includes(UnifiedSearchKind::Object) {
            search_objects(
                user,
                backend,
                params,
                &search_spec,
                scopes,
                is_admin,
                authorization,
            )
            .await
        } else {
            Ok(SearchPage {
                items: vec![],
                next: None,
            })
        }
    };
    let (collections, classes, objects) =
        tokio::try_join!(collections_future, classes_future, objects_future)?;

    Ok(UnifiedSearchResponse {
        query: params.query.clone(),
        results: UnifiedSearchResults {
            collections: collections.items,
            classes: classes.items,
            objects: objects.items,
        },
        next: UnifiedSearchNext {
            collections: collections.next,
            classes: classes.next,
            objects: objects.next,
        },
    })
}

pub async fn execute_unified_search_batch<C, S>(
    user: &S,
    backend: &C,
    params: &StorageUnifiedSearchQuery,
    kind: UnifiedSearchKind,
    scopes: Option<&TokenScope>,
) -> Result<UnifiedSearchBatchResponse, ApiError>
where
    C: AuthorizationContext,
    S: AuthzSubject + ?Sized,
{
    let search_spec = params.search_spec();
    let external_backend = match backend.authorization_mode() {
        AuthorizationMode::Delegated(permission_backend)
            if !permission_backend.supports_storage_visibility_filtering() =>
        {
            Some(permission_backend)
        }
        AuthorizationMode::LocalStorage | AuthorizationMode::Delegated(_) => None,
    };
    let principal = if external_backend.is_some() {
        Some(PrincipalRef::load(backend, user).await?)
    } else {
        None
    };
    let is_admin = if external_backend.is_some() {
        true
    } else {
        crate::traits::AuthzSubject::is_admin(user, backend).await?
    };
    let authorization = external_backend.zip(principal.as_ref());
    match kind {
        UnifiedSearchKind::Collection => {
            let page = search_collections(
                user,
                backend,
                params,
                &search_spec,
                scopes,
                is_admin,
                authorization,
            )
            .await?;
            Ok(UnifiedSearchBatchResponse {
                kind: kind.batch_key().to_string(),
                collections: page.items,
                classes: vec![],
                objects: vec![],
                next: page.next,
            })
        }
        UnifiedSearchKind::Class => {
            let page = search_classes(
                user,
                backend,
                params,
                &search_spec,
                scopes,
                is_admin,
                authorization,
            )
            .await?;
            Ok(UnifiedSearchBatchResponse {
                kind: kind.batch_key().to_string(),
                collections: vec![],
                classes: page.items,
                objects: vec![],
                next: page.next,
            })
        }
        UnifiedSearchKind::Object => {
            let page = search_objects(
                user,
                backend,
                params,
                &search_spec,
                scopes,
                is_admin,
                authorization,
            )
            .await?;
            Ok(UnifiedSearchBatchResponse {
                kind: kind.batch_key().to_string(),
                collections: vec![],
                classes: vec![],
                objects: page.items,
                next: page.next,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unified_search_defaults() {
        let parsed = parse_unified_search_query("q=server").unwrap();
        assert_eq!(parsed.query, "server");
        assert_eq!(
            parsed.limit_per_kind,
            page_limits().unwrap().default_limit()
        );
        assert!(parsed.includes(UnifiedSearchKind::Collection));
        assert!(parsed.includes(UnifiedSearchKind::Class));
        assert!(parsed.includes(UnifiedSearchKind::Object));
        assert!(!parsed.search_class_schema);
        assert!(!parsed.search_object_data);
    }

    #[test]
    fn parse_unified_search_decodes_form_encoded_query_text() {
        let parsed = parse_unified_search_query("q=core+router%2Fedge").unwrap();

        assert_eq!(parsed.query, "core router/edge");
    }

    #[test]
    fn parse_unified_search_flags_and_cursor() {
        let cursor = encode_cursor(&UnifiedSearchCursorToken {
            rank: 2,
            name: "server".to_string(),
            id: 42,
        })
        .unwrap();
        let parsed = parse_unified_search_query(&format!(
            "q=server&kinds=class,object&limit_per_kind=5&search_class_schema=true&search_object_data=true&cursor_classes={cursor}"
        ))
        .unwrap();
        assert!(!parsed.includes(UnifiedSearchKind::Collection));
        assert!(parsed.includes(UnifiedSearchKind::Class));
        assert!(parsed.includes(UnifiedSearchKind::Object));
        assert!(parsed.search_class_schema);
        assert!(parsed.search_object_data);
        assert_eq!(parsed.limit_per_kind, 5);
        assert!(parsed.class_cursor.is_some());
    }

    #[test]
    fn unified_search_query_reduces_to_backend_spec() {
        let cursor = encode_cursor(&UnifiedSearchCursorToken {
            rank: 1,
            name: "server".to_string(),
            id: 7,
        })
        .unwrap();
        let parsed = parse_unified_search_query(&format!(
            "q=server&kinds=class&limit_per_kind=5&search_class_schema=true&search_object_data=true&cursor_classes={cursor}"
        ))
        .unwrap();

        let spec = parsed.search_spec();

        assert_eq!(
            spec,
            UnifiedSearchSpec {
                query: "server".to_string(),
                search_class_schema: true,
                search_object_data: true,
                limit_per_kind: 5,
                collection_cursor: None,
                class_cursor: parsed.class_cursor.clone(),
                object_cursor: None,
            }
        );
    }

    #[test]
    fn parse_unified_search_rejects_unknown_parameter() {
        let error = parse_unified_search_query("q=server&foo=bar").unwrap_err();
        assert_eq!(error.to_string(), "Invalid query parameter: 'foo'");
    }

    #[test]
    fn parse_unified_search_rejects_oversized_query() {
        let query = "a".repeat(MAX_UNIFIED_SEARCH_QUERY_LENGTH + 1);
        let error = parse_unified_search_query(&format!("q={query}")).unwrap_err();
        assert_eq!(error.to_string(), "q must be at most 256 characters");
    }

    #[test]
    fn parse_with_limits_uses_default_when_absent() {
        let page_limits = PageLimits::new(25, 100).unwrap();
        let parsed = parse_unified_search_query_with_limits("q=server", page_limits).unwrap();
        assert_eq!(parsed.limit_per_kind, 25);
    }

    #[test]
    fn parse_with_limits_validates_against_provided_max() {
        let page_limits = PageLimits::new(25, 100).unwrap();
        let parsed =
            parse_unified_search_query_with_limits("q=server&limit_per_kind=50", page_limits)
                .unwrap();
        assert_eq!(parsed.limit_per_kind, 50);

        let clamped =
            parse_unified_search_query_with_limits("q=server&limit_per_kind=101", page_limits)
                .unwrap();
        assert_eq!(clamped.limit_per_kind, 100);
    }

    #[test]
    fn cursor_round_trips_through_encode_and_decode() {
        let token = UnifiedSearchCursorToken {
            rank: 2,
            name: "asset-001".to_string(),
            id: 4242,
        };
        let encoded = encode_cursor(&token).unwrap();
        assert_eq!(decode_cursor(&encoded).unwrap(), token);
    }

    #[test]
    fn cursor_decoder_accepts_legacy_json_tokens() {
        let token = UnifiedSearchCursorToken {
            rank: 2,
            name: "asset-001".to_string(),
            id: 4242,
        };
        let legacy = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&token).unwrap());

        assert_eq!(decode_cursor(&legacy).unwrap(), token);
    }
}
