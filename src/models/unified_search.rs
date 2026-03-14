use std::collections::BTreeSet;
use std::str::FromStr;

use base64::Engine;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::{HubuumClassExpanded, HubuumObject, Namespace, User};
use crate::pagination::{page_limits, validate_page_limit};
use crate::traits::{BackendContext, Search};
use crate::utilities::extensions::CustomStringExtensions;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema, Hash,
)]
#[serde(rename_all = "lowercase")]
pub enum UnifiedSearchKind {
    Namespace,
    Class,
    Object,
}

impl UnifiedSearchKind {
    pub fn batch_key(self) -> &'static str {
        match self {
            Self::Namespace => "namespaces",
            Self::Class => "classes",
            Self::Object => "objects",
        }
    }
}

impl FromStr for UnifiedSearchKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "namespace" => Ok(Self::Namespace),
            "class" => Ok(Self::Class),
            "object" => Ok(Self::Object),
            _ => Err(ApiError::BadRequest(format!(
                "Invalid search kind: '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct UnifiedSearchCursorToken {
    rank: i32,
    name: String,
    id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedSearchQuery {
    pub query: String,
    pub kinds: BTreeSet<UnifiedSearchKind>,
    pub limit_per_kind: usize,
    pub search_class_schema: bool,
    pub search_object_data: bool,
    namespace_cursor: Option<UnifiedSearchCursorToken>,
    class_cursor: Option<UnifiedSearchCursorToken>,
    object_cursor: Option<UnifiedSearchCursorToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedSearchSpec {
    pub query: String,
    pub search_class_schema: bool,
    pub search_object_data: bool,
}

#[derive(Default)]
struct UnifiedSearchQueryParts {
    query: Option<String>,
    kinds: Option<BTreeSet<UnifiedSearchKind>>,
    limit_per_kind: Option<usize>,
    search_class_schema: Option<bool>,
    search_object_data: Option<bool>,
    namespace_cursor: Option<UnifiedSearchCursorToken>,
    class_cursor: Option<UnifiedSearchCursorToken>,
    object_cursor: Option<UnifiedSearchCursorToken>,
}

impl UnifiedSearchQuery {
    pub fn includes(&self, kind: UnifiedSearchKind) -> bool {
        self.kinds.contains(&kind)
    }

    pub fn search_spec(&self) -> UnifiedSearchSpec {
        UnifiedSearchSpec::from(self)
    }

    fn cursor_for(&self, kind: UnifiedSearchKind) -> Option<&UnifiedSearchCursorToken> {
        match kind {
            UnifiedSearchKind::Namespace => self.namespace_cursor.as_ref(),
            UnifiedSearchKind::Class => self.class_cursor.as_ref(),
            UnifiedSearchKind::Object => self.object_cursor.as_ref(),
        }
    }
}

impl From<&UnifiedSearchQuery> for UnifiedSearchSpec {
    fn from(value: &UnifiedSearchQuery) -> Self {
        Self {
            query: value.query.clone(),
            search_class_schema: value.search_class_schema,
            search_object_data: value.search_object_data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchResults {
    pub namespaces: Vec<Namespace>,
    pub classes: Vec<HubuumClassExpanded>,
    pub objects: Vec<HubuumObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UnifiedSearchNext {
    pub namespaces: Option<String>,
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
    pub namespaces: Vec<Namespace>,
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

fn parse_query_chunk(chunk: &str) -> Result<(&str, String), ApiError> {
    let parts: Vec<_> = chunk.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(ApiError::BadRequest(format!(
            "Invalid query parameter: '{chunk}'"
        )));
    }

    let value = percent_encoding::percent_decode(parts[1].as_bytes())
        .decode_utf8()
        .map_err(|error| {
            ApiError::BadRequest(format!(
                "Invalid query parameter: '{chunk}', invalid value: {error}",
            ))
        })?
        .to_string();

    Ok((parts[0], value))
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
    Ok(trimmed)
}

impl UnifiedSearchQueryParts {
    fn apply(&mut self, key: &str, value: String) -> Result<(), ApiError> {
        match key {
            "q" => {
                reject_duplicate(&self.query, "q")?;
                self.query = Some(parse_required_query(&value)?);
            }
            "kinds" => {
                reject_duplicate(&self.kinds, "kinds")?;
                self.kinds = Some(parse_kinds(&value)?);
            }
            "limit_per_kind" => {
                reject_duplicate(&self.limit_per_kind, "limit_per_kind")?;
                let parsed_limit = value.parse::<usize>().map_err(|error| {
                    ApiError::BadRequest(format!("bad limit_per_kind: {error}"))
                })?;
                self.limit_per_kind = Some(validate_page_limit(parsed_limit)?);
            }
            "search_class_schema" => {
                reject_duplicate(&self.search_class_schema, "search_class_schema")?;
                self.search_class_schema = Some(value.as_boolean()?);
            }
            "search_object_data" => {
                reject_duplicate(&self.search_object_data, "search_object_data")?;
                self.search_object_data = Some(value.as_boolean()?);
            }
            "cursor_namespaces" => {
                reject_duplicate(&self.namespace_cursor, "cursor_namespaces")?;
                self.namespace_cursor = Some(decode_cursor(&value)?);
            }
            "cursor_classes" => {
                reject_duplicate(&self.class_cursor, "cursor_classes")?;
                self.class_cursor = Some(decode_cursor(&value)?);
            }
            "cursor_objects" => {
                reject_duplicate(&self.object_cursor, "cursor_objects")?;
                self.object_cursor = Some(decode_cursor(&value)?);
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Invalid query parameter: '{key}'"
                )));
            }
        }

        Ok(())
    }

    fn build(self) -> Result<UnifiedSearchQuery, ApiError> {
        Ok(UnifiedSearchQuery {
            query: self
                .query
                .ok_or_else(|| ApiError::BadRequest("missing q".to_string()))?,
            kinds: self.kinds.unwrap_or_else(default_kinds),
            limit_per_kind: self.limit_per_kind.unwrap_or(page_limits()?.0),
            search_class_schema: self.search_class_schema.unwrap_or(false),
            search_object_data: self.search_object_data.unwrap_or(false),
            namespace_cursor: self.namespace_cursor,
            class_cursor: self.class_cursor,
            object_cursor: self.object_cursor,
        })
    }
}

pub fn parse_unified_search_query(qs: &str) -> Result<UnifiedSearchQuery, ApiError> {
    let mut parts = UnifiedSearchQueryParts::default();

    for chunk in qs.split('&').filter(|chunk| !chunk.is_empty()) {
        let (key, value) = parse_query_chunk(chunk)?;
        parts.apply(key, value)?;
    }

    parts.build()
}

fn default_kinds() -> BTreeSet<UnifiedSearchKind> {
    BTreeSet::from([
        UnifiedSearchKind::Namespace,
        UnifiedSearchKind::Class,
        UnifiedSearchKind::Object,
    ])
}

fn encode_cursor(token: &UnifiedSearchCursorToken) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(token).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to encode search cursor: {error}"))
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(cursor: &str) -> Result<UnifiedSearchCursorToken, ApiError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| ApiError::BadRequest(format!("Invalid search cursor: {error}")))?;
    serde_json::from_slice::<UnifiedSearchCursorToken>(&bytes)
        .map_err(|error| ApiError::BadRequest(format!("Invalid search cursor: {error}")))
}

struct SearchPage<T> {
    items: Vec<T>,
    next: Option<String>,
}

fn lower_string(value: &str) -> String {
    value.to_lowercase()
}

fn contains_case_insensitive(value: &str, query_lower: &str) -> bool {
    lower_string(value).contains(query_lower)
}

fn compute_rank(
    name: &str,
    description: &str,
    query_lower: &str,
    extra_match: bool,
) -> Option<i32> {
    let lowered_name = lower_string(name);
    if lowered_name == query_lower {
        Some(0)
    } else if lowered_name.starts_with(query_lower) {
        Some(1)
    } else if lowered_name.contains(query_lower) {
        Some(2)
    } else if contains_case_insensitive(description, query_lower) {
        Some(3)
    } else if extra_match {
        Some(4)
    } else {
        None
    }
}

fn cursor_for_item(id: i32, name: &str, rank: i32) -> UnifiedSearchCursorToken {
    UnifiedSearchCursorToken {
        rank,
        name: lower_string(name),
        id,
    }
}

fn paginate_scored<T>(
    mut scored: Vec<(UnifiedSearchCursorToken, T)>,
    cursor: Option<&UnifiedSearchCursorToken>,
    limit: usize,
) -> Result<SearchPage<T>, ApiError> {
    scored.sort_by(|left, right| left.0.cmp(&right.0));

    if let Some(cursor) = cursor {
        scored.retain(|(token, _)| token > cursor);
    }

    let has_more = scored.len() > limit;
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

fn schema_matches(schema: Option<&serde_json::Value>, query_lower: &str) -> bool {
    schema
        .map(|schema| contains_case_insensitive(&schema.to_string(), query_lower))
        .unwrap_or(false)
}

fn object_value_matches(value: &serde_json::Value, query_lower: &str) -> bool {
    match value {
        serde_json::Value::String(string) => contains_case_insensitive(string, query_lower),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|nested| object_value_matches(nested, query_lower)),
        serde_json::Value::Object(map) => map
            .values()
            .any(|nested| object_value_matches(nested, query_lower)),
        _ => false,
    }
}

async fn search_namespaces<C>(
    user: &User,
    backend: &C,
    params: &UnifiedSearchQuery,
    search_spec: &UnifiedSearchSpec,
) -> Result<SearchPage<Namespace>, ApiError>
where
    C: BackendContext + ?Sized,
{
    let rows = user.search_unified_namespaces(backend, search_spec).await?;
    if rows.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next: None,
        });
    }

    let query_lower = lower_string(&params.query);

    let scored = rows
        .into_iter()
        .filter_map(|namespace| {
            let rank = compute_rank(&namespace.name, &namespace.description, &query_lower, false)?;
            Some((
                cursor_for_item(namespace.id, &namespace.name, rank),
                namespace,
            ))
        })
        .collect();

    paginate_scored(
        scored,
        params.cursor_for(UnifiedSearchKind::Namespace),
        params.limit_per_kind,
    )
}

async fn search_classes<C>(
    user: &User,
    backend: &C,
    params: &UnifiedSearchQuery,
    search_spec: &UnifiedSearchSpec,
) -> Result<SearchPage<HubuumClassExpanded>, ApiError>
where
    C: BackendContext + ?Sized,
{
    let rows = user.search_unified_classes(backend, search_spec).await?;
    if rows.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next: None,
        });
    }

    let query_lower = lower_string(&params.query);

    let scored = rows
        .into_iter()
        .filter_map(|class| {
            let extra_match = params.search_class_schema
                && schema_matches(class.json_schema.as_ref(), &query_lower);
            let rank = compute_rank(&class.name, &class.description, &query_lower, extra_match)?;
            Some((cursor_for_item(class.id, &class.name, rank), class))
        })
        .collect::<Vec<_>>();

    let page = paginate_scored(
        scored,
        params.cursor_for(UnifiedSearchKind::Class),
        params.limit_per_kind,
    )?;
    Ok(SearchPage {
        items: page.items,
        next: page.next,
    })
}

async fn search_objects<C>(
    user: &User,
    backend: &C,
    params: &UnifiedSearchQuery,
    search_spec: &UnifiedSearchSpec,
) -> Result<SearchPage<HubuumObject>, ApiError>
where
    C: BackendContext + ?Sized,
{
    let rows = user.search_unified_objects(backend, search_spec).await?;
    if rows.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next: None,
        });
    }

    let query_lower = lower_string(&params.query);

    let scored = rows
        .into_iter()
        .filter_map(|object| {
            let extra_match =
                params.search_object_data && object_value_matches(&object.data, &query_lower);
            let rank = compute_rank(&object.name, &object.description, &query_lower, extra_match)?;
            Some((cursor_for_item(object.id, &object.name, rank), object))
        })
        .collect::<Vec<_>>();

    paginate_scored(
        scored,
        params.cursor_for(UnifiedSearchKind::Object),
        params.limit_per_kind,
    )
}

pub async fn execute_unified_search<C>(
    user: &User,
    backend: &C,
    params: &UnifiedSearchQuery,
) -> Result<UnifiedSearchResponse, ApiError>
where
    C: BackendContext + ?Sized,
{
    let search_spec = params.search_spec();
    let namespaces = if params.includes(UnifiedSearchKind::Namespace) {
        search_namespaces(user, backend, params, &search_spec).await?
    } else {
        SearchPage {
            items: vec![],
            next: None,
        }
    };

    let classes = if params.includes(UnifiedSearchKind::Class) {
        search_classes(user, backend, params, &search_spec).await?
    } else {
        SearchPage {
            items: vec![],
            next: None,
        }
    };

    let objects = if params.includes(UnifiedSearchKind::Object) {
        search_objects(user, backend, params, &search_spec).await?
    } else {
        SearchPage {
            items: vec![],
            next: None,
        }
    };

    Ok(UnifiedSearchResponse {
        query: params.query.clone(),
        results: UnifiedSearchResults {
            namespaces: namespaces.items,
            classes: classes.items,
            objects: objects.items,
        },
        next: UnifiedSearchNext {
            namespaces: namespaces.next,
            classes: classes.next,
            objects: objects.next,
        },
    })
}

pub async fn execute_unified_search_batch<C>(
    user: &User,
    backend: &C,
    params: &UnifiedSearchQuery,
    kind: UnifiedSearchKind,
) -> Result<UnifiedSearchBatchResponse, ApiError>
where
    C: BackendContext + ?Sized,
{
    let search_spec = params.search_spec();
    match kind {
        UnifiedSearchKind::Namespace => {
            let page = search_namespaces(user, backend, params, &search_spec).await?;
            Ok(UnifiedSearchBatchResponse {
                kind: kind.batch_key().to_string(),
                namespaces: page.items,
                classes: vec![],
                objects: vec![],
                next: page.next,
            })
        }
        UnifiedSearchKind::Class => {
            let page = search_classes(user, backend, params, &search_spec).await?;
            Ok(UnifiedSearchBatchResponse {
                kind: kind.batch_key().to_string(),
                namespaces: vec![],
                classes: page.items,
                objects: vec![],
                next: page.next,
            })
        }
        UnifiedSearchKind::Object => {
            let page = search_objects(user, backend, params, &search_spec).await?;
            Ok(UnifiedSearchBatchResponse {
                kind: kind.batch_key().to_string(),
                namespaces: vec![],
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
        assert_eq!(parsed.limit_per_kind, page_limits().unwrap().0);
        assert!(parsed.includes(UnifiedSearchKind::Namespace));
        assert!(parsed.includes(UnifiedSearchKind::Class));
        assert!(parsed.includes(UnifiedSearchKind::Object));
        assert!(!parsed.search_class_schema);
        assert!(!parsed.search_object_data);
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
        assert!(!parsed.includes(UnifiedSearchKind::Namespace));
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
            }
        );
    }

    #[test]
    fn parse_unified_search_rejects_unknown_parameter() {
        let error = parse_unified_search_query("q=server&foo=bar").unwrap_err();
        assert_eq!(error.to_string(), "Invalid query parameter: 'foo'");
    }
}
