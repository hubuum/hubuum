use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::str::FromStr;
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::events::EventResponse;
use crate::models::search::{
    DataType, FilterField, JsonFieldPathRef, ParsedQueryParam, QueryOptions, SearchOperator,
    SortParam, StructuredQueryExpression, StructuredQueryField,
};
use crate::models::{
    ClassSelector, Collection, GroupResponse, HubuumClassExpanded, HubuumClassID, HubuumObject,
    ServiceAccountResponse, UserResponse,
};
use crate::pagination::MAX_ENCODED_CURSOR_BYTES;

pub const STRUCTURED_SEARCH_VERSION: u8 = 1;
pub const MAX_STRUCTURED_SEARCH_BYTES: usize = 64 * 1024;
pub const MAX_STRUCTURED_SEARCH_EXPRESSION_NODES: usize = 64;
pub const MAX_STRUCTURED_SEARCH_EXPRESSION_DEPTH: usize = 8;
pub const MAX_STRUCTURED_SEARCH_FIELD_PREDICATES: usize = 32;
pub const MAX_STRUCTURED_SEARCH_RELATED_PREDICATES: usize = 4;
pub const MAX_STRUCTURED_SEARCH_RELATED_FILTERS: usize = 16;
pub const MAX_STRUCTURED_SEARCH_EXTERNAL_CANDIDATES: usize = 10_000;

const STRUCTURED_SEARCH_CURSOR_VERSION: u8 = 1;
const MAX_STRUCTURED_SEARCH_VALUE_ITEMS: usize = 50;
const MAX_STRUCTURED_SEARCH_SORT_FIELDS: usize = 8;

/// Resource catalogs addressable by version 1 of the structured search DSL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredSearchResourceKind {
    Collection,
    Class,
    Object,
    AuditEvent,
    User,
    Group,
    ServiceAccount,
}

impl StructuredSearchResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Collection => "collection",
            Self::Class => "class",
            Self::Object => "object",
            Self::AuditEvent => "audit_event",
            Self::User => "user",
            Self::Group => "group",
            Self::ServiceAccount => "service_account",
        }
    }
}

/// Exact object-class selector. Exactly one of `id` or `name` is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(untagged, deny_unknown_fields)]
pub enum StructuredClassSelector {
    Id {
        id: HubuumClassID,
    },
    Name {
        #[schema(min_length = 1)]
        name: String,
    },
}

impl StructuredClassSelector {
    pub fn validate(&self) -> Result<(), ApiError> {
        match self {
            Self::Id { .. } => Ok(()),
            Self::Name { name } if !name.trim().is_empty() => Ok(()),
            Self::Name { .. } => Err(ApiError::BadRequest(
                "target.class.name must not be empty".to_string(),
            )),
        }
    }

    pub fn selector(&self) -> Result<ClassSelector, ApiError> {
        self.validate()?;
        match self {
            Self::Id { id } => Ok(ClassSelector::by_id(*id)),
            Self::Name { name } => Ok(ClassSelector::by_name(name.clone())),
        }
    }

    pub(crate) fn related_class_filter(&self, alias: &str) -> Result<ParsedQueryParam, ApiError> {
        self.validate()?;
        match self {
            Self::Id { id } => ParsedQueryParam::new(
                &format!("related.{alias}.class.id"),
                None,
                &id.id().to_string(),
            )
            .map_err(Into::into),
            Self::Name { name } => {
                ParsedQueryParam::new(&format!("related.{alias}.class.name"), None, name)
                    .map_err(Into::into)
            }
        }
    }
}

/// One resource catalog selected by a structured search request.
///
/// Object searches may optionally be constrained to one exact class. All
/// other target variants are property-free tagged objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StructuredSearchTarget {
    Collection,
    Class,
    Object {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        class: Option<StructuredClassSelector>,
    },
    AuditEvent,
    User,
    Group,
    ServiceAccount,
}

impl StructuredSearchTarget {
    pub const fn kind(&self) -> StructuredSearchResourceKind {
        match self {
            Self::Collection => StructuredSearchResourceKind::Collection,
            Self::Class => StructuredSearchResourceKind::Class,
            Self::Object { .. } => StructuredSearchResourceKind::Object,
            Self::AuditEvent => StructuredSearchResourceKind::AuditEvent,
            Self::User => StructuredSearchResourceKind::User,
            Self::Group => StructuredSearchResourceKind::Group,
            Self::ServiceAccount => StructuredSearchResourceKind::ServiceAccount,
        }
    }

    pub fn class_selector(&self) -> Option<&StructuredClassSelector> {
        match self {
            Self::Object { class } => class.as_ref(),
            _ => None,
        }
    }

    fn validate(&self) -> Result<(), ApiError> {
        if let Some(class) = self.class_selector() {
            class.validate()?;
        }
        Ok(())
    }
}

/// Operators supported by the structured DSL.
///
/// Operator applicability depends on the selected target field. JSON-path
/// fields additionally support array, key, and network operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredSearchOperator {
    Equals,
    Iequals,
    Contains,
    Icontains,
    Startswith,
    Istartswith,
    Endswith,
    Iendswith,
    Like,
    Regex,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
    WithinNetwork,
    ContainsNetwork,
    ContainsIp,
    OverlapsNetwork,
    InetEquals,
    In,
    All,
    ArrayLength,
    HasKey,
    IsNull,
}

impl StructuredSearchOperator {
    const fn as_query_operator(self) -> &'static str {
        match self {
            Self::Equals => "equals",
            Self::Iequals => "iequals",
            Self::Contains => "contains",
            Self::Icontains => "icontains",
            Self::Startswith => "startswith",
            Self::Istartswith => "istartswith",
            Self::Endswith => "endswith",
            Self::Iendswith => "iendswith",
            Self::Like => "like",
            Self::Regex => "regex",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Between => "between",
            Self::WithinNetwork => "within_network",
            Self::ContainsNetwork => "contains_network",
            Self::ContainsIp => "contains_ip",
            Self::OverlapsNetwork => "overlaps_network",
            Self::InetEquals => "inet_equals",
            Self::In => "in",
            Self::All => "all",
            Self::ArrayLength => "array_length",
            Self::HasKey => "has_key",
            Self::IsNull => "is_null",
        }
    }

    pub(crate) fn search_operator(self) -> Result<SearchOperator, ApiError> {
        SearchOperator::new_from_string(self.as_query_operator()).map_err(Into::into)
    }

    const fn requires_value(self) -> bool {
        !matches!(self, Self::IsNull)
    }
}

/// Searchable and sortable fields across all version 1 target catalogs.
///
/// Each request validates this enum against its selected target. See
/// `docs/search_api.md` for the target/field matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredSearchField {
    Id,
    Name,
    Description,
    CollectionId,
    CreatedAt,
    UpdatedAt,
    Revision,
    JsonData,
    ValidateSchema,
    JsonSchema,
    IdentityScope,
    ProperName,
    Email,
    OccurredAt,
    EntityType,
    EntityId,
    EntityName,
    Action,
    ActorKind,
    ActorUserId,
    InitiatorUserId,
    Summary,
    Metadata,
    ManagedBy,
    ExternalKey,
    LastSyncAttemptedAt,
    LastSyncSuccessAt,
    OwnerGroupId,
    CreatedBy,
    DisabledAt,
}

impl StructuredSearchField {
    const fn query_field(self) -> StructuredQueryField {
        match self {
            Self::Id => StructuredQueryField::Id,
            Self::Name => StructuredQueryField::Name,
            Self::Description => StructuredQueryField::Description,
            Self::CollectionId => StructuredQueryField::CollectionId,
            Self::CreatedAt => StructuredQueryField::CreatedAt,
            Self::UpdatedAt => StructuredQueryField::UpdatedAt,
            Self::Revision => StructuredQueryField::Revision,
            Self::JsonData => StructuredQueryField::JsonData,
            Self::ValidateSchema => StructuredQueryField::ValidateSchema,
            Self::JsonSchema => StructuredQueryField::JsonSchema,
            Self::IdentityScope => StructuredQueryField::IdentityScope,
            Self::ProperName => StructuredQueryField::ProperName,
            Self::Email => StructuredQueryField::Email,
            Self::OccurredAt => StructuredQueryField::OccurredAt,
            Self::EntityType => StructuredQueryField::EntityType,
            Self::EntityId => StructuredQueryField::EntityId,
            Self::EntityName => StructuredQueryField::EntityName,
            Self::Action => StructuredQueryField::Action,
            Self::ActorKind => StructuredQueryField::ActorKind,
            Self::ActorUserId => StructuredQueryField::ActorUserId,
            Self::InitiatorUserId => StructuredQueryField::InitiatorUserId,
            Self::Summary => StructuredQueryField::Summary,
            Self::Metadata => StructuredQueryField::Metadata,
            Self::ManagedBy => StructuredQueryField::ManagedBy,
            Self::ExternalKey => StructuredQueryField::ExternalKey,
            Self::LastSyncAttemptedAt => StructuredQueryField::LastSyncAttemptedAt,
            Self::LastSyncSuccessAt => StructuredQueryField::LastSyncSuccessAt,
            Self::OwnerGroupId => StructuredQueryField::OwnerGroupId,
            Self::CreatedBy => StructuredQueryField::CreatedBy,
            Self::DisabledAt => StructuredQueryField::DisabledAt,
        }
    }

    pub(crate) fn filter_field(
        self,
        kind: StructuredSearchResourceKind,
    ) -> Result<FilterField, ApiError> {
        self.validate_for(kind)?;
        match self {
            Self::Id => Ok(FilterField::Id),
            Self::Name => Ok(FilterField::Name),
            Self::Description => Ok(FilterField::Description),
            Self::CollectionId => Ok(FilterField::Collections),
            Self::CreatedAt => Ok(FilterField::CreatedAt),
            Self::UpdatedAt => Ok(FilterField::UpdatedAt),
            Self::Revision => Ok(FilterField::Revision),
            Self::JsonData => Ok(FilterField::JsonData),
            Self::ValidateSchema => Ok(FilterField::ValidateSchema),
            Self::JsonSchema => Ok(FilterField::JsonSchema),
            Self::IdentityScope => Ok(FilterField::IdentityScope),
            Self::ProperName => Ok(FilterField::ProperName),
            Self::Email => Ok(FilterField::Email),
            Self::OccurredAt => Ok(FilterField::OccurredAt),
            Self::EntityId | Self::ActorUserId | Self::InitiatorUserId => Ok(FilterField::Id),
            Self::EntityType
            | Self::EntityName
            | Self::Action
            | Self::ActorKind
            | Self::Summary => Ok(FilterField::Name),
            Self::Metadata => Ok(FilterField::JsonData),
            Self::ManagedBy | Self::ExternalKey => Ok(FilterField::Name),
            Self::LastSyncAttemptedAt | Self::LastSyncSuccessAt | Self::DisabledAt => {
                Ok(FilterField::CreatedAt)
            }
            Self::OwnerGroupId | Self::CreatedBy => Ok(FilterField::Id),
        }
    }

    const fn query_name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::Description => "description",
            Self::CollectionId => "collection_id",
            Self::CreatedAt => "created_at",
            Self::UpdatedAt => "updated_at",
            Self::Revision => "revision",
            Self::JsonData => "json_data",
            Self::ValidateSchema => "validate_schema",
            Self::JsonSchema => "json_schema",
            Self::IdentityScope => "identity_scope",
            Self::ProperName => "proper_name",
            Self::Email => "email",
            Self::OccurredAt => "occurred_at",
            Self::EntityType => "entity_type",
            Self::EntityId => "entity_id",
            Self::EntityName => "entity_name",
            Self::Action => "action",
            Self::ActorKind => "actor_kind",
            Self::ActorUserId => "actor_user_id",
            Self::InitiatorUserId => "initiator_user_id",
            Self::Summary => "summary",
            Self::Metadata => "metadata",
            Self::ManagedBy => "managed_by",
            Self::ExternalKey => "external_key",
            Self::LastSyncAttemptedAt => "last_sync_attempted_at",
            Self::LastSyncSuccessAt => "last_sync_success_at",
            Self::OwnerGroupId => "owner_group_id",
            Self::CreatedBy => "created_by",
            Self::DisabledAt => "disabled_at",
        }
    }

    const fn related_query_name(self) -> &'static str {
        match self {
            Self::CollectionId => "collection_id",
            _ => self.query_name(),
        }
    }

    const fn data_type(self) -> Option<DataType> {
        match self {
            Self::Id
            | Self::CollectionId
            | Self::CreatedAt
            | Self::UpdatedAt
            | Self::Revision
            | Self::OccurredAt
            | Self::EntityId
            | Self::ActorUserId
            | Self::InitiatorUserId => Some(DataType::NumericOrDate),
            Self::OwnerGroupId | Self::CreatedBy => Some(DataType::NumericOrDate),
            Self::LastSyncAttemptedAt | Self::LastSyncSuccessAt | Self::DisabledAt => {
                Some(DataType::NumericOrDate)
            }
            Self::Name
            | Self::Description
            | Self::IdentityScope
            | Self::ProperName
            | Self::Email
            | Self::EntityType
            | Self::EntityName
            | Self::Action
            | Self::ActorKind
            | Self::Summary => Some(DataType::String),
            Self::ManagedBy | Self::ExternalKey => Some(DataType::String),
            Self::ValidateSchema => Some(DataType::Boolean),
            Self::JsonData | Self::JsonSchema | Self::Metadata => None,
        }
    }

    const fn is_json(self) -> bool {
        matches!(self, Self::JsonData | Self::JsonSchema | Self::Metadata)
    }

    pub(crate) fn validate_for(self, kind: StructuredSearchResourceKind) -> Result<(), ApiError> {
        let valid = match kind {
            StructuredSearchResourceKind::Collection => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::Description
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
            ),
            StructuredSearchResourceKind::Class => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::Description
                    | Self::CollectionId
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
                    | Self::ValidateSchema
                    | Self::JsonSchema
            ),
            StructuredSearchResourceKind::Object => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::Description
                    | Self::CollectionId
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
                    | Self::JsonData
            ),
            StructuredSearchResourceKind::User => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::IdentityScope
                    | Self::ProperName
                    | Self::Email
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
            ),
            StructuredSearchResourceKind::Group => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::Description
                    | Self::IdentityScope
                    | Self::ManagedBy
                    | Self::ExternalKey
                    | Self::LastSyncAttemptedAt
                    | Self::LastSyncSuccessAt
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
            ),
            StructuredSearchResourceKind::ServiceAccount => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::Description
                    | Self::IdentityScope
                    | Self::OwnerGroupId
                    | Self::CreatedBy
                    | Self::DisabledAt
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
            ),
            StructuredSearchResourceKind::AuditEvent => matches!(
                self,
                Self::Id
                    | Self::OccurredAt
                    | Self::EntityType
                    | Self::EntityId
                    | Self::EntityName
                    | Self::CollectionId
                    | Self::Action
                    | Self::ActorKind
                    | Self::ActorUserId
                    | Self::InitiatorUserId
                    | Self::Summary
                    | Self::Metadata
            ),
        };
        if valid {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "field '{}' is not searchable for target kind '{}'",
                self.query_name(),
                kind.as_str()
            )))
        }
    }

    pub(crate) fn validate_sort_for(
        self,
        kind: StructuredSearchResourceKind,
    ) -> Result<(), ApiError> {
        self.validate_for(kind)?;
        let sortable = match kind {
            StructuredSearchResourceKind::AuditEvent => {
                matches!(self, Self::Id | Self::OccurredAt)
            }
            StructuredSearchResourceKind::Group => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::Description
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
            ),
            StructuredSearchResourceKind::ServiceAccount => matches!(
                self,
                Self::Id
                    | Self::Name
                    | Self::IdentityScope
                    | Self::CreatedAt
                    | Self::UpdatedAt
                    | Self::Revision
            ),
            _ => !self.is_json() && self != Self::ValidateSchema,
        };
        if sortable {
            Ok(())
        } else {
            Err(ApiError::BadRequest(format!(
                "field '{}' is not sortable for target kind '{}'",
                self.query_name(),
                kind.as_str()
            )))
        }
    }
}

/// One typed field predicate.
///
/// JSON fields require `path`. `is_null` omits `value`; all other operators
/// require a scalar or bounded scalar array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StructuredSearchFieldPredicate {
    pub field: StructuredSearchField,
    pub operator: StructuredSearchOperator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(min_length = 1)]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

impl StructuredSearchFieldPredicate {
    pub(crate) fn query_param(
        &self,
        kind: StructuredSearchResourceKind,
    ) -> Result<ParsedQueryParam, ApiError> {
        self.validate_for(kind)?;
        let operator = self.operator.search_operator()?;
        let value = if self.field.is_json() {
            let path = structured_json_path(self.path.as_deref().expect("validated JSON path"))?;
            if self.operator == StructuredSearchOperator::IsNull {
                path
            } else {
                format!(
                    "{path}={}",
                    structured_value_string(
                        self.value.as_ref().expect("validated predicate value")
                    )?
                )
            }
        } else if self.operator == StructuredSearchOperator::IsNull {
            "true".to_string()
        } else {
            structured_value_string(self.value.as_ref().expect("validated predicate value"))?
        };

        Ok(ParsedQueryParam {
            field: self.field.filter_field(kind)?,
            operator,
            value,
        })
    }

    pub(crate) fn related_query_param(&self, alias: &str) -> Result<ParsedQueryParam, ApiError> {
        let mut param = self.query_param(StructuredSearchResourceKind::Object)?;
        param.field = FilterField::from_str(&format!(
            "related.{alias}.object.{}",
            self.field.related_query_name()
        ))?;
        Ok(param)
    }

    fn validate_for(&self, kind: StructuredSearchResourceKind) -> Result<(), ApiError> {
        self.field.validate_for(kind)?;
        if self.field.is_json() {
            let path = self.path.as_deref().ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "{} predicates require path",
                    self.field.query_name()
                ))
            })?;
            structured_json_path(path)?;
        } else if self.path.is_some() {
            return Err(ApiError::BadRequest(format!(
                "field '{}' does not accept path",
                self.field.query_name()
            )));
        }

        match (self.operator.requires_value(), &self.value) {
            (true, None) => {
                return Err(ApiError::BadRequest(format!(
                    "operator '{}' requires value",
                    self.operator.as_query_operator()
                )));
            }
            (false, Some(_)) => {
                return Err(ApiError::BadRequest(
                    "is_null does not accept value; wrap the predicate in not for IS NOT NULL"
                        .to_string(),
                ));
            }
            _ => {}
        }

        if let Some(data_type) = self.field.data_type()
            && !self.operator.search_operator()?.is_applicable_to(data_type)
        {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{}' is not applicable to field '{}'",
                self.operator.as_query_operator(),
                self.field.query_name()
            )));
        }

        if let Some(value) = &self.value {
            let _ = structured_value_string(value)?;
        }
        Ok(())
    }
}

/// Translate the public dot-separated JSON path grammar to the shared
/// query layer's comma-separated PostgreSQL path representation.
fn structured_json_path(path: &str) -> Result<String, ApiError> {
    if path.contains(',') {
        return Err(ApiError::BadRequest(format!(
            "Invalid JSON path '{path}': use non-empty dot-separated segments containing only ASCII letters, digits, '_', or '$'"
        )));
    }
    let canonical = path.replace('.', ",");
    JsonFieldPathRef::new(&canonical).map_err(|_| {
        ApiError::BadRequest(format!(
            "Invalid JSON path '{path}': use non-empty dot-separated segments containing only ASCII letters, digits, '_', or '$'"
        ))
    })?;
    Ok(canonical)
}

/// Existential object-relation predicate.
///
/// The selected target class and all filters must match one object reachable
/// over a visible bidirectional path no deeper than `depth`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StructuredRelatedPredicate {
    pub class: StructuredClassSelector,
    #[serde(default)]
    #[schema(max_items = 16)]
    pub filters: Vec<StructuredSearchFieldPredicate>,
    #[serde(default = "default_related_depth")]
    #[schema(minimum = 1, maximum = 10)]
    pub depth: u8,
}

const fn default_related_depth() -> u8 {
    1
}

impl StructuredRelatedPredicate {
    pub(crate) fn query_params(&self, alias: &str) -> Result<Vec<ParsedQueryParam>, ApiError> {
        self.validate()?;
        let mut params = Vec::with_capacity(self.filters.len() + 2);
        params.push(self.class.related_class_filter(alias)?);
        for filter in &self.filters {
            params.push(filter.related_query_param(alias)?);
        }
        params.push(ParsedQueryParam::new(
            &format!("related.{alias}.depth"),
            Some(SearchOperator::Lte { is_negated: false }),
            &self.depth.to_string(),
        )?);
        Ok(params)
    }

    fn validate(&self) -> Result<(), ApiError> {
        self.class.validate()?;
        if !(1..=crate::models::search::MAX_RELATED_FILTER_DEPTH).contains(&self.depth) {
            return Err(ApiError::BadRequest(format!(
                "related depth must be from 1 to {}",
                crate::models::search::MAX_RELATED_FILTER_DEPTH
            )));
        }
        if self.filters.len() > MAX_STRUCTURED_SEARCH_RELATED_FILTERS {
            return Err(ApiError::BadRequest(format!(
                "related accepts at most {MAX_STRUCTURED_SEARCH_RELATED_FILTERS} filters"
            )));
        }
        for filter in &self.filters {
            filter.validate_for(StructuredSearchResourceKind::Object)?;
        }
        Ok(())
    }
}

/// Recursive boolean expression used by version 1 structured search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum StructuredSearchExpression {
    #[schema(max_properties = 2)]
    And {
        #[schema(no_recursion, min_items = 2, max_items = 64)]
        args: Vec<StructuredSearchExpression>,
    },
    #[schema(max_properties = 2)]
    Or {
        #[schema(no_recursion, min_items = 2, max_items = 64)]
        args: Vec<StructuredSearchExpression>,
    },
    #[schema(max_properties = 2)]
    Not {
        #[schema(no_recursion)]
        arg: Box<StructuredSearchExpression>,
    },
    #[schema(max_properties = 2)]
    Field {
        predicate: StructuredSearchFieldPredicate,
    },
    #[schema(max_properties = 2)]
    Related {
        predicate: StructuredRelatedPredicate,
    },
}

impl StructuredSearchExpression {
    pub(crate) fn query_expression(
        &self,
        kind: StructuredSearchResourceKind,
    ) -> Result<StructuredQueryExpression, ApiError> {
        Ok(match self {
            Self::And { args } => StructuredQueryExpression::And(
                args.iter()
                    .map(|argument| argument.query_expression(kind))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Or { args } => StructuredQueryExpression::Or(
                args.iter()
                    .map(|argument| argument.query_expression(kind))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Not { arg } => {
                StructuredQueryExpression::Not(Box::new(arg.query_expression(kind)?))
            }
            Self::Field { predicate } => StructuredQueryExpression::Field {
                field: predicate.field.query_field(),
                parameter: predicate.query_param(kind)?,
            },
            Self::Related { predicate } => {
                StructuredQueryExpression::Related(predicate.query_params("dsl")?)
            }
        })
    }

    fn validate_for(&self, kind: StructuredSearchResourceKind) -> Result<(), ApiError> {
        let mut limits = ExpressionLimits::default();
        self.validate_at_depth(1, kind, &mut limits)
    }

    fn validate_at_depth(
        &self,
        depth: usize,
        kind: StructuredSearchResourceKind,
        limits: &mut ExpressionLimits,
    ) -> Result<(), ApiError> {
        if depth > MAX_STRUCTURED_SEARCH_EXPRESSION_DEPTH {
            return Err(ApiError::BadRequest(format!(
                "filter expression depth exceeds {MAX_STRUCTURED_SEARCH_EXPRESSION_DEPTH}"
            )));
        }
        limits.nodes += 1;
        if limits.nodes > MAX_STRUCTURED_SEARCH_EXPRESSION_NODES {
            return Err(ApiError::BadRequest(format!(
                "filter expression contains more than {MAX_STRUCTURED_SEARCH_EXPRESSION_NODES} nodes"
            )));
        }

        match self {
            Self::And { args } | Self::Or { args } => {
                if args.len() < 2 {
                    return Err(ApiError::BadRequest(
                        "and/or expressions require at least two args".to_string(),
                    ));
                }
                for arg in args {
                    arg.validate_at_depth(depth + 1, kind, limits)?;
                }
            }
            Self::Not { arg } => arg.validate_at_depth(depth + 1, kind, limits)?,
            Self::Field { predicate } => {
                limits.fields += 1;
                if limits.fields > MAX_STRUCTURED_SEARCH_FIELD_PREDICATES {
                    return Err(ApiError::BadRequest(format!(
                        "filter expression contains more than {MAX_STRUCTURED_SEARCH_FIELD_PREDICATES} field predicates"
                    )));
                }
                predicate.validate_for(kind)?;
            }
            Self::Related { predicate } => {
                if kind != StructuredSearchResourceKind::Object {
                    return Err(ApiError::BadRequest(
                        "related predicates are only valid for object searches".to_string(),
                    ));
                }
                limits.related += 1;
                if limits.related > MAX_STRUCTURED_SEARCH_RELATED_PREDICATES {
                    return Err(ApiError::BadRequest(format!(
                        "filter expression contains more than {MAX_STRUCTURED_SEARCH_RELATED_PREDICATES} related predicates"
                    )));
                }
                predicate.validate()?;
                limits.fields += predicate.filters.len();
                if limits.fields > MAX_STRUCTURED_SEARCH_FIELD_PREDICATES {
                    return Err(ApiError::BadRequest(format!(
                        "filter expression contains more than {MAX_STRUCTURED_SEARCH_FIELD_PREDICATES} field predicates"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct ExpressionLimits {
    nodes: usize,
    fields: usize,
    related: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StructuredSearchSortDirection {
    Asc,
    Desc,
}

/// Stable sort key and direction for structured results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StructuredSearchSort {
    pub field: StructuredSearchField,
    #[serde(default = "default_sort_direction")]
    pub direction: StructuredSearchSortDirection,
}

const fn default_sort_direction() -> StructuredSearchSortDirection {
    StructuredSearchSortDirection::Asc
}

/// Version 1 structured resource-search request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(example = structured_search_request_example)]
#[serde(deny_unknown_fields)]
pub struct StructuredSearchRequest {
    /// DSL grammar version. Version 1 is currently required.
    #[schema(minimum = 1, maximum = 1)]
    pub version: u8,
    /// Resource catalog and optional exact object-class constraint.
    pub target: StructuredSearchTarget,
    /// Optional recursive boolean predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<StructuredSearchExpression>,
    /// Stable target-specific sort keys. The resource default applies when empty.
    #[serde(default)]
    #[schema(max_items = 8)]
    pub sort: Vec<StructuredSearchSort>,
    /// Requested page size, clamped by server pagination configuration.
    #[schema(minimum = 1)]
    pub limit: Option<usize>,
    /// Opaque cursor returned by an identical prior request and auth context.
    #[schema(min_length = 1)]
    pub cursor: Option<String>,
    /// Compute and return an exact authorized result count when true.
    #[serde(default)]
    pub include_total: bool,
}

impl StructuredSearchRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.version != STRUCTURED_SEARCH_VERSION {
            return Err(ApiError::BadRequest(format!(
                "Unsupported structured search version {}; expected {STRUCTURED_SEARCH_VERSION}",
                self.version
            )));
        }
        self.target.validate()?;
        let kind = self.target.kind();
        if let Some(filter) = &self.filter {
            filter.validate_for(kind)?;
        }
        if self.sort.len() > MAX_STRUCTURED_SEARCH_SORT_FIELDS {
            return Err(ApiError::BadRequest(format!(
                "search accepts at most {MAX_STRUCTURED_SEARCH_SORT_FIELDS} sort fields"
            )));
        }
        let mut seen_sort_fields = std::collections::HashSet::new();
        for sort in &self.sort {
            sort.field.validate_sort_for(kind)?;
            if !seen_sort_fields.insert(sort.field) {
                return Err(ApiError::BadRequest(format!(
                    "duplicate sort field '{:?}'",
                    sort.field
                )));
            }
        }
        if let Some(limit) = self.limit {
            let _ = crate::pagination::validate_page_limit(limit)?;
        }
        if self.cursor.as_ref().is_some_and(String::is_empty) {
            return Err(ApiError::BadRequest("cursor must not be empty".to_string()));
        }
        Ok(())
    }

    pub fn class_selector(&self) -> Result<Option<ClassSelector>, ApiError> {
        self.target
            .class_selector()
            .map(StructuredClassSelector::selector)
            .transpose()
    }

    pub(crate) fn query_options(
        &self,
        class_id: Option<HubuumClassID>,
        page_cursor: Option<String>,
    ) -> Result<QueryOptions, ApiError> {
        let kind = self.target.kind();
        let filters = class_id
            .map(|class_id| ParsedQueryParam {
                field: FilterField::ClassId,
                operator: SearchOperator::Equals { is_negated: false },
                value: class_id.id().to_string(),
            })
            .into_iter()
            .collect();
        let sort = self
            .sort
            .iter()
            .map(|sort| {
                Ok::<SortParam, ApiError>(SortParam {
                    field: sort.field.filter_field(kind)?,
                    descending: sort.direction == StructuredSearchSortDirection::Desc,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut options =
            QueryOptions::new(filters, sort, self.limit, page_cursor, self.include_total)?;
        options.set_structured_filter(
            self.filter
                .as_ref()
                .map(|filter| filter.query_expression(kind))
                .transpose()?,
        );
        Ok(options)
    }

    pub(crate) fn fingerprint(
        &self,
        resolved_class_id: Option<HubuumClassID>,
        principal_id: i32,
        token_id: i32,
        token_revision: i64,
    ) -> Result<String, ApiError> {
        let mut request = self.clone();
        request.cursor = None;
        let canonical = serde_json::to_vec(&serde_json::json!({
            "request": request,
            "resolved_class_id": resolved_class_id.map(|class_id| class_id.id()),
            "principal_id": principal_id,
            "token_id": token_id,
            "token_revision": token_revision,
        }))
        .map_err(|error| {
            ApiError::InternalServerError(format!(
                "Failed to canonicalize structured search: {error}"
            ))
        })?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(canonical)))
    }

    /// Maximum cursor length that keeps a compact serialization of this same
    /// request within the structured-search request-body limit.
    pub(crate) fn reusable_cursor_budget(&self) -> Result<usize, ApiError> {
        let mut request = self.clone();
        request.cursor = Some(String::new());
        let fixed_request_bytes = serde_json::to_vec(&request)
            .map_err(|error| {
                ApiError::InternalServerError(format!(
                    "Failed to size structured search cursor request: {error}"
                ))
            })?
            .len();
        Ok(MAX_STRUCTURED_SEARCH_BYTES
            .saturating_sub(fixed_request_bytes)
            .min(MAX_ENCODED_CURSOR_BYTES))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StructuredSearchCursor {
    version: u8,
    fingerprint: String,
    page_cursor: String,
}

pub(crate) fn encode_structured_search_cursor(
    fingerprint: &str,
    page_cursor: String,
    max_encoded_bytes: usize,
) -> Result<String, ApiError> {
    let payload = serde_json::to_vec(&StructuredSearchCursor {
        version: STRUCTURED_SEARCH_CURSOR_VERSION,
        fingerprint: fingerprint.to_string(),
        page_cursor,
    })
    .map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to encode structured search cursor: {error}"
        ))
    })?;
    let limit = max_encoded_bytes.min(MAX_ENCODED_CURSOR_BYTES);
    let encoded_length = payload.len().saturating_mul(4).saturating_add(2) / 3;
    if encoded_length > limit {
        return Err(structured_cursor_too_large());
    }
    let cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    if cursor.len() > limit {
        return Err(structured_cursor_too_large());
    }
    Ok(cursor)
}

fn structured_cursor_too_large() -> ApiError {
    ApiError::BadRequest(
        "Structured search cursor exceeds the reusable request size limit; use smaller sort values"
            .to_string(),
    )
}

pub(crate) fn decode_structured_search_cursor(
    cursor: Option<&str>,
    expected_fingerprint: &str,
) -> Result<Option<String>, ApiError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
        return Err(ApiError::BadRequest(
            "Structured search cursor is too large".to_string(),
        ));
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ApiError::BadRequest("Invalid structured search cursor".to_string()))?;
    let cursor: StructuredSearchCursor = serde_json::from_slice(&payload)
        .map_err(|_| ApiError::BadRequest("Invalid structured search cursor".to_string()))?;
    if cursor.version != STRUCTURED_SEARCH_CURSOR_VERSION {
        return Err(ApiError::BadRequest(
            "Unsupported structured search cursor version".to_string(),
        ));
    }
    if cursor.fingerprint != expected_fingerprint {
        return Err(ApiError::BadRequest(
            "Structured search cursor does not match this query or authorization context"
                .to_string(),
        ));
    }
    if cursor.page_cursor.is_empty() {
        return Err(ApiError::BadRequest(
            "Invalid structured search cursor".to_string(),
        ));
    }
    Ok(Some(cursor.page_cursor))
}

/// Cursor-paginated result envelope for structured resource search.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StructuredSearchResponse {
    /// DSL version used to produce this response.
    pub version: u8,
    /// Resource kind shared by every result item.
    pub kind: StructuredSearchResourceKind,
    /// Tagged resource values using the normal public list representation.
    pub results: Vec<StructuredSearchResult>,
    /// Opaque next-page cursor, or null at the final page.
    pub next: Option<String>,
    /// Exact authorized count when requested; otherwise null.
    pub total: Option<i64>,
}

/// First event emitted by the structured-search SSE endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StructuredSearchStartedEvent {
    /// DSL version used to execute the request.
    pub version: u8,
    /// Resource kind selected by the request.
    pub kind: StructuredSearchResourceKind,
}

/// Terminal success event emitted by the structured-search SSE endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StructuredSearchDoneEvent {
    /// DSL version used to execute the request.
    pub version: u8,
    /// Resource kind shared by all preceding result events.
    pub kind: StructuredSearchResourceKind,
    /// Opaque next-page cursor, or null at the final page.
    pub next: Option<String>,
    /// Exact authorized count when requested; otherwise null.
    pub total: Option<i64>,
    /// Effective page size after applying server pagination limits.
    pub page_limit: usize,
}

/// Terminal failure event emitted after an SSE response has started.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StructuredSearchErrorEvent {
    /// DSL version supplied by the request.
    pub version: u8,
    /// Resource kind selected by the request.
    pub kind: StructuredSearchResourceKind,
    /// Public API error message.
    pub message: String,
}

/// Tagged union of public resource representations returned by the DSL.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "resource", rename_all = "snake_case")]
pub enum StructuredSearchResult {
    Collection(Collection),
    Class(HubuumClassExpanded),
    Object(HubuumObject),
    AuditEvent(Box<EventResponse>),
    User(UserResponse),
    Group(GroupResponse),
    ServiceAccount(ServiceAccountResponse),
}

fn structured_search_request_example() -> Value {
    serde_json::json!({
        "version": 1,
        "target": {
            "kind": "object",
            "class": {"name": "Server"}
        },
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "field",
                    "predicate": {
                        "field": "description",
                        "operator": "icontains",
                        "value": "production"
                    }
                },
                {
                    "op": "not",
                    "arg": {
                        "op": "related",
                        "predicate": {
                            "class": {"name": "Room"},
                            "filters": [{
                                "field": "json_data",
                                "path": "status",
                                "operator": "equals",
                                "value": "retired"
                            }],
                            "depth": 2
                        }
                    }
                }
            ]
        },
        "sort": [{"field": "name", "direction": "asc"}],
        "limit": 100,
        "include_total": false
    })
}

fn structured_value_string(value: &Value) -> Result<String, ApiError> {
    match value {
        Value::String(value) if value.is_empty() => Err(ApiError::BadRequest(
            "predicate value must not be empty".to_string(),
        )),
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => Err(ApiError::BadRequest(
            "predicate value must not be null; use is_null without value".to_string(),
        )),
        Value::Array(values) => {
            if values.is_empty() || values.len() > MAX_STRUCTURED_SEARCH_VALUE_ITEMS {
                return Err(ApiError::BadRequest(format!(
                    "predicate arrays require from 1 to {MAX_STRUCTURED_SEARCH_VALUE_ITEMS} values"
                )));
            }
            values
                .iter()
                .map(|value| {
                    if matches!(value, Value::Array(_) | Value::Object(_)) {
                        return Err(ApiError::BadRequest(
                            "predicate arrays may contain only scalar values".to_string(),
                        ));
                    }
                    let value = structured_value_string(value)?;
                    if value.contains(',') {
                        return Err(ApiError::BadRequest(
                            "predicate array string values must not contain commas".to_string(),
                        ));
                    }
                    Ok(value)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join(","))
        }
        Value::Object(_) => Err(ApiError::BadRequest(
            "predicate value must be a scalar or scalar array".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> StructuredSearchTarget {
        StructuredSearchTarget::Object {
            class: Some(StructuredClassSelector::Id {
                id: HubuumClassID::new(7).unwrap(),
            }),
        }
    }

    #[test]
    fn cursor_is_bound_to_the_structured_query() {
        let request = StructuredSearchRequest {
            version: 1,
            target: target(),
            filter: None,
            sort: vec![],
            limit: Some(25),
            cursor: None,
            include_total: false,
        };
        let first_fingerprint = request
            .fingerprint(Some(HubuumClassID::new(7).unwrap()), 3, 5, 1)
            .unwrap();
        let cursor = encode_structured_search_cursor(
            &first_fingerprint,
            "page".to_string(),
            request.reusable_cursor_budget().unwrap(),
        )
        .unwrap();
        let mut changed = request.clone();
        changed.limit = Some(50);
        let changed_fingerprint = changed
            .fingerprint(Some(HubuumClassID::new(7).unwrap()), 3, 5, 1)
            .unwrap();

        let error =
            decode_structured_search_cursor(Some(&cursor), &changed_fingerprint).unwrap_err();

        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn expression_depth_is_bounded() {
        let mut expression = StructuredSearchExpression::Field {
            predicate: StructuredSearchFieldPredicate {
                field: StructuredSearchField::Name,
                operator: StructuredSearchOperator::Equals,
                path: None,
                value: Some(Value::String("server".to_string())),
            },
        };
        for _ in 0..MAX_STRUCTURED_SEARCH_EXPRESSION_DEPTH {
            expression = StructuredSearchExpression::Not {
                arg: Box::new(expression),
            };
        }

        let error = expression
            .validate_for(StructuredSearchResourceKind::Object)
            .unwrap_err();

        assert!(error.to_string().contains("depth exceeds"));
    }

    #[test]
    fn json_predicate_requires_a_valid_path() {
        let predicate = StructuredSearchFieldPredicate {
            field: StructuredSearchField::JsonData,
            operator: StructuredSearchOperator::Equals,
            path: Some("bad..path".to_string()),
            value: Some(Value::String("value".to_string())),
        };

        let error = predicate
            .query_param(StructuredSearchResourceKind::Object)
            .unwrap_err();

        assert!(error.to_string().contains("Invalid JSON path"));
    }

    #[test]
    fn dotted_json_paths_are_translated_for_direct_and_related_filters() {
        let predicate = StructuredSearchFieldPredicate {
            field: StructuredSearchField::JsonData,
            operator: StructuredSearchOperator::Equals,
            path: Some("hardware.cpu.count".to_string()),
            value: Some(Value::Number(8.into())),
        };

        let direct = predicate
            .query_param(StructuredSearchResourceKind::Object)
            .unwrap();
        let related = predicate.related_query_param("target").unwrap();

        assert_eq!(direct.value, "hardware,cpu,count=8");
        assert_eq!(related.value, "hardware,cpu,count=8");
    }

    #[test]
    fn expression_nodes_reject_unknown_properties() {
        let error = serde_json::from_value::<StructuredSearchExpression>(serde_json::json!({
            "op": "field",
            "predicate": {
                "field": "name",
                "operator": "equals",
                "value": "server"
            },
            "not": true
        }))
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn wrapped_cursor_must_fit_its_reusable_request_budget() {
        let error = encode_structured_search_cursor(
            "fingerprint",
            "x".repeat(MAX_ENCODED_CURSOR_BYTES * 3 / 4),
            MAX_ENCODED_CURSOR_BYTES,
        )
        .unwrap_err();

        assert!(error.to_string().contains("reusable request size limit"));
    }

    #[test]
    fn cursor_budget_reserves_the_rest_of_the_request_envelope() {
        let request = StructuredSearchRequest {
            version: 1,
            target: target(),
            filter: None,
            sort: vec![],
            limit: Some(25),
            cursor: None,
            include_total: false,
        };
        let budget = request.reusable_cursor_budget().unwrap();
        let mut next_request = request;
        next_request.cursor = Some("x".repeat(budget));

        assert!(serde_json::to_vec(&next_request).unwrap().len() <= MAX_STRUCTURED_SEARCH_BYTES);
    }

    #[test]
    fn predicate_null_requires_the_is_null_operator() {
        let predicate = StructuredSearchFieldPredicate {
            field: StructuredSearchField::Name,
            operator: StructuredSearchOperator::Equals,
            path: None,
            value: Some(Value::Null),
        };

        let error = predicate
            .query_param(StructuredSearchResourceKind::Collection)
            .unwrap_err();

        assert!(error.to_string().contains("use is_null"));
    }

    #[test]
    fn predicate_arrays_reject_nested_values() {
        let predicate = StructuredSearchFieldPredicate {
            field: StructuredSearchField::Name,
            operator: StructuredSearchOperator::In,
            path: None,
            value: Some(serde_json::json!([["nested"]])),
        };

        let error = predicate
            .query_param(StructuredSearchResourceKind::Object)
            .unwrap_err();

        assert!(error.to_string().contains("only scalar values"));
    }
}
