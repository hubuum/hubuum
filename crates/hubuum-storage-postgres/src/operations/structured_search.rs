//! PostgreSQL compilation for backend-neutral recursive search predicates.

use std::future::Future;
use std::pin::Pin;

use hubuum_query::{
    FilterField, Operator, ParsedQueryParam, StructuredQueryExpression, StructuredQueryField,
};
use hubuum_storage_core::StorageVisibility;

use crate::operations::dynamic_sql::{
    BoundSqlPredicate, SqlComponent, SqlValue, bound_sql_predicate,
};
use crate::operations::json_filter::json_filter_sql;
use crate::operations::related_filter::{
    related_date_clause, related_integer_clause, related_object_filter_component,
    related_revision_clause, related_string_clause, wrap_negated,
};
use crate::{PostgresConnection, PostgresStorageError};

#[derive(Clone, Copy)]
pub(crate) enum StructuredResourceKind {
    Collection,
    Class,
    Object,
    AuditEvent,
    User,
    Group,
    ServiceAccount,
}

#[derive(Clone, Copy)]
enum StructuredSqlField {
    Integer(&'static str),
    BigInteger(&'static str),
    Date(&'static str),
    String(&'static str),
    Boolean(&'static str),
    Json(&'static str),
}

pub(crate) async fn structured_filter_predicate(
    connection: &mut PostgresConnection,
    expression: &StructuredQueryExpression,
    kind: StructuredResourceKind,
    visibility: Option<&StorageVisibility>,
) -> Result<BoundSqlPredicate, PostgresStorageError> {
    bound_sql_predicate(
        structured_expression_component(connection, expression, kind, visibility).await?,
    )
}

fn structured_expression_component<'a>(
    connection: &'a mut PostgresConnection,
    expression: &'a StructuredQueryExpression,
    kind: StructuredResourceKind,
    visibility: Option<&'a StorageVisibility>,
) -> Pin<Box<dyn Future<Output = Result<SqlComponent, PostgresStorageError>> + Send + 'a>> {
    Box::pin(async move {
        match expression {
            StructuredQueryExpression::And(arguments) => {
                structured_boolean_component(connection, arguments, "AND", kind, visibility).await
            }
            StructuredQueryExpression::Or(arguments) => {
                structured_boolean_component(connection, arguments, "OR", kind, visibility).await
            }
            StructuredQueryExpression::Not(argument) => {
                let component =
                    structured_expression_component(connection, argument, kind, visibility).await?;
                Ok(SqlComponent {
                    sql: format!("NOT ({})", component.sql),
                    bind_variables: component.bind_variables,
                })
            }
            StructuredQueryExpression::Field { field, parameter } => {
                let mut bind_variables = Vec::new();
                let sql = structured_field_clause(*field, kind, parameter, &mut bind_variables)?;
                Ok(SqlComponent {
                    sql: format!("COALESCE(({sql}), FALSE)"),
                    bind_variables,
                })
            }
            StructuredQueryExpression::Related(filters) => {
                if !matches!(kind, StructuredResourceKind::Object) {
                    return Err(PostgresStorageError::invalid_input(
                        "Related predicates are only valid for object searches",
                    ));
                }
                let visibility = visibility.ok_or_else(|| {
                    PostgresStorageError::internal(
                        "Structured related search is missing visibility inputs",
                    )
                })?;
                related_object_filter_component(connection, filters, visibility)
                    .await?
                    .ok_or_else(|| {
                        PostgresStorageError::invalid_input(
                            "Structured related predicate did not contain a relation filter",
                        )
                    })
            }
        }
    })
}

async fn structured_boolean_component(
    connection: &mut PostgresConnection,
    arguments: &[StructuredQueryExpression],
    operator: &str,
    kind: StructuredResourceKind,
    visibility: Option<&StorageVisibility>,
) -> Result<SqlComponent, PostgresStorageError> {
    let mut sql = Vec::with_capacity(arguments.len());
    let mut bind_variables = Vec::new();
    for argument in arguments {
        let component =
            structured_expression_component(connection, argument, kind, visibility).await?;
        sql.push(format!("({})", component.sql));
        bind_variables.extend(component.bind_variables);
    }
    Ok(SqlComponent {
        sql: sql.join(&format!(" {operator} ")),
        bind_variables,
    })
}

fn structured_field_clause(
    field: StructuredQueryField,
    kind: StructuredResourceKind,
    parameter: &ParsedQueryParam,
    bind_variables: &mut Vec<SqlValue>,
) -> Result<String, PostgresStorageError> {
    use StructuredQueryField as Field;
    use StructuredResourceKind as Kind;

    let sql_field = match (kind, field) {
        (Kind::Collection, Field::Id) => StructuredSqlField::Integer("collections.id"),
        (Kind::Collection, Field::Name) => StructuredSqlField::String("collections.name"),
        (Kind::Collection, Field::Description) => {
            StructuredSqlField::String("collections.description")
        }
        (Kind::Collection, Field::CreatedAt) => StructuredSqlField::Date("collections.created_at"),
        (Kind::Collection, Field::UpdatedAt) => StructuredSqlField::Date("collections.updated_at"),
        (Kind::Collection, Field::Revision) => {
            StructuredSqlField::BigInteger("collections.revision")
        }
        (Kind::Class, Field::Id) => StructuredSqlField::Integer("hubuumclass.id"),
        (Kind::Class, Field::Name) => StructuredSqlField::String("hubuumclass.name"),
        (Kind::Class, Field::Description) => StructuredSqlField::String("hubuumclass.description"),
        (Kind::Class, Field::CollectionId) => {
            StructuredSqlField::Integer("hubuumclass.collection_id")
        }
        (Kind::Class, Field::CreatedAt) => StructuredSqlField::Date("hubuumclass.created_at"),
        (Kind::Class, Field::UpdatedAt) => StructuredSqlField::Date("hubuumclass.updated_at"),
        (Kind::Class, Field::Revision) => StructuredSqlField::BigInteger("hubuumclass.revision"),
        (Kind::Class, Field::ValidateSchema) => {
            StructuredSqlField::Boolean("hubuumclass.validate_schema")
        }
        (Kind::Class, Field::JsonSchema) => StructuredSqlField::Json("hubuumclass.json_schema"),
        (Kind::Object, Field::Id) => StructuredSqlField::Integer("hubuumobject.id"),
        (Kind::Object, Field::Name) => StructuredSqlField::String("hubuumobject.name"),
        (Kind::Object, Field::Description) => {
            StructuredSqlField::String("hubuumobject.description")
        }
        (Kind::Object, Field::CollectionId) => {
            StructuredSqlField::Integer("hubuumobject.collection_id")
        }
        (Kind::Object, Field::CreatedAt) => StructuredSqlField::Date("hubuumobject.created_at"),
        (Kind::Object, Field::UpdatedAt) => StructuredSqlField::Date("hubuumobject.updated_at"),
        (Kind::Object, Field::Revision) => StructuredSqlField::BigInteger("hubuumobject.revision"),
        (Kind::Object, Field::JsonData) => StructuredSqlField::Json("hubuumobject.data"),
        (Kind::User, Field::Id) => StructuredSqlField::Integer("users.id"),
        (Kind::User, Field::Name) => StructuredSqlField::String("principals.name"),
        (Kind::User, Field::IdentityScope) => StructuredSqlField::String("identity_scopes.name"),
        (Kind::User, Field::ProperName) => StructuredSqlField::String("users.proper_name"),
        (Kind::User, Field::Email) => StructuredSqlField::String("users.email"),
        (Kind::User, Field::CreatedAt) => StructuredSqlField::Date("users.created_at"),
        (Kind::User, Field::UpdatedAt) => StructuredSqlField::Date("users.updated_at"),
        (Kind::User, Field::Revision) => StructuredSqlField::BigInteger("principals.revision"),
        (Kind::Group, Field::Id) => StructuredSqlField::Integer("groups.id"),
        (Kind::Group, Field::Name) => StructuredSqlField::String("groups.groupname"),
        (Kind::Group, Field::Description) => StructuredSqlField::String("groups.description"),
        (Kind::Group, Field::IdentityScope) => StructuredSqlField::String("identity_scopes.name"),
        (Kind::Group, Field::ManagedBy) => StructuredSqlField::String("groups.managed_by"),
        (Kind::Group, Field::ExternalKey) => StructuredSqlField::String("groups.external_key"),
        (Kind::Group, Field::LastSyncAttemptedAt) => {
            StructuredSqlField::Date("groups.last_sync_attempted_at")
        }
        (Kind::Group, Field::LastSyncSuccessAt) => {
            StructuredSqlField::Date("groups.last_sync_success_at")
        }
        (Kind::Group, Field::CreatedAt) => StructuredSqlField::Date("groups.created_at"),
        (Kind::Group, Field::UpdatedAt) => StructuredSqlField::Date("groups.updated_at"),
        (Kind::Group, Field::Revision) => StructuredSqlField::BigInteger("groups.revision"),
        (Kind::ServiceAccount, Field::Id) => StructuredSqlField::Integer("service_accounts.id"),
        (Kind::ServiceAccount, Field::Name) => StructuredSqlField::String("principals.name"),
        (Kind::ServiceAccount, Field::Description) => {
            StructuredSqlField::String("service_accounts.description")
        }
        (Kind::ServiceAccount, Field::IdentityScope) => {
            StructuredSqlField::String("identity_scopes.name")
        }
        (Kind::ServiceAccount, Field::OwnerGroupId) => {
            StructuredSqlField::Integer("service_accounts.owner_group_id")
        }
        (Kind::ServiceAccount, Field::CreatedBy) => {
            StructuredSqlField::Integer("service_accounts.created_by")
        }
        (Kind::ServiceAccount, Field::DisabledAt) => {
            StructuredSqlField::Date("service_accounts.disabled_at")
        }
        (Kind::ServiceAccount, Field::CreatedAt) => {
            StructuredSqlField::Date("service_accounts.created_at")
        }
        (Kind::ServiceAccount, Field::UpdatedAt) => {
            StructuredSqlField::Date("service_accounts.updated_at")
        }
        (Kind::ServiceAccount, Field::Revision) => {
            StructuredSqlField::BigInteger("principals.revision")
        }
        (Kind::AuditEvent, Field::Id) => StructuredSqlField::BigInteger("events.id"),
        (Kind::AuditEvent, Field::OccurredAt) => StructuredSqlField::Date("events.occurred_at"),
        (Kind::AuditEvent, Field::EntityType) => StructuredSqlField::String("events.entity_type"),
        (Kind::AuditEvent, Field::EntityId) => StructuredSqlField::Integer("events.entity_id"),
        (Kind::AuditEvent, Field::EntityName) => StructuredSqlField::String("events.entity_name"),
        (Kind::AuditEvent, Field::CollectionId) => {
            StructuredSqlField::Integer("events.collection_id")
        }
        (Kind::AuditEvent, Field::Action) => StructuredSqlField::String("events.action"),
        (Kind::AuditEvent, Field::ActorKind) => StructuredSqlField::String("events.actor_kind"),
        (Kind::AuditEvent, Field::ActorUserId) => {
            StructuredSqlField::Integer("events.actor_user_id")
        }
        (Kind::AuditEvent, Field::InitiatorUserId) => {
            StructuredSqlField::Integer("events.initiator_user_id")
        }
        (Kind::AuditEvent, Field::Summary) => StructuredSqlField::String("events.summary"),
        (Kind::AuditEvent, Field::Metadata) => StructuredSqlField::Json("events.metadata"),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Structured field '{field:?}' is not searchable for this resource kind"
            )));
        }
    };

    let column = match sql_field {
        StructuredSqlField::Integer(column)
        | StructuredSqlField::BigInteger(column)
        | StructuredSqlField::Date(column)
        | StructuredSqlField::String(column)
        | StructuredSqlField::Boolean(column)
        | StructuredSqlField::Json(column) => column,
    };
    let (operator, negated) = parameter.operator.op_and_neg();
    if operator == Operator::IsNull && !matches!(sql_field, StructuredSqlField::Json(_)) {
        let should_be_null = hubuum_query::parse_boolean_value(&parameter.value)
            .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?
            != negated;
        return Ok(format!(
            "{column} IS {}NULL",
            if should_be_null { "" } else { "NOT " }
        ));
    }

    match sql_field {
        StructuredSqlField::Integer(_) => related_integer_clause(parameter, column, bind_variables),
        StructuredSqlField::BigInteger(_) => {
            related_revision_clause(parameter, column, bind_variables)
        }
        StructuredSqlField::Date(_) => related_date_clause(parameter, column, bind_variables),
        StructuredSqlField::String(_) => related_string_clause(parameter, column, bind_variables),
        StructuredSqlField::Boolean(_) => {
            if operator != Operator::Equals {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Operator '{}' is not implemented for boolean field '{}'",
                    parameter.operator, parameter.field
                )));
            }
            let value = hubuum_query::parse_boolean_value(&parameter.value)
                .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
            bind_variables.push(SqlValue::Boolean(value));
            Ok(wrap_negated(format!("{column} = ?"), negated))
        }
        StructuredSqlField::Json(_) => {
            let mut json_parameter = parameter.clone();
            json_parameter.field = FilterField::JsonData;
            let component = json_filter_sql(&json_parameter, column)?;
            bind_variables.extend(component.bind_variables);
            Ok(format!("({})", component.sql))
        }
    }
}
