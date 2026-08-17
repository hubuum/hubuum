use std::fmt;

/// Failure to construct a positive Hubuum identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PositiveIdError {
    noun: &'static str,
    value: i64,
}

impl PositiveIdError {
    const fn new(noun: &'static str, value: i64) -> Self {
        Self { noun, value }
    }

    /// Human-readable identifier kind, such as `collection id`.
    #[must_use]
    pub const fn noun(self) -> &'static str {
        self.noun
    }

    /// Rejected raw identifier.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.value
    }
}

impl fmt::Display for PositiveIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Invalid {} '{}': must be a positive integer",
            self.noun, self.value
        )
    }
}

impl std::error::Error for PositiveIdError {}

macro_rules! positive_id {
    ($(#[$meta:meta])* $name:ident, $noun:literal, $schema_name:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(i32);

        impl $name {
            /// Construct an identifier after validating that it is positive.
            pub const fn new(id: i32) -> Result<Self, PositiveIdError> {
                if id <= 0 {
                    return Err(PositiveIdError::new($noun, id as i64));
                }
                Ok(Self(id))
            }

            /// Return the validated raw identifier.
            #[must_use]
            pub const fn id(self) -> i32 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl From<$name> for i32 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let id = <i32 as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(id).map_err(serde::de::Error::custom)
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::PartialSchema for $name {
            fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                use utoipa::openapi::schema::{SchemaFormat, Type};
                use utoipa::openapi::{KnownFormat, ObjectBuilder};

                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
                    .minimum(Some(1))
                    .description(Some(concat!("Validated positive ", $noun, ".")))
                    .into()
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::ToSchema for $name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed($schema_name)
            }
        }
    };
}

macro_rules! positive_i64_id {
    ($(#[$meta:meta])* $name:ident, $noun:literal, $schema_name:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(i64);

        impl $name {
            /// Construct an identifier after validating that it is positive.
            pub const fn new(id: i64) -> Result<Self, PositiveIdError> {
                if id <= 0 {
                    return Err(PositiveIdError::new($noun, id));
                }
                Ok(Self(id))
            }

            /// Return the validated raw identifier.
            #[must_use]
            pub const fn id(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl From<$name> for i64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let id = <i64 as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(id).map_err(serde::de::Error::custom)
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::PartialSchema for $name {
            fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                use utoipa::openapi::schema::{SchemaFormat, Type};
                use utoipa::openapi::{KnownFormat, ObjectBuilder};

                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
                    .minimum(Some(1))
                    .description(Some(concat!("Validated positive ", $noun, ".")))
                    .into()
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::ToSchema for $name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed($schema_name)
            }
        }
    };
}

positive_id!(
    /// Resource-kind-neutral identifier used only by generic record metadata.
    ResourceId,
    "resource id",
    "ResourceID"
);
positive_id!(
    /// Identifier for an identity-provider scope.
    IdentityScopeId,
    "identity scope id",
    "IdentityScopeID"
);
positive_id!(
    /// Identifier for a collection.
    CollectionId,
    "collection id",
    "CollectionID"
);
positive_id!(
    /// Identifier for a class.
    ClassId,
    "class id",
    "HubuumClassID"
);
positive_id!(
    /// Identifier for an object.
    ObjectId,
    "object id",
    "HubuumObjectID"
);
positive_id!(
    /// Identifier for a class relation.
    ClassRelationId,
    "class relation id",
    "HubuumClassRelationID"
);
positive_id!(
    /// Identifier for an object relation.
    ObjectRelationId,
    "object relation id",
    "HubuumObjectRelationID"
);
positive_id!(
    /// Identifier for an authentication token.
    TokenId,
    "token id",
    "TokenID"
);
positive_id!(
    /// Identifier for an export template.
    ExportTemplateId,
    "export template id",
    "ExportTemplateID"
);
positive_id!(
    /// Identifier for a remote target.
    RemoteTargetId,
    "remote target id",
    "RemoteTargetID"
);
positive_id!(
    /// Identifier for a service account.
    ServiceAccountId,
    "service account id",
    "ServiceAccountID"
);
positive_id!(
    /// Identifier for a computed-field definition.
    ComputedFieldDefinitionId,
    "computed field definition id",
    "ComputedFieldDefinitionID"
);
positive_id!(
    /// Identifier for a task.
    TaskId,
    "task id",
    "TaskID"
);
positive_id!(
    /// Identifier for an event sink.
    EventSinkId,
    "event sink id",
    "EventSinkID"
);
positive_id!(
    /// Identifier for an event subscription.
    EventSubscriptionId,
    "event subscription id",
    "EventSubscriptionID"
);
positive_id!(
    /// Identifier for a group.
    GroupId,
    "group id",
    "GroupID"
);
positive_id!(
    /// Identifier for a principal.
    PrincipalId,
    "principal id",
    "PrincipalID"
);
positive_id!(
    /// Identifier for a human user.
    UserId,
    "user id",
    "UserID"
);
positive_i64_id!(
    /// Identifier for a staged restore job.
    RestoreJobId,
    "restore job id",
    "RestoreJobID"
);
positive_i64_id!(
    /// Identifier for an event delivery.
    EventDeliveryId,
    "event delivery id",
    "EventDeliveryID"
);

#[cfg(test)]
mod tests {
    use super::{CollectionId, PositiveIdError};

    #[test]
    fn positive_identifiers_round_trip() {
        let id = CollectionId::new(17).unwrap();
        assert_eq!(id.id(), 17);
        assert_eq!(serde_json::to_value(id).unwrap(), serde_json::json!(17));
        assert_eq!(
            serde_json::from_value::<CollectionId>(serde_json::json!(17)).unwrap(),
            id
        );
    }

    #[test]
    fn non_positive_identifiers_are_rejected() {
        assert_eq!(
            CollectionId::new(0),
            Err(PositiveIdError::new("collection id", 0))
        );
        assert!(serde_json::from_value::<CollectionId>(serde_json::json!(-1)).is_err());
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn schema_names_preserve_the_http_contract() {
        use super::{
            ClassId, ClassRelationId, ComputedFieldDefinitionId, EventDeliveryId, EventSinkId,
            EventSubscriptionId, ExportTemplateId, GroupId, IdentityScopeId, ObjectId,
            ObjectRelationId, PrincipalId, RemoteTargetId, ResourceId, RestoreJobId,
            ServiceAccountId, TaskId, TokenId, UserId,
        };
        use utoipa::ToSchema;

        let names = [
            ResourceId::name(),
            IdentityScopeId::name(),
            CollectionId::name(),
            ClassId::name(),
            ObjectId::name(),
            ClassRelationId::name(),
            ObjectRelationId::name(),
            TokenId::name(),
            ExportTemplateId::name(),
            RemoteTargetId::name(),
            ServiceAccountId::name(),
            ComputedFieldDefinitionId::name(),
            TaskId::name(),
            EventSinkId::name(),
            EventSubscriptionId::name(),
            GroupId::name(),
            PrincipalId::name(),
            UserId::name(),
            RestoreJobId::name(),
            EventDeliveryId::name(),
        ];

        assert_eq!(
            names.map(|name| name.into_owned()),
            [
                "ResourceID",
                "IdentityScopeID",
                "CollectionID",
                "HubuumClassID",
                "HubuumObjectID",
                "HubuumClassRelationID",
                "HubuumObjectRelationID",
                "TokenID",
                "ExportTemplateID",
                "RemoteTargetID",
                "ServiceAccountID",
                "ComputedFieldDefinitionID",
                "TaskID",
                "EventSinkID",
                "EventSubscriptionID",
                "GroupID",
                "PrincipalID",
                "UserID",
                "RestoreJobID",
                "EventDeliveryID",
            ]
        );
    }
}
