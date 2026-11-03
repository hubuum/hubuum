use super::*;

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryAgeRule {
    age_seconds: HistoryRetentionSeconds,
    #[serde(
        default,
        deserialize_with = "super::deserialize_present_age",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    deleted_age_seconds: Option<HistoryRetentionSeconds>,
}
impl HistoryAgeRule {
    pub const fn new(
        age_seconds: HistoryRetentionSeconds,
        deleted_age_seconds: Option<HistoryRetentionSeconds>,
    ) -> Self {
        Self {
            age_seconds,
            deleted_age_seconds,
        }
    }
    pub fn deadline(
        &self,
        anchor: HistoryRetentionAnchor,
    ) -> Result<DateTime<Utc>, StorageValidationError> {
        let age = match anchor {
            HistoryRetentionAnchor::Deleted(_) => {
                self.deleted_age_seconds.unwrap_or(self.age_seconds)
            }
            _ => self.age_seconds,
        };
        deadline(anchor.at(), age)
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "SizeInput")]
pub struct HistorySizeRule {
    upper_logical_bytes: HistoryLogicalBytes,
    lower_logical_bytes: HistoryLogicalBytes,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SizeInput {
    upper_logical_bytes: HistoryLogicalBytes,
    lower_logical_bytes: HistoryLogicalBytes,
}
impl HistorySizeRule {
    pub fn new(
        upper: HistoryLogicalBytes,
        lower: HistoryLogicalBytes,
    ) -> Result<Self, StorageValidationError> {
        if lower >= upper {
            return Err(invalid(
                "Logical size lower target must be strictly below its upper threshold",
            ));
        }
        Ok(Self {
            upper_logical_bytes: upper,
            lower_logical_bytes: lower,
        })
    }
    pub const fn upper(&self) -> HistoryLogicalBytes {
        self.upper_logical_bytes
    }
    pub const fn lower(&self) -> HistoryLogicalBytes {
        self.lower_logical_bytes
    }
}
impl TryFrom<SizeInput> for HistorySizeRule {
    type Error = StorageValidationError;
    fn try_from(value: SizeInput) -> Result<Self, Self::Error> {
        Self::new(value.upper_logical_bytes, value.lower_logical_bytes)
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryExpiryTrigger {
    Age {
        age_seconds: HistoryRetentionSeconds,
        #[serde(
            default,
            deserialize_with = "super::deserialize_present_age",
            skip_serializing_if = "Option::is_none"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        deleted_age_seconds: Option<HistoryRetentionSeconds>,
    },
    LogicalSize {
        upper_logical_bytes: HistoryLogicalBytes,
        lower_logical_bytes: HistoryLogicalBytes,
    },
    AgeOrSize {
        age: HistoryAgeRule,
        size: HistorySizeRule,
    },
    AgeAndSize {
        age: HistoryAgeRule,
        size: HistorySizeRule,
    },
}
impl HistoryExpiryTrigger {
    pub fn validate(&self) -> Result<(), StorageValidationError> {
        if let Self::LogicalSize {
            upper_logical_bytes,
            lower_logical_bytes,
        } = self
        {
            HistorySizeRule::new(*upper_logical_bytes, *lower_logical_bytes)?;
        }
        Ok(())
    }
    pub fn age(&self) -> Option<HistoryAgeRule> {
        match self {
            Self::Age {
                age_seconds,
                deleted_age_seconds,
            } => Some(HistoryAgeRule::new(*age_seconds, *deleted_age_seconds)),
            Self::LogicalSize { .. } => None,
            Self::AgeOrSize { age, .. } | Self::AgeAndSize { age, .. } => Some(age.clone()),
        }
    }
    pub fn size(&self) -> Option<HistorySizeRule> {
        match self {
            Self::Age { .. } => None,
            Self::LogicalSize {
                upper_logical_bytes,
                lower_logical_bytes,
            } => HistorySizeRule::new(*upper_logical_bytes, *lower_logical_bytes).ok(),
            Self::AgeOrSize { size, .. } | Self::AgeAndSize { size, .. } => Some(size.clone()),
        }
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryPolicyBinding {
    Inherit,
    DisabledExpiry,
    Indefinite,
    Expire {
        trigger: HistoryExpiryTrigger,
        minimum_age_seconds: HistoryRetentionSeconds,
    },
}
impl HistoryPolicyBinding {
    pub fn validate(&self) -> Result<(), StorageValidationError> {
        if let Self::Expire { trigger, .. } = self {
            trigger.validate()?;
        }
        Ok(())
    }
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryPolicyApplication {
    Prospective,
    Retrospective {
        #[serde(deserialize_with = "super::deserialize_utc")]
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
        closed_through: DateTime<Utc>,
    },
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPolicyAction {
    ReplacePolicy,
}

/// Strict wire proposal. Deserialization performs semantic validation as well as
/// rejecting unknown nested write fields. Stateful cutoffs are checked at issue/apply.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "ChangeInput")]
pub struct HistoryPolicyChange {
    action: HistoryPolicyAction,
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScope,
    expected_policy_revision: HistoryPolicyRevision,
    binding: HistoryPolicyBinding,
    application: HistoryPolicyApplication,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeInput {
    action: HistoryPolicyAction,
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScope,
    expected_policy_revision: HistoryPolicyRevision,
    binding: HistoryPolicyBinding,
    application: HistoryPolicyApplication,
}
impl HistoryPolicyChange {
    pub fn new(
        key: HistoryPolicyKey,
        expected: HistoryPolicyRevision,
        binding: HistoryPolicyBinding,
        application: HistoryPolicyApplication,
    ) -> Result<Self, StorageValidationError> {
        binding.validate()?;
        Ok(Self {
            action: HistoryPolicyAction::ReplacePolicy,
            family: key.family(),
            scope: key.scope(),
            expected_policy_revision: expected,
            binding,
            application,
        })
    }
    pub fn key(&self) -> HistoryPolicyKey {
        HistoryPolicyKey {
            family: self.family,
            scope: self.scope,
        }
    }
    pub const fn expected_policy_revision(&self) -> HistoryPolicyRevision {
        self.expected_policy_revision
    }
    pub const fn binding(&self) -> &HistoryPolicyBinding {
        &self.binding
    }
    pub const fn application(&self) -> &HistoryPolicyApplication {
        &self.application
    }
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), StorageValidationError> {
        if let HistoryPolicyApplication::Retrospective { closed_through } = &self.application
            && *closed_through > now
        {
            return Err(invalid(
                "Retrospective cutoff cannot exceed authoritative review time",
            ));
        }
        Ok(())
    }
}
impl TryFrom<ChangeInput> for HistoryPolicyChange {
    type Error = StorageValidationError;
    fn try_from(value: ChangeInput) -> Result<Self, Self::Error> {
        let HistoryPolicyAction::ReplacePolicy = value.action;
        Self::new(
            HistoryPolicyKey::new(value.family, value.scope)?,
            value.expected_policy_revision,
            value.binding,
            value.application,
        )
    }
}

pub(super) fn deadline(
    anchor: DateTime<Utc>,
    age: HistoryRetentionSeconds,
) -> Result<DateTime<Utc>, StorageValidationError> {
    anchor
        .checked_add_signed(chrono::Duration::seconds(age.value() as i64))
        .ok_or_else(|| invalid("Retention deadline is outside the supported timestamp range"))
}
