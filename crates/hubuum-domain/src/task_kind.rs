#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum TaskKind {
    Import,
    Export,
    Backup,
    Reindex,
    RemoteCall,
    SchemaValidation,
}

impl TaskKind {
    pub const ALL: [Self; 6] = [
        Self::Import,
        Self::Export,
        Self::Backup,
        Self::Reindex,
        Self::RemoteCall,
        Self::SchemaValidation,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Import => "import",
            Self::Export => "export",
            Self::Backup => "backup",
            Self::Reindex => "reindex",
            Self::SchemaValidation => "schema_validation",
            Self::RemoteCall => "remote_call",
        }
    }

    #[must_use]
    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "import" => Some(Self::Import),
            "export" => Some(Self::Export),
            "backup" => Some(Self::Backup),
            "reindex" => Some(Self::Reindex),
            "schema_validation" => Some(Self::SchemaValidation),
            "remote_call" => Some(Self::RemoteCall),
            _ => None,
        }
    }
}
