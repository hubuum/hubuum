-- Shared runtime settings are deployment configuration. Like the query
-- observation override they replace, they and their audit trail survive
-- logical backup restores and are excluded from portable backups.
CREATE TABLE runtime_settings (
    setting_group TEXT PRIMARY KEY
        CONSTRAINT runtime_settings_group_check
        CHECK (setting_group IN ('query_observations', 'pagination')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    -- NULL records deployment ownership; an object is the managed value.
    value JSONB CHECK (value IS NULL OR jsonb_typeof(value) = 'object'),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Attribution survives principal deletion, as with event actors.
    changed_by INTEGER
);

CREATE TABLE runtime_setting_changes (
    setting_group TEXT NOT NULL REFERENCES runtime_settings (setting_group),
    revision BIGINT NOT NULL CHECK (revision > 1),
    previous_value JSONB
        CHECK (previous_value IS NULL OR jsonb_typeof(previous_value) = 'object'),
    value JSONB CHECK (value IS NULL OR jsonb_typeof(value) = 'object'),
    changed_at TIMESTAMPTZ NOT NULL,
    actor_kind TEXT NOT NULL
        CONSTRAINT runtime_setting_changes_actor_kind_check
        CHECK (actor_kind IN ('user', 'system', 'worker')),
    actor_principal_id INTEGER,
    request_id UUID,
    correlation_id TEXT
        CHECK (correlation_id IS NULL OR octet_length(correlation_id) BETWEEN 1 AND 128),
    PRIMARY KEY (setting_group, revision)
);

CREATE FUNCTION reject_runtime_setting_change_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'runtime setting changes are append-only'
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER runtime_setting_changes_append_only
    BEFORE UPDATE OR DELETE ON runtime_setting_changes
    FOR EACH ROW EXECUTE FUNCTION reject_runtime_setting_change_mutation();

CREATE TRIGGER runtime_setting_changes_no_truncate
    BEFORE TRUNCATE ON runtime_setting_changes
    FOR EACH STATEMENT EXECUTE FUNCTION reject_runtime_setting_change_mutation();

INSERT INTO runtime_settings (setting_group, revision, value)
VALUES ('query_observations', 1, NULL), ('pagination', 1, NULL);

-- Adopt an existing query observation override once, as an audited system
-- revision. Afterwards the runtime settings record is authoritative; the
-- legacy row is only mirrored for replicas of the previous release.
WITH legacy AS (
    SELECT enabled FROM query_observation_control WHERE enabled IS NOT NULL
), adopted AS (
    UPDATE runtime_settings
    SET revision = 2, value = jsonb_build_object('enabled', legacy.enabled), changed_at = now()
    FROM legacy
    WHERE setting_group = 'query_observations'
    RETURNING value, changed_at
)
INSERT INTO runtime_setting_changes (
    setting_group, revision, previous_value, value, changed_at, actor_kind
)
SELECT 'query_observations', 2, NULL, value, changed_at, 'system' FROM adopted;
