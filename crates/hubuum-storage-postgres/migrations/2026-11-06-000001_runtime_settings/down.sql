UPDATE query_observation_control
SET enabled = (
    SELECT (value ->> 'enabled')::boolean
    FROM runtime_settings
    WHERE setting_group = 'query_observations'
);

DROP TABLE runtime_setting_changes;
DROP FUNCTION reject_runtime_setting_change_mutation();
DROP TABLE runtime_settings;
