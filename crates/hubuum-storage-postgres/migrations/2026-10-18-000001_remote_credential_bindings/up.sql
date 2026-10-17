-- Offline upgrade: stop older API servers and workers before applying this migration.
-- Existing aliases were selectable by collection managers, so their destination
-- bindings cannot be assumed to have credential-owner approval. An unscoped
-- runtime administrator must review each target and explicitly enable it again.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

UPDATE remote_targets SET enabled = FALSE
WHERE enabled AND auth_config ->> 'type' IS DISTINCT FROM 'none';
