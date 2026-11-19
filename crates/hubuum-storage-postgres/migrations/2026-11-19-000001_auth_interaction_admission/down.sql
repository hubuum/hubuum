DROP FUNCTION hubuum_clear_provider_memberships(TEXT[]);
-- Restore the legacy shape for migration rehearsal. A production downgrade
-- requires the pre-upgrade backup, including its original grants and settings.
ALTER TABLE authentication_provider_bindings DISABLE TRIGGER authentication_provider_binding_immutable;
UPDATE authentication_provider_bindings SET namespace=left(namespace,-68) || ']'
WHERE CASE WHEN kind='oauth' THEN jsonb_array_length(namespace::jsonb)=4 ELSE false END;
ALTER TABLE authentication_provider_bindings ENABLE TRIGGER authentication_provider_binding_immutable;
ALTER TABLE authentication_provider_bindings DROP CONSTRAINT authentication_provider_bindings_namespace_check;
ALTER TABLE authentication_provider_bindings ADD CONSTRAINT authentication_provider_bindings_namespace_check
    CHECK (length(namespace) BETWEEN 1 AND 4096);
UPDATE auth_upstream_credentials c SET document=jsonb_set(c.document,'{namespace}',to_jsonb(b.namespace))
FROM authentication_provider_bindings b WHERE b.kind='oauth' AND b.provider_id=c.provider_id;
DROP TRIGGER auth_interactions_index_deadline ON auth_interactions;
DROP FUNCTION hubuum_index_auth_interaction_deadline();
ALTER TABLE auth_interactions DROP COLUMN active_until;
DROP TRIGGER auth_interactions_count_insert ON auth_interactions;
DROP TRIGGER auth_interactions_count_delete ON auth_interactions;
DROP TRIGGER auth_interactions_count_truncate ON auth_interactions;
DROP FUNCTION hubuum_count_auth_interactions();
DROP TABLE auth_interaction_capacity;
