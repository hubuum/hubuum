SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Retained approval evidence intentionally prevents downgrade after first use.
ALTER TABLE credential_approvals DROP CONSTRAINT credential_approvals_operation_check;
ALTER TABLE credential_approvals ADD CONSTRAINT credential_approvals_operation_check
    CHECK (operation IN ('create_token', 'renew_token', 'create_user', 'update_user',
                         'import_credentials', 'confirm_restore'));
ALTER TABLE credential_approvals DROP COLUMN security_candidate_id;
DROP TABLE security_candidates;

DROP TABLE authentication_provider_bindings;
DROP FUNCTION hubuum_preserve_provider_identity_binding();
