-- Adapter-local physical ownership is deliberately absent from portable backups.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE query_usage_resources (
    id BIGSERIAL PRIMARY KEY,
    path TEXT NOT NULL UNIQUE CHECK (octet_length(path) BETWEEN 1 AND 512 AND path ~ '^[A-Za-z0-9_$]+(,[A-Za-z0-9_$]+)*$'),
    identity UUID NOT NULL DEFAULT gen_random_uuid(),
    index_oid OID,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','ready','cleanup')),
    last_error TEXT CHECK (last_error IN ('native_operation_failed','source_budget_exceeded','identity_mismatch','catalog_budget_exceeded')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE query_usage_resource_owners (
    declaration_id INTEGER PRIMARY KEY REFERENCES query_usage_declarations(id) ON DELETE CASCADE,
    resource_id BIGINT NOT NULL REFERENCES query_usage_resources(id) ON DELETE CASCADE
);
-- Bounded transactional build: this table is new and empty in this migration.
CREATE INDEX query_usage_resource_owners_resource ON query_usage_resource_owners(resource_id); -- hubuum-compat: bounded-transactional-index

-- API roles can withdraw their own declaration's ownership, but cannot allocate
-- resources or forge ownership records. No dynamic SQL or caller-supplied names.
CREATE FUNCTION hubuum_withdraw_query_usage_resource() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.pattern IS DISTINCT FROM NEW.pattern THEN
            DELETE FROM public.query_usage_resource_owners WHERE declaration_id = OLD.id;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'TRUNCATE' THEN
        UPDATE public.query_usage_resources SET state = 'cleanup', updated_at = clock_timestamp();
        RETURN NULL;
    ELSE
        -- Serialize concurrent last-owner withdrawals before assessing the
        -- remaining owners; the next statement sees a fresh committed snapshot.
        PERFORM 1 FROM public.query_usage_resources WHERE id = OLD.resource_id FOR UPDATE;
        UPDATE public.query_usage_resources r SET state = 'cleanup', updated_at = clock_timestamp()
        WHERE r.id = OLD.resource_id AND NOT EXISTS (
            SELECT 1 FROM public.query_usage_resource_owners o WHERE o.resource_id = r.id
        );
        RETURN OLD;
    END IF;
END;
$$;
REVOKE ALL ON FUNCTION hubuum_withdraw_query_usage_resource() FROM PUBLIC;
CREATE TRIGGER query_usage_pattern_withdrawal BEFORE UPDATE OF pattern ON query_usage_declarations
FOR EACH ROW EXECUTE FUNCTION hubuum_withdraw_query_usage_resource();
CREATE TRIGGER query_usage_owner_withdrawal AFTER DELETE ON query_usage_resource_owners
FOR EACH ROW EXECUTE FUNCTION hubuum_withdraw_query_usage_resource();
CREATE TRIGGER query_usage_owners_restore AFTER TRUNCATE ON query_usage_resource_owners
FOR EACH STATEMENT EXECUTE FUNCTION hubuum_withdraw_query_usage_resource();

-- A single durable scan cursor prevents covered or unsupported declarations
-- from permanently occupying the front of every bounded planning batch.
CREATE TABLE query_usage_executor_cursor (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    after_declaration_id INTEGER NOT NULL DEFAULT 0 CHECK (after_declaration_id >= 0)
);
INSERT INTO query_usage_executor_cursor DEFAULT VALUES;
