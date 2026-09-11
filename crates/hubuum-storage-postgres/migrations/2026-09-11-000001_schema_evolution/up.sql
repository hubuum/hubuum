-- Deploy during a quiet period; timeout rolls back this entire migration.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE public.class_schema_revisions (
    class_id INTEGER NOT NULL REFERENCES public.hubuumclass(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision > 0),
    json_schema JSONB,
    validate_schema BOOLEAN NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('staged', 'active', 'retired', 'abandoned')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_by INTEGER,
    activated_at TIMESTAMPTZ,
    activation_policy TEXT CHECK (activation_policy IN ('reject_incompatible', 'allow_pending')),
    PRIMARY KEY (class_id, revision),
    CHECK (NOT validate_schema OR json_schema IS NOT NULL),
    CHECK ((status IN ('active', 'retired')) = (activated_at IS NOT NULL)),
    CHECK ((activated_at IS NOT NULL) = (activation_policy IS NOT NULL))
);
CREATE UNIQUE INDEX class_schema_one_active ON public.class_schema_revisions(class_id) WHERE status = 'active'; -- hubuum-compat: bounded-transactional-index

CREATE TABLE public.class_schema_state (
    class_id INTEGER PRIMARY KEY REFERENCES public.hubuumclass(id) ON DELETE CASCADE,
    active_revision BIGINT NOT NULL CHECK (active_revision > 0),
    last_revision BIGINT NOT NULL CHECK (last_revision >= active_revision),
    object_count BIGINT NOT NULL DEFAULT 0 CHECK (object_count >= 0),
    object_epoch BIGINT NOT NULL DEFAULT 0 CHECK (object_epoch >= 0),
    FOREIGN KEY (class_id, active_revision) REFERENCES public.class_schema_revisions(class_id, revision)
        DEFERRABLE INITIALLY DEFERRED
);

-- This append-only provenance is independent of live-class deletion. It retains
-- immutable documents referenced by historical object/schema events.
CREATE TABLE public.class_schema_history (
    id BIGSERIAL PRIMARY KEY,
    class_id INTEGER NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    snapshot JSONB NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('create', 'update', 'delete')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor_id INTEGER,
    task_id INTEGER
);

CREATE TABLE object_schema_evidence (
    object_id INTEGER PRIMARY KEY REFERENCES public.hubuumobject(id) ON DELETE CASCADE,
    class_id INTEGER NOT NULL,
    schema_revision BIGINT NOT NULL,
    object_revision BIGINT NOT NULL CHECK (object_revision > 0),
    valid BOOLEAN NOT NULL,
    validated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (class_id, schema_revision) REFERENCES public.class_schema_revisions(class_id, revision) ON DELETE CASCADE
);
CREATE INDEX object_schema_evidence_class ON object_schema_evidence(class_id, schema_revision, valid, object_id); -- hubuum-compat: bounded-transactional-index

CREATE TABLE schema_validation_work (
    task_id INTEGER PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    class_id INTEGER NOT NULL REFERENCES public.hubuumclass(id) ON DELETE CASCADE,
    schema_revision BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('impact', 'revalidation')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    checkpoint JSONB NOT NULL CHECK (jsonb_typeof(checkpoint) = 'object'),
    FOREIGN KEY (class_id, schema_revision) REFERENCES public.class_schema_revisions(class_id, revision) ON DELETE CASCADE
);
CREATE UNIQUE INDEX schema_validation_work_dedup ON schema_validation_work(class_id, schema_revision, kind) WHERE active; -- hubuum-compat: bounded-transactional-index

ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check; -- hubuum-compat: widen-enum-check
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'export', 'backup', 'reindex', 'remote_call', 'schema_validation')) NOT VALID;
ALTER TABLE tasks VALIDATE CONSTRAINT tasks_kind_check;

INSERT INTO public.class_schema_revisions(class_id, revision, json_schema, validate_schema, status, activated_at, activation_policy)
SELECT id, 1, json_schema, validate_schema, 'active', clock_timestamp(), 'reject_incompatible' FROM public.hubuumclass;
INSERT INTO public.class_schema_state(class_id, active_revision, last_revision, object_count)
SELECT c.id, 1, 1, (SELECT count(*) FROM public.hubuumobject o WHERE o.hubuum_class_id=c.id) FROM public.hubuumclass c;
CREATE INDEX object_schema_scan ON public.hubuumobject(hubuum_class_id, id); -- hubuum-compat: bounded-transactional-index
-- Existing objects deliberately have no validation evidence.

CREATE FUNCTION hubuum_schema_revision_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' AND EXISTS (SELECT 1 FROM public.hubuumclass WHERE id=OLD.class_id) THEN
        RAISE EXCEPTION 'retained schema revisions cannot be deleted' USING ERRCODE='23514';
    END IF;
    IF TG_OP = 'UPDATE' AND (
        NEW.class_id IS DISTINCT FROM OLD.class_id OR NEW.revision IS DISTINCT FROM OLD.revision
        OR NEW.json_schema IS DISTINCT FROM OLD.json_schema OR NEW.validate_schema IS DISTINCT FROM OLD.validate_schema
        OR NEW.created_at IS DISTINCT FROM OLD.created_at OR NEW.created_by IS DISTINCT FROM OLD.created_by
    ) THEN
        RAISE EXCEPTION 'schema revision documents are immutable' USING ERRCODE = '23514';
    END IF;
    IF TG_OP='UPDATE' AND NEW.status<>OLD.status AND NOT ((OLD.status='staged' AND NEW.status IN ('active','abandoned')) OR (OLD.status='active' AND NEW.status='retired')) THEN
        RAISE EXCEPTION 'invalid schema revision transition' USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' AND OLD.activated_at IS NOT NULL AND (NEW.activated_at IS DISTINCT FROM OLD.activated_at OR NEW.activation_policy IS DISTINCT FROM OLD.activation_policy) THEN
        RAISE EXCEPTION 'schema activation provenance is immutable' USING ERRCODE='23514';
    END IF;
    IF NOT (current_setting('hubuum.restore_history', true) IS NOT DISTINCT FROM 'on' AND pg_has_role(session_user,current_user,'MEMBER')) THEN
        INSERT INTO public.class_schema_history(class_id, revision, snapshot, operation, actor_id, task_id)
        VALUES (COALESCE(NEW.class_id, OLD.class_id), COALESCE(NEW.revision, OLD.revision),
            CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END, CASE TG_OP WHEN 'INSERT' THEN 'create' WHEN 'UPDATE' THEN 'update' ELSE 'delete' END,
            nullif(current_setting('hubuum.actor_id', true), '')::integer,
            nullif(current_setting('hubuum.task_id', true), '')::integer);
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER schema_revision_guard BEFORE INSERT OR UPDATE OR DELETE ON public.class_schema_revisions
FOR EACH ROW EXECUTE FUNCTION hubuum_schema_revision_guard();

CREATE FUNCTION hubuum_class_schema_projection() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE target BIGINT; current_policy public.class_schema_revisions%ROWTYPE;
BEGIN
    IF current_setting('hubuum.restore_history', true) = 'on' AND pg_has_role(session_user,current_user,'MEMBER') THEN RETURN NEW; END IF;
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.class_schema_revisions(class_id, revision, json_schema, validate_schema, status, created_by, activated_at, activation_policy)
        VALUES (NEW.id, 1, NEW.json_schema, NEW.validate_schema, 'active',
            nullif(current_setting('hubuum.actor_id', true), '')::integer, clock_timestamp(), 'reject_incompatible');
        INSERT INTO public.class_schema_state(class_id, active_revision, last_revision) VALUES (NEW.id, 1, 1);
    ELSIF NEW.json_schema IS DISTINCT FROM OLD.json_schema OR NEW.validate_schema IS DISTINCT FROM OLD.validate_schema THEN
        SELECT r.* INTO current_policy FROM public.class_schema_revisions r JOIN public.class_schema_state s
        ON s.class_id=r.class_id AND s.active_revision=r.revision WHERE s.class_id=NEW.id;
        IF current_policy.json_schema IS NOT DISTINCT FROM NEW.json_schema AND current_policy.validate_schema=NEW.validate_schema THEN RETURN NEW; END IF;
        IF EXISTS (SELECT 1 FROM public.hubuumobject WHERE hubuum_class_id=NEW.id LIMIT 1) THEN
            RAISE EXCEPTION 'stage, analyze and explicitly activate schema changes for nonempty classes'
                USING ERRCODE='23514', CONSTRAINT='class_schema_activation_required';
        END IF;
        UPDATE public.class_schema_state SET last_revision=last_revision+1 WHERE class_id=NEW.id RETURNING last_revision INTO target;
        UPDATE public.class_schema_revisions SET status='retired' WHERE class_id=NEW.id AND status='active';
        INSERT INTO public.class_schema_revisions(class_id, revision, json_schema, validate_schema, status, created_by, activated_at, activation_policy)
        VALUES (NEW.id, target, NEW.json_schema, NEW.validate_schema, 'active',
            nullif(current_setting('hubuum.actor_id', true), '')::integer, clock_timestamp(), 'reject_incompatible');
        UPDATE public.class_schema_state SET active_revision=target WHERE class_id=NEW.id;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER class_schema_projection AFTER INSERT OR UPDATE ON public.hubuumclass
FOR EACH ROW EXECUTE FUNCTION hubuum_class_schema_projection();

CREATE FUNCTION hubuum_schema_projection_check() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE target_id INTEGER := COALESCE(NEW.class_id, OLD.class_id);
BEGIN
    IF EXISTS (SELECT 1 FROM public.hubuumclass c WHERE c.id=target_id) AND NOT EXISTS (
        SELECT 1 FROM public.hubuumclass c JOIN public.class_schema_state s ON s.class_id=c.id
        JOIN public.class_schema_revisions r ON r.class_id=s.class_id AND r.revision=s.active_revision
        WHERE c.id=target_id AND r.status='active' AND c.json_schema IS NOT DISTINCT FROM r.json_schema AND c.validate_schema=r.validate_schema
    ) THEN RAISE EXCEPTION 'active schema projection is inconsistent' USING ERRCODE='23514'; END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER schema_state_projection_check AFTER INSERT OR UPDATE OR DELETE ON public.class_schema_state
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hubuum_schema_projection_check();
CREATE CONSTRAINT TRIGGER schema_revision_projection_check AFTER INSERT OR UPDATE OR DELETE ON public.class_schema_revisions
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hubuum_schema_projection_check();

CREATE FUNCTION hubuum_object_schema_epoch() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF current_setting('hubuum.restore_history', true) = 'on' AND pg_has_role(session_user,current_user,'MEMBER') THEN
        IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
    END IF;
    IF TG_OP='UPDATE' AND NEW.revision=OLD.revision AND NEW.hubuum_class_id=OLD.hubuum_class_id THEN RETURN NEW; END IF;
    IF TG_OP='UPDATE' AND NEW.hubuum_class_id<>OLD.hubuum_class_id THEN
        DELETE FROM public.object_schema_evidence WHERE object_id=NEW.id;
    END IF;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        UPDATE public.class_schema_state SET object_epoch=object_epoch+1, object_count=object_count-CASE WHEN TG_OP='DELETE' OR NEW.hubuum_class_id<>OLD.hubuum_class_id THEN 1 ELSE 0 END WHERE class_id=OLD.hubuum_class_id;
    END IF;
    IF TG_OP='INSERT' OR (TG_OP='UPDATE' AND NEW.hubuum_class_id<>OLD.hubuum_class_id) THEN
        UPDATE public.class_schema_state SET object_epoch=object_epoch+1, object_count=object_count+1 WHERE class_id=NEW.hubuum_class_id;
    END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER object_schema_epoch AFTER INSERT OR UPDATE OR DELETE ON public.hubuumobject
FOR EACH ROW EXECUTE FUNCTION hubuum_object_schema_epoch();

-- Cascade deletion removes rebuildable checkpoints, but terminalizes their
-- tasks and records the cause in the durable audit/outbox before they vanish.
-- The existing event trigger also runs beneath this restricted trigger context.
ALTER FUNCTION public.record_event_related_collections() SET search_path=pg_catalog,public;
CREATE FUNCTION hubuum_schema_work_deleted() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE task_row public.tasks%ROWTYPE;
BEGIN
    IF current_setting('hubuum.restore_history', true) = 'on' AND pg_has_role(session_user,current_user,'MEMBER') THEN RETURN OLD; END IF;
    UPDATE public.tasks SET status='cancelled', finished_at=clock_timestamp(), updated_at=clock_timestamp(),
        lease_token=NULL, lease_expires_at=NULL, request_payload=NULL, request_redacted_at=clock_timestamp(),
        summary='Schema validation cancelled because its class was deleted'
    WHERE id=OLD.task_id AND status IN ('queued','validating','running') RETURNING * INTO task_row;
    IF FOUND THEN
        INSERT INTO public.events(event_id,entity_type,entity_id,action,actor_user_id,actor_kind,initiator_user_id,task_id,summary,metadata,schema_version)
        VALUES (gen_random_uuid(),'task',OLD.task_id,'cancelled',nullif(current_setting('hubuum.actor_id',true),'')::integer,
            coalesce(nullif(current_setting('hubuum.actor_kind',true),''),'system'),task_row.initiator_user_id,OLD.task_id,
            'Schema validation cancelled because its class was deleted',
            jsonb_build_object('task_id',OLD.task_id,'task_kind','schema_validation','class_id',OLD.class_id,'schema_revision',OLD.schema_revision),1);
    END IF;
    RETURN OLD;
END $$;
CREATE TRIGGER schema_work_deleted BEFORE DELETE ON public.schema_validation_work
FOR EACH ROW EXECUTE FUNCTION hubuum_schema_work_deleted();
