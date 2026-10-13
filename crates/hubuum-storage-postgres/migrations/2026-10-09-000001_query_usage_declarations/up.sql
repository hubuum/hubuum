SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE public.query_usage_declarations (
    id SERIAL PRIMARY KEY,
    class_id INTEGER NOT NULL REFERENCES public.hubuumclass(id) ON DELETE CASCADE,
    pattern JSONB NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_by INTEGER CHECK (created_by > 0),
    updated_by INTEGER CHECK (updated_by > 0),
    CHECK (updated_at >= created_at),
    CHECK (jsonb_typeof(pattern) = 'object' AND pattern ?& ARRAY['path','value_type','operations'] AND pattern - ARRAY['path','value_type','operations'] = '{}'::jsonb),
    CHECK (jsonb_typeof(pattern->'path') = 'string' AND octet_length(pattern->>'path') <= 512 AND (pattern->>'path') ~ '^[a-zA-Z0-9_$]+(,[a-zA-Z0-9_$]+){0,31}$'),
    CHECK (jsonb_typeof(pattern->'value_type') = 'string' AND pattern->>'value_type' IN ('string', 'numeric', 'boolean')),
    CHECK (jsonb_typeof(pattern->'operations') = 'array' AND jsonb_array_length(pattern->'operations') BETWEEN 1 AND 6),
    CHECK (pattern->'operations' <@ '["equals","gt","gte","lt","lte","between"]'::jsonb),
    CHECK (pattern->>'value_type' = 'numeric' OR pattern->'operations' = '["equals"]'::jsonb)
);
CREATE UNIQUE INDEX query_usage_declaration_pattern ON public.query_usage_declarations(class_id, (pattern->>'path'), (pattern->>'value_type')); -- hubuum-compat: bounded-transactional-index

-- Parent locking serializes quotas with declarations, class moves and deletion.
CREATE FUNCTION public.guard_query_usage_declaration() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    PERFORM 1 FROM public.hubuumclass WHERE id=NEW.class_id FOR UPDATE;
    IF TG_OP='UPDATE' AND (NEW.id<>OLD.id OR NEW.class_id<>OLD.class_id OR NEW.created_at<>OLD.created_at OR NEW.created_by IS DISTINCT FROM OLD.created_by) THEN
        RAISE EXCEPTION 'query usage declaration identity is immutable' USING ERRCODE='23514';
    END IF;
    IF TG_OP='INSERT' AND (SELECT count(*) FROM public.query_usage_declarations WHERE class_id=NEW.class_id)>=32 THEN
        RAISE EXCEPTION 'class query usage declaration limit reached' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER query_usage_declaration_guard BEFORE INSERT OR UPDATE ON public.query_usage_declarations
FOR EACH ROW EXECUTE FUNCTION public.guard_query_usage_declaration();
