SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE FUNCTION record_shared_computed_field_history(target_class INTEGER)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    previous public.computed_field_history%ROWTYPE;
    snapshot JSONB;
    boundary TIMESTAMPTZ;
BEGIN
    -- Live definition writers hold the class row before recording history.
    -- A class/collection DELETE already owns that row and makes it invisible
    -- here. Do not take the advisory lock on that path: a computed worker may
    -- hold it while waiting for the deleted row, which would invert the order.
    IF EXISTS (SELECT 1 FROM public.hubuumclass WHERE id = target_class) THEN
        PERFORM pg_advisory_xact_lock(1133113, target_class);
    END IF;
    SELECT * INTO previous FROM public.computed_field_history
        WHERE class_id = target_class AND valid_to IS NULL FOR UPDATE;
    boundary := GREATEST(clock_timestamp(), previous.valid_from + INTERVAL '1 microsecond');
    IF NOT EXISTS (SELECT 1 FROM public.hubuumclass WHERE id = target_class) THEN
        UPDATE public.computed_field_history SET valid_to = boundary
            WHERE class_id = target_class AND valid_to IS NULL;
        RETURN;
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'id', id, 'class_id', class_id, 'revision', revision,
        'definition', jsonb_build_object(
            'key', key, 'label', label, 'description', description,
            'operation', operation, 'result_type', result_type,
            'enabled', enabled, 'semantics_version', semantics_version
        )
    ) ORDER BY id), '[]'::jsonb) INTO snapshot
        FROM public.computed_field_definitions WHERE class_id = target_class AND visibility = 'shared';
    IF previous.definitions = snapshot AND previous.evaluator_version = 1 THEN
        RETURN;
    END IF;
    UPDATE public.computed_field_history SET valid_to = boundary
        WHERE class_id = target_class AND valid_to IS NULL;
    INSERT INTO public.computed_field_history (class_id, valid_from, evaluator_version, definitions)
        VALUES (target_class, boundary, 1, snapshot);
END;
$$;
