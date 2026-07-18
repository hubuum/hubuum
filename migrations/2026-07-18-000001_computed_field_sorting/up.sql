CREATE FUNCTION hubuum_computed_canonical_json(input_data JSONB)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    result TEXT;
BEGIN
    CASE jsonb_typeof(input_data)
        WHEN 'object' THEN
            SELECT '{' || COALESCE(string_agg(
                to_jsonb(entry.key)::TEXT || ':' || hubuum_computed_canonical_json(entry.value),
                ',' ORDER BY entry.key COLLATE "C"
            ), '') || '}'
            INTO result
            FROM jsonb_each(input_data) AS entry;
        WHEN 'array' THEN
            SELECT '[' || COALESCE(string_agg(
                hubuum_computed_canonical_json(entry.value),
                ',' ORDER BY entry.ordinality
            ), '') || ']'
            INTO result
            FROM jsonb_array_elements(input_data) WITH ORDINALITY AS entry(value, ordinality);
        ELSE
            result := input_data::TEXT;
    END CASE;
    RETURN result;
END;
$$;

CREATE FUNCTION hubuum_computed_source_sha256(input_data JSONB)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN encode(sha256(convert_to(hubuum_computed_canonical_json(input_data), 'UTF8')), 'hex');

CREATE TYPE hubuum_computed_pointer_result AS (
    value JSONB,
    found BOOLEAN,
    work_units INTEGER,
    limit_exceeded BOOLEAN
);

CREATE FUNCTION hubuum_computed_resolve_pointer(
    input_data JSONB,
    pointer TEXT,
    max_work_units INTEGER
)
RETURNS hubuum_computed_pointer_result
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    current_value JSONB := input_data;
    raw_tokens TEXT[];
    raw_token TEXT;
    token TEXT;
    work_units INTEGER := 0;
BEGIN
    IF pointer = '' THEN
        RETURN (current_value, TRUE, 0, FALSE);
    END IF;
    IF left(pointer, 1) <> '/' THEN
        RETURN (NULL::JSONB, FALSE, 0, FALSE);
    END IF;
    raw_tokens := CASE
        WHEN pointer = '/' THEN ARRAY['']::TEXT[]
        ELSE string_to_array(substr(pointer, 2), '/')
    END;

    FOREACH raw_token IN ARRAY raw_tokens LOOP
        work_units := work_units + 1;
        IF work_units > GREATEST(max_work_units, 0) THEN
            RETURN (NULL::JSONB, FALSE, work_units, TRUE);
        END IF;
        token := replace(replace(raw_token, '~1', '/'), '~0', '~');
        CASE jsonb_typeof(current_value)
            WHEN 'object' THEN
                IF NOT current_value ? token THEN
                    RETURN (NULL::JSONB, FALSE, work_units, FALSE);
                END IF;
                current_value := current_value -> token;
            WHEN 'array' THEN
                IF token !~ '^(0|[1-9][0-9]*)$' THEN
                    RETURN (NULL::JSONB, FALSE, work_units, FALSE);
                END IF;
                BEGIN
                    current_value := current_value -> token::INTEGER;
                EXCEPTION
                    WHEN numeric_value_out_of_range THEN
                        RETURN (NULL::JSONB, FALSE, work_units, FALSE);
                END;
                IF current_value IS NULL THEN
                    RETURN (NULL::JSONB, FALSE, work_units, FALSE);
                END IF;
            ELSE
                RETURN (NULL::JSONB, FALSE, work_units, FALSE);
        END CASE;
    END LOOP;
    RETURN (current_value, TRUE, work_units, FALSE);
END;
$$;

CREATE FUNCTION hubuum_computed_pointer_value(input_data JSONB, pointer TEXT)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN (hubuum_computed_resolve_pointer(input_data, pointer, 2147483647)).value;

CREATE FUNCTION hubuum_computed_numeric(value JSONB)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    source TEXT;
    unsigned_source TEXT;
    integer_part TEXT;
    fractional_part TEXT;
    digits TEXT;
    first_nonzero INTEGER;
    last_nonzero INTEGER;
    significant_digits INTEGER;
    decimal_exponent INTEGER;
BEGIN
    IF jsonb_typeof(value) <> 'number' THEN
        RETURN NULL;
    END IF;
    source := value #>> '{}';
    IF octet_length(source) > 512 THEN
        RETURN NULL;
    END IF;
    unsigned_source := ltrim(source, '+-');
    integer_part := split_part(unsigned_source, '.', 1);
    fractional_part := CASE
        WHEN position('.' IN unsigned_source) > 0 THEN split_part(unsigned_source, '.', 2)
        ELSE ''
    END;
    digits := integer_part || fractional_part;
    first_nonzero := NULLIF(position('1' IN translate(digits, '23456789', '11111111')), 0);
    IF first_nonzero IS NULL THEN
        RETURN 0;
    END IF;
    last_nonzero := length(digits)
        - position('1' IN reverse(translate(digits, '23456789', '11111111'))) + 1;
    significant_digits := last_nonzero - first_nonzero + 1;
    decimal_exponent := length(integer_part) - first_nonzero;
    IF significant_digits > 34 OR decimal_exponent NOT BETWEEN -308 AND 308 THEN
        RETURN NULL;
    END IF;
    RETURN source::NUMERIC;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

CREATE FUNCTION hubuum_round_half_even(value NUMERIC, scale INTEGER)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    factor NUMERIC := power(10::NUMERIC, abs(scale));
    shifted NUMERIC;
    lower_value NUMERIC;
    fraction NUMERIC;
    rounded_value NUMERIC;
BEGIN
    shifted := CASE
        WHEN scale >= 0 THEN value * factor
        ELSE value / factor
    END;
    lower_value := floor(abs(shifted));
    fraction := abs(shifted) - lower_value;
    rounded_value := CASE
        WHEN fraction < 0.5 THEN lower_value
        WHEN fraction > 0.5 THEN lower_value + 1
        WHEN mod(lower_value, 2) = 0 THEN lower_value
        ELSE lower_value + 1
    END;
    rounded_value := rounded_value * sign(value);
    RETURN CASE
        WHEN scale >= 0 THEN rounded_value * power(10::NUMERIC, -scale)
        ELSE rounded_value * factor
    END;
END;
$$;

CREATE TYPE hubuum_computed_equality_result AS (
    equal BOOLEAN,
    work_units INTEGER,
    limit_exceeded BOOLEAN,
    numeric_out_of_range BOOLEAN
);

CREATE FUNCTION hubuum_computed_values_equal(
    left_value JSONB,
    right_value JSONB,
    max_work_units INTEGER
)
RETURNS hubuum_computed_equality_result
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    work_units INTEGER := 1;
    index_value INTEGER;
    key_value TEXT;
    child hubuum_computed_equality_result;
BEGIN
    IF work_units > GREATEST(max_work_units, 0) THEN
        RETURN (FALSE, work_units, TRUE, FALSE);
    END IF;
    IF jsonb_typeof(left_value) <> jsonb_typeof(right_value) THEN
        RETURN (FALSE, work_units, FALSE, FALSE);
    END IF;

    CASE jsonb_typeof(left_value)
        WHEN 'number' THEN
            IF hubuum_computed_numeric(left_value) IS NULL
                OR hubuum_computed_numeric(right_value) IS NULL
            THEN
                RETURN (FALSE, work_units, FALSE, TRUE);
            END IF;
            RETURN (left_value = right_value, work_units, FALSE, FALSE);
        WHEN 'array' THEN
            IF jsonb_array_length(left_value) <> jsonb_array_length(right_value) THEN
                RETURN (FALSE, work_units, FALSE, FALSE);
            END IF;
            FOR index_value IN 0..jsonb_array_length(left_value) - 1 LOOP
                child := hubuum_computed_values_equal(
                    left_value -> index_value,
                    right_value -> index_value,
                    GREATEST(max_work_units - work_units, 0)
                );
                work_units := work_units + child.work_units;
                IF child.limit_exceeded OR child.numeric_out_of_range OR NOT child.equal THEN
                    RETURN (
                        FALSE,
                        work_units,
                        child.limit_exceeded,
                        child.numeric_out_of_range
                    );
                END IF;
            END LOOP;
            RETURN (TRUE, work_units, FALSE, FALSE);
        WHEN 'object' THEN
            IF (SELECT count(*) FROM jsonb_each(left_value))
                <> (SELECT count(*) FROM jsonb_each(right_value))
            THEN
                RETURN (FALSE, work_units, FALSE, FALSE);
            END IF;
            FOR key_value IN
                SELECT entry.key
                FROM jsonb_each(left_value) AS entry
                ORDER BY entry.key COLLATE "C"
            LOOP
                IF NOT right_value ? key_value THEN
                    RETURN (FALSE, work_units, FALSE, FALSE);
                END IF;
                child := hubuum_computed_values_equal(
                    left_value -> key_value,
                    right_value -> key_value,
                    GREATEST(max_work_units - work_units, 0)
                );
                work_units := work_units + child.work_units;
                IF child.limit_exceeded OR child.numeric_out_of_range OR NOT child.equal THEN
                    RETURN (
                        FALSE,
                        work_units,
                        child.limit_exceeded,
                        child.numeric_out_of_range
                    );
                END IF;
            END LOOP;
            RETURN (TRUE, work_units, FALSE, FALSE);
        ELSE
            RETURN (left_value = right_value, work_units, FALSE, FALSE);
    END CASE;
END;
$$;

CREATE FUNCTION hubuum_computed_error(code TEXT, message TEXT, path TEXT DEFAULT NULL)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURN CASE
    WHEN path IS NULL THEN jsonb_build_object('code', code, 'message', message)
    ELSE jsonb_build_object('code', code, 'path', path, 'message', message)
END;

CREATE TYPE hubuum_computed_field_result AS (
    value JSONB,
    error JSONB,
    work_units INTEGER
);

CREATE FUNCTION hubuum_computed_evaluate_field(
    input_data JSONB,
    operation JSONB,
    result_type TEXT,
    scope_work_units INTEGER
)
RETURNS hubuum_computed_field_result
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    operation_type TEXT := operation ->> 'type';
    result_value JSONB := 'null'::JSONB;
    error_value JSONB;
    path_item JSONB;
    pointer TEXT;
    resolved hubuum_computed_pointer_result;
    equality hubuum_computed_equality_result;
    first_value JSONB;
    numeric_value NUMERIC;
    numeric_values NUMERIC[] := ARRAY[]::NUMERIC[];
    aggregate_value NUMERIC;
    value_count INTEGER := 0;
    present_count INTEGER := 0;
    aggregate_source TEXT;
    aggregate_integer_part TEXT;
    aggregate_fractional_part TEXT;
    aggregate_first_nonzero INTEGER;
    decimal_exponent INTEGER;
    decimal_scale INTEGER;
    field_work_units INTEGER := 0;
    remaining_work INTEGER;
BEGIN
    FOR path_item IN SELECT value FROM jsonb_array_elements(operation -> 'paths') LOOP
        pointer := path_item #>> '{}';
        remaining_work := LEAST(
            10000 - field_work_units,
            50000 - scope_work_units - field_work_units
        );
        resolved := hubuum_computed_resolve_pointer(
            input_data,
            pointer,
            GREATEST(remaining_work, 0)
        );
        field_work_units := field_work_units + resolved.work_units;
        IF resolved.limit_exceeded THEN
            error_value := hubuum_computed_error(
                'evaluation_limit_exceeded',
                'Computed field evaluation exceeded its work limit'
            );
            RETURN ('null'::JSONB, error_value, field_work_units);
        END IF;

        IF operation_type = 'first_non_null' THEN
            IF resolved.found AND resolved.value <> 'null'::JSONB THEN
                result_value := resolved.value;
                EXIT;
            END IF;
        ELSIF operation_type = 'all_present' THEN
            IF NOT resolved.found OR resolved.value = 'null'::JSONB THEN
                result_value := 'false'::JSONB;
                EXIT;
            END IF;
            result_value := 'true'::JSONB;
        ELSIF operation_type = 'any_present' THEN
            IF resolved.found AND resolved.value <> 'null'::JSONB THEN
                result_value := 'true'::JSONB;
                EXIT;
            END IF;
            result_value := 'false'::JSONB;
        ELSIF operation_type = 'count_present' THEN
            IF resolved.found AND resolved.value <> 'null'::JSONB THEN
                present_count := present_count + 1;
            END IF;
            result_value := to_jsonb(present_count);
        ELSIF operation_type = 'all_present_and_equal' THEN
            IF NOT resolved.found OR resolved.value = 'null'::JSONB THEN
                result_value := 'false'::JSONB;
                EXIT;
            END IF;
            IF first_value IS NULL THEN
                first_value := resolved.value;
                result_value := 'true'::JSONB;
            ELSE
                remaining_work := LEAST(
                    10000 - field_work_units,
                    50000 - scope_work_units - field_work_units
                );
                equality := hubuum_computed_values_equal(
                    first_value,
                    resolved.value,
                    GREATEST(remaining_work, 0)
                );
                field_work_units := field_work_units + equality.work_units;
                IF equality.limit_exceeded THEN
                    error_value := hubuum_computed_error(
                        'evaluation_limit_exceeded',
                        'Computed field evaluation exceeded its work limit'
                    );
                    RETURN ('null'::JSONB, error_value, field_work_units);
                END IF;
                IF equality.numeric_out_of_range THEN
                    error_value := hubuum_computed_error(
                        'numeric_out_of_range',
                        'Number is outside the supported decimal range'
                    );
                    RETURN ('null'::JSONB, error_value, field_work_units);
                END IF;
                IF NOT equality.equal THEN
                    result_value := 'false'::JSONB;
                    EXIT;
                END IF;
            END IF;
        ELSIF operation_type IN ('sum', 'average', 'min', 'max') THEN
            IF NOT resolved.found OR resolved.value = 'null'::JSONB THEN
                CONTINUE;
            END IF;
            IF jsonb_typeof(resolved.value) <> 'number' THEN
                error_value := hubuum_computed_error(
                    'non_numeric_operand',
                    'Expected a number',
                    pointer
                );
                RETURN ('null'::JSONB, error_value, field_work_units);
            END IF;
            numeric_value := hubuum_computed_numeric(resolved.value);
            IF numeric_value IS NULL THEN
                error_value := hubuum_computed_error(
                    'numeric_out_of_range',
                    'Number is outside the supported decimal range',
                    pointer
                );
                RETURN ('null'::JSONB, error_value, field_work_units);
            END IF;
            numeric_values := array_append(numeric_values, numeric_value);
        ELSE
            RETURN (result_value, NULL::JSONB, field_work_units);
        END IF;
    END LOOP;

    IF operation_type IN ('sum', 'average', 'min', 'max') THEN
        IF cardinality(numeric_values) = 0 THEN
            result_value := 'null'::JSONB;
        ELSE
            IF operation_type = 'average' THEN
                SELECT sum(value), count(*)
                INTO aggregate_value, value_count
                FROM unnest(numeric_values) AS values(value);
                aggregate_value := aggregate_value::NUMERIC(1000, 500)
                    / value_count::NUMERIC(1000, 500);
            ELSE
                SELECT CASE operation_type
                    WHEN 'sum' THEN sum(value)
                    WHEN 'min' THEN min(value)
                    WHEN 'max' THEN max(value)
                END
                INTO aggregate_value
                FROM unnest(numeric_values) AS values(value);
            END IF;

            IF aggregate_value <> 0 THEN
                aggregate_source := abs(aggregate_value)::TEXT;
                aggregate_integer_part := split_part(aggregate_source, '.', 1);
                aggregate_fractional_part := CASE
                    WHEN position('.' IN aggregate_source) > 0
                        THEN split_part(aggregate_source, '.', 2)
                    ELSE ''
                END;
                IF aggregate_integer_part <> '0' THEN
                    decimal_exponent := length(aggregate_integer_part) - 1;
                ELSE
                    aggregate_first_nonzero := NULLIF(position(
                        '1' IN translate(
                            aggregate_fractional_part,
                            '23456789',
                            '11111111'
                        )
                    ), 0);
                    decimal_exponent := -aggregate_first_nonzero;
                END IF;
                decimal_scale := 33 - decimal_exponent;
                aggregate_value := hubuum_round_half_even(aggregate_value, decimal_scale);
                IF decimal_exponent NOT BETWEEN -308 AND 308 THEN
                    error_value := hubuum_computed_error(
                        'numeric_out_of_range',
                        'Number is outside the supported decimal range'
                    );
                    RETURN ('null'::JSONB, error_value, field_work_units);
                END IF;
            END IF;
            result_value := to_jsonb(trim_scale(aggregate_value));
        END IF;
    END IF;

    IF result_value <> 'null'::JSONB AND (CASE result_type
        WHEN 'string' THEN jsonb_typeof(result_value) <> 'string'
        WHEN 'number' THEN jsonb_typeof(result_value) <> 'number'
        WHEN 'integer' THEN jsonb_typeof(result_value) <> 'number'
        WHEN 'boolean' THEN jsonb_typeof(result_value) <> 'boolean'
        WHEN 'object' THEN jsonb_typeof(result_value) <> 'object'
        WHEN 'array' THEN jsonb_typeof(result_value) <> 'array'
        ELSE TRUE
    END) THEN
        error_value := hubuum_computed_error(
            'result_type_mismatch',
            'Computed value does not match the definition result type'
        );
        RETURN ('null'::JSONB, error_value, field_work_units);
    END IF;
    IF jsonb_typeof(result_value) = 'number' THEN
        numeric_value := hubuum_computed_numeric(result_value);
        IF numeric_value IS NULL THEN
            error_value := hubuum_computed_error(
                'numeric_out_of_range',
                'Number is outside the supported decimal range'
            );
            RETURN ('null'::JSONB, error_value, field_work_units);
        END IF;
        IF result_type = 'integer' AND trunc(numeric_value) <> numeric_value THEN
            error_value := hubuum_computed_error(
                'non_integer_result',
                'Computed value is not an integer'
            );
            RETURN ('null'::JSONB, error_value, field_work_units);
        END IF;
    END IF;
    RETURN (result_value, NULL::JSONB, field_work_units);
EXCEPTION
    WHEN OTHERS THEN
        RETURN ('null'::JSONB, NULL::JSONB, field_work_units);
END;
$$;

CREATE FUNCTION hubuum_computed_limit_result(
    definitions JSONB,
    code TEXT,
    message TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    definition JSONB;
    values_result JSONB := '{}'::JSONB;
    errors_result JSONB := '{}'::JSONB;
    key_value TEXT;
BEGIN
    FOR definition IN SELECT value FROM jsonb_array_elements(definitions) LOOP
        key_value := definition ->> 'key';
        values_result := values_result || jsonb_build_object(key_value, 'null'::JSONB);
        errors_result := errors_result || jsonb_build_object(
            key_value,
            hubuum_computed_error(code, message)
        );
    END LOOP;
    RETURN jsonb_build_object('values', values_result, 'errors', errors_result);
END;
$$;

CREATE FUNCTION hubuum_computed_evaluate_scope(input_data JSONB, definitions JSONB)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    definition JSONB;
    field_result hubuum_computed_field_result;
    values_result JSONB := '{}'::JSONB;
    errors_result JSONB := '{}'::JSONB;
    result JSONB;
    key_value TEXT;
    scope_work_units INTEGER := 0;
BEGIN
    IF octet_length(convert_to(hubuum_computed_canonical_json(input_data), 'UTF8')) > 1048576 THEN
        RETURN hubuum_computed_limit_result(
            definitions,
            'input_too_large',
            'Computed field input exceeds the size limit'
        );
    END IF;

    FOR definition IN SELECT value FROM jsonb_array_elements(definitions) LOOP
        key_value := definition ->> 'key';
        field_result := hubuum_computed_evaluate_field(
            input_data,
            definition -> 'operation',
            definition ->> 'result_type',
            scope_work_units
        );
        scope_work_units := scope_work_units + field_result.work_units;
        IF octet_length(convert_to(
            hubuum_computed_canonical_json(field_result.value),
            'UTF8'
        )) > 65536 THEN
            field_result.value := 'null'::JSONB;
            field_result.error := hubuum_computed_error(
                'result_too_large',
                'Computed field result exceeds the size limit'
            );
        END IF;
        values_result := values_result || jsonb_build_object(key_value, field_result.value);
        IF field_result.error IS NOT NULL THEN
            errors_result := errors_result || jsonb_build_object(key_value, field_result.error);
        END IF;
    END LOOP;

    result := jsonb_build_object('values', values_result, 'errors', errors_result);
    IF octet_length(convert_to(hubuum_computed_canonical_json(result), 'UTF8')) > 262144 THEN
        RETURN hubuum_computed_limit_result(
            definitions,
            'result_too_large',
            'Computed scope result exceeds the size limit'
        );
    END IF;
    RETURN result;
END;
$$;

CREATE FUNCTION hubuum_computed_sort_value(
    input_data JSONB,
    operation JSONB,
    result_type TEXT
)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN NULLIF(
    hubuum_computed_evaluate_scope(
        input_data,
        jsonb_build_array(jsonb_build_object(
            'key', 'sort_value',
            'operation', operation,
            'result_type', result_type
        ))
    ) -> 'values' -> 'sort_value',
    'null'::JSONB
);
