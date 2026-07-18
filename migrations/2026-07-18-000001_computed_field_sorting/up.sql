CREATE FUNCTION hubuum_computed_pointer_value(input_data JSONB, pointer TEXT)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    current_value JSONB := input_data;
    raw_token TEXT;
    token TEXT;
BEGIN
    IF pointer = '' THEN
        RETURN current_value;
    END IF;
    IF left(pointer, 1) <> '/' THEN
        RETURN NULL;
    END IF;

    FOREACH raw_token IN ARRAY string_to_array(substr(pointer, 2), '/') LOOP
        token := replace(replace(raw_token, '~1', '/'), '~0', '~');
        CASE jsonb_typeof(current_value)
            WHEN 'object' THEN
                current_value := current_value -> token;
            WHEN 'array' THEN
                IF token !~ '^(0|[1-9][0-9]*)$' THEN
                    RETURN NULL;
                END IF;
                BEGIN
                    current_value := current_value -> token::INTEGER;
                EXCEPTION
                    WHEN numeric_value_out_of_range THEN
                        RETURN NULL;
                END;
            ELSE
                RETURN NULL;
        END CASE;
        IF current_value IS NULL THEN
            RETURN NULL;
        END IF;
    END LOOP;
    RETURN current_value;
END;
$$;

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
        -- NUMERIC division defaults to 20 fractional digits when both operands
        -- have scale zero. Multiplication by an exact negative power preserves
        -- the evaluator's full 34-significant-digit result.
        WHEN scale >= 0 THEN rounded_value * power(10::NUMERIC, -scale)
        ELSE rounded_value * factor
    END;
END;
$$;

CREATE FUNCTION hubuum_computed_sort_value(
    input_data JSONB,
    operation JSONB,
    result_type TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    operation_type TEXT := operation ->> 'type';
    path_value JSONB;
    first_value JSONB;
    result JSONB;
    path_item JSONB;
    numeric_value NUMERIC;
    numeric_values NUMERIC[] := ARRAY[]::NUMERIC[];
    aggregate_value NUMERIC;
    value_count INTEGER := 0;
    present_count INTEGER := 0;
    decimal_exponent INTEGER;
    decimal_scale INTEGER;
BEGIN
    IF jsonb_typeof(operation -> 'paths') <> 'array'
        OR octet_length(input_data::TEXT) > 1048576
    THEN
        RETURN NULL;
    END IF;

    IF operation_type = 'first_non_null' THEN
        FOR path_item IN SELECT value FROM jsonb_array_elements(operation -> 'paths') LOOP
            path_value := hubuum_computed_pointer_value(input_data, path_item #>> '{}');
            IF path_value IS NOT NULL AND path_value <> 'null'::JSONB THEN
                result := path_value;
                EXIT;
            END IF;
        END LOOP;
    ELSIF operation_type IN ('all_present', 'any_present', 'count_present') THEN
        FOR path_item IN SELECT value FROM jsonb_array_elements(operation -> 'paths') LOOP
            value_count := value_count + 1;
            path_value := hubuum_computed_pointer_value(input_data, path_item #>> '{}');
            IF path_value IS NOT NULL AND path_value <> 'null'::JSONB THEN
                present_count := present_count + 1;
            END IF;
        END LOOP;
        result := CASE operation_type
            WHEN 'all_present' THEN to_jsonb(present_count = value_count)
            WHEN 'any_present' THEN to_jsonb(present_count > 0)
            ELSE to_jsonb(present_count)
        END;
    ELSIF operation_type = 'all_present_and_equal' THEN
        FOR path_item IN SELECT value FROM jsonb_array_elements(operation -> 'paths') LOOP
            path_value := hubuum_computed_pointer_value(input_data, path_item #>> '{}');
            IF path_value IS NULL OR path_value = 'null'::JSONB THEN
                RETURN 'false'::JSONB;
            END IF;
            IF first_value IS NULL THEN
                first_value := path_value;
            ELSIF first_value <> path_value THEN
                RETURN 'false'::JSONB;
            END IF;
        END LOOP;
        result := 'true'::JSONB;
    ELSIF operation_type IN ('sum', 'average', 'min', 'max') THEN
        FOR path_item IN SELECT value FROM jsonb_array_elements(operation -> 'paths') LOOP
            path_value := hubuum_computed_pointer_value(input_data, path_item #>> '{}');
            IF path_value IS NULL OR path_value = 'null'::JSONB THEN
                CONTINUE;
            END IF;
            numeric_value := hubuum_computed_numeric(path_value);
            IF numeric_value IS NULL THEN
                RETURN NULL;
            END IF;
            numeric_values := array_append(numeric_values, numeric_value);
        END LOOP;
        IF cardinality(numeric_values) = 0 THEN
            RETURN NULL;
        END IF;
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
            decimal_exponent := floor(log(10, abs(aggregate_value)))::INTEGER;
            decimal_scale := 33 - decimal_exponent;
            aggregate_value := hubuum_round_half_even(aggregate_value, decimal_scale);
            IF decimal_exponent NOT BETWEEN -308 AND 308 THEN
                RETURN NULL;
            END IF;
        END IF;
        IF result_type = 'integer' AND trunc(aggregate_value) <> aggregate_value THEN
            RETURN NULL;
        END IF;
        aggregate_value := trim_scale(aggregate_value);
        result := to_jsonb(aggregate_value);
    ELSE
        RETURN NULL;
    END IF;

    IF result IS NULL OR result = 'null'::JSONB THEN
        RETURN NULL;
    END IF;
    IF (CASE result_type
        WHEN 'string' THEN jsonb_typeof(result) <> 'string'
        WHEN 'number' THEN jsonb_typeof(result) <> 'number'
        WHEN 'integer' THEN jsonb_typeof(result) <> 'number'
            OR trunc((result #>> '{}')::NUMERIC) <> (result #>> '{}')::NUMERIC
        WHEN 'boolean' THEN jsonb_typeof(result) <> 'boolean'
        WHEN 'object' THEN jsonb_typeof(result) <> 'object'
        WHEN 'array' THEN jsonb_typeof(result) <> 'array'
        ELSE TRUE
    END) THEN
        RETURN NULL;
    END IF;
    IF result_type IN ('number', 'integer')
        AND hubuum_computed_numeric(result) IS NULL
    THEN
        RETURN NULL;
    END IF;
    IF octet_length(result::TEXT) > 65536 THEN
        RETURN NULL;
    END IF;
    RETURN result;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;
