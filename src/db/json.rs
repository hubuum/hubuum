use serde_json::{Number, Value};

const POSTGRES_NUMERIC_MAX_INTEGRAL_DIGITS: i64 = 131_072;
const POSTGRES_NUMERIC_MAX_FRACTIONAL_DIGITS: i64 = 16_383;
const POSTGRES_NUMERIC_MAX_EXPONENT_ABS: i64 = i32::MAX as i64 / 2;
pub(crate) const MAX_POSTGRES_JSONB_NESTING_DEPTH: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PostgresJsonbValidationError {
    UnsupportedValue,
    NestingTooDeep,
}

pub(crate) fn validate_postgres_jsonb_value(
    value: &Value,
) -> Result<(), PostgresJsonbValidationError> {
    let mut pending = vec![(value, 0_usize)];
    while let Some((value, depth)) = pending.pop() {
        match value {
            Value::String(value) if value.contains('\0') => {
                return Err(PostgresJsonbValidationError::UnsupportedValue);
            }
            Value::Number(value) if !postgres_numeric_can_represent(value) => {
                return Err(PostgresJsonbValidationError::UnsupportedValue);
            }
            Value::Array(values) => {
                validate_container_depth(depth)?;
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                validate_container_depth(depth)?;
                for (key, value) in values {
                    if key.contains('\0') {
                        return Err(PostgresJsonbValidationError::UnsupportedValue);
                    }
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_container_depth(depth: usize) -> Result<(), PostgresJsonbValidationError> {
    if depth >= MAX_POSTGRES_JSONB_NESTING_DEPTH {
        return Err(PostgresJsonbValidationError::NestingTooDeep);
    }
    Ok(())
}

fn postgres_numeric_can_represent(value: &Number) -> bool {
    // PostgreSQL strips leading zero groups when determining numeric weight,
    // but retains the input scale after applying any exponent.
    let source = value.to_string();
    let unsigned = source.strip_prefix('-').unwrap_or(&source);
    let exponent_start = unsigned.find(['e', 'E']);
    let (mantissa, exponent) = match exponent_start {
        Some(index) => {
            let Ok(exponent) = unsigned[index + 1..].parse::<i64>() else {
                return false;
            };
            (&unsigned[..index], exponent)
        }
        None => (unsigned, 0),
    };
    if !(-POSTGRES_NUMERIC_MAX_EXPONENT_ABS..=POSTGRES_NUMERIC_MAX_EXPONENT_ABS).contains(&exponent)
    {
        return false;
    }
    let integral_digits = mantissa.find('.').unwrap_or(mantissa.len());
    let total_digits = mantissa.len() - usize::from(mantissa.contains('.'));
    let first_nonzero = mantissa
        .bytes()
        .filter(|digit| *digit != b'.')
        .position(|digit| digit != b'0');
    let Ok(integral_digits) = i64::try_from(integral_digits) else {
        return false;
    };
    let Ok(total_digits) = i64::try_from(total_digits) else {
        return false;
    };
    let Some(decimal_position) = integral_digits.checked_add(exponent) else {
        return false;
    };
    let digits_before_decimal = match first_nonzero {
        Some(first_nonzero) => {
            let Ok(first_nonzero) = i64::try_from(first_nonzero) else {
                return false;
            };
            decimal_position.saturating_sub(first_nonzero).max(0)
        }
        None => 0,
    };
    let fractional_digits = total_digits - integral_digits;
    let digits_after_decimal = fractional_digits.saturating_sub(exponent).max(0);

    digits_before_decimal <= POSTGRES_NUMERIC_MAX_INTEGRAL_DIGITS
        && digits_after_decimal <= POSTGRES_NUMERIC_MAX_FRACTIONAL_DIGITS
}
