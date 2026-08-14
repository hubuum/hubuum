//! Portable JSON constraints shared by storage contracts and adapters.

use serde_json::{Number, Value};

const MAX_NUMERIC_INTEGRAL_DIGITS: i64 = 131_072;
const MAX_NUMERIC_FRACTIONAL_DIGITS: i64 = 16_383;
const MAX_NUMERIC_EXPONENT_ABS: i64 = i32::MAX as i64 / 2;

/// Maximum number of nested JSON containers accepted by Hubuum storage APIs.
pub const MAX_STORAGE_JSON_NESTING_DEPTH: usize = 64;

/// Stable reason that JSON cannot be represented by a Hubuum storage backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageJsonValidationError {
    /// A string, key, or number is outside the portable storage envelope.
    UnsupportedValue,
    /// The document exceeds [`MAX_STORAGE_JSON_NESTING_DEPTH`].
    NestingTooDeep,
}

/// Validate JSON against Hubuum's portable storage envelope.
///
/// The limits deliberately fit PostgreSQL JSONB, the reference backend. Other
/// backends must enforce the same envelope so API behavior does not depend on
/// the statically selected adapter.
pub fn validate_storage_json_value(value: &Value) -> Result<(), StorageJsonValidationError> {
    let mut pending = vec![(value, 0_usize)];
    while let Some((value, depth)) = pending.pop() {
        match value {
            Value::String(value) if value.contains('\0') => {
                return Err(StorageJsonValidationError::UnsupportedValue);
            }
            Value::Number(value) if !storage_numeric_can_represent(value) => {
                return Err(StorageJsonValidationError::UnsupportedValue);
            }
            Value::Array(values) => {
                validate_container_depth(depth)?;
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                validate_container_depth(depth)?;
                for (key, value) in values {
                    if key.contains('\0') {
                        return Err(StorageJsonValidationError::UnsupportedValue);
                    }
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_container_depth(depth: usize) -> Result<(), StorageJsonValidationError> {
    if depth >= MAX_STORAGE_JSON_NESTING_DEPTH {
        return Err(StorageJsonValidationError::NestingTooDeep);
    }
    Ok(())
}

fn storage_numeric_can_represent(value: &Number) -> bool {
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
    if !(-MAX_NUMERIC_EXPONENT_ABS..=MAX_NUMERIC_EXPONENT_ABS).contains(&exponent) {
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

    digits_before_decimal <= MAX_NUMERIC_INTEGRAL_DIGITS
        && digits_after_decimal <= MAX_NUMERIC_FRACTIONAL_DIGITS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_nul_strings_from_the_portable_storage_envelope() {
        assert_eq!(
            validate_storage_json_value(&Value::String("invalid\0value".to_string())),
            Err(StorageJsonValidationError::UnsupportedValue)
        );
    }

    #[test]
    fn accepts_regular_nested_json() {
        assert_eq!(
            validate_storage_json_value(&serde_json::json!({"items": [1, 2, 3]})),
            Ok(())
        );
    }
}
