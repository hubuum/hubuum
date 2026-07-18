//! Deterministic evaluation of typed computed fields over one JSON document.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;

use bigdecimal::{BigDecimal, RoundingMode};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};
use thiserror::Error;

pub const SEMANTICS_VERSION: i16 = 1;
pub const MAX_SHARED_DEFINITIONS: usize = 32;
pub const MAX_PERSONAL_DEFINITIONS: usize = 16;
pub const MAX_PATHS_PER_DEFINITION: usize = 16;
pub const MAX_KEY_BYTES: usize = 64;
pub const MAX_LABEL_BYTES: usize = 128;
pub const MAX_DESCRIPTION_BYTES: usize = 2_048;
pub const MAX_POINTER_BYTES: usize = 512;
pub const MAX_POINTER_TOKENS: usize = 32;
pub const MAX_DECIMAL_SIGNIFICANT_DIGITS: usize = 34;
pub const MIN_DECIMAL_EXPONENT: i64 = -308;
pub const MAX_DECIMAL_EXPONENT: i64 = 308;
const MAX_DECIMAL_SOURCE_BYTES: usize = 512;
pub const MAX_INPUT_BYTES: usize = 1024 * 1024;

pub fn compare_decimal_strings(left: &str, right: &str) -> Option<Ordering> {
    let left = BigDecimal::from_str(left).ok()?;
    let right = BigDecimal::from_str(right).ok()?;
    Some(left.cmp(&right))
}

/// Release-owned safety limits applied to one evaluation scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationLimits {
    max_input_bytes: usize,
    max_nodes_per_field: usize,
    max_work_units: usize,
    max_field_output_bytes: usize,
    max_scope_output_bytes: usize,
}

impl EvaluationLimits {
    pub const fn standard() -> Self {
        Self {
            max_input_bytes: MAX_INPUT_BYTES,
            max_nodes_per_field: 10_000,
            max_work_units: 50_000,
            max_field_output_bytes: 64 * 1_024,
            max_scope_output_bytes: 256 * 1_024,
        }
    }

    #[cfg(test)]
    fn for_tests(
        max_input_bytes: usize,
        max_nodes_per_field: usize,
        max_work_units: usize,
        max_field_output_bytes: usize,
        max_scope_output_bytes: usize,
    ) -> Self {
        Self {
            max_input_bytes,
            max_nodes_per_field,
            max_work_units,
            max_field_output_bytes,
            max_scope_output_bytes,
        }
    }
}

impl Default for EvaluationLimits {
    fn default() -> Self {
        Self::standard()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FieldKey(String);

impl FieldKey {
    pub fn new(value: impl Into<String>) -> Result<Self, DefinitionError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_KEY_BYTES
            && value
                .bytes()
                .enumerate()
                .all(|(index, byte)| match (index, byte) {
                    (0, b'a'..=b'z') => true,
                    (0, _) => false,
                    (_, b'a'..=b'z' | b'0'..=b'9' | b'_') => true,
                    (_, _) => false,
                });
        if !valid {
            return Err(DefinitionError::InvalidKey);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FieldKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for FieldKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for FieldKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonPointer {
    raw: String,
    tokens: Vec<String>,
}

impl JsonPointer {
    pub fn new(value: impl Into<String>) -> Result<Self, DefinitionError> {
        let raw = value.into();
        if raw.len() > MAX_POINTER_BYTES || (!raw.is_empty() && !raw.starts_with('/')) {
            return Err(DefinitionError::InvalidPointer(raw));
        }

        let tokens = if raw.is_empty() {
            Vec::new()
        } else {
            raw[1..]
                .split('/')
                .map(decode_pointer_token)
                .collect::<Result<Vec<_>, _>>()?
        };
        if tokens.len() > MAX_POINTER_TOKENS || tokens.iter().any(|token| token == "-") {
            return Err(DefinitionError::InvalidPointer(raw));
        }

        Ok(Self { raw, tokens })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    fn resolve<'a>(&self, root: &'a Value, budget: &mut FieldBudget) -> Resolution<'a> {
        let mut current = root;
        for token in &self.tokens {
            if !budget.visit() {
                return Resolution::LimitExceeded;
            }
            current = match current {
                Value::Object(values) => match values.get(token) {
                    Some(value) => value,
                    None => return Resolution::Missing,
                },
                Value::Array(values) => {
                    if token.is_empty()
                        || !token.bytes().all(|byte| byte.is_ascii_digit())
                        || (token.len() > 1 && token.starts_with('0'))
                    {
                        return Resolution::Missing;
                    }
                    let Ok(index) = token.parse::<usize>() else {
                        return Resolution::Missing;
                    };
                    match values.get(index) {
                        Some(value) => value,
                        None => return Resolution::Missing,
                    }
                }
                _ => return Resolution::Missing,
            };
        }
        Resolution::Present(current)
    }
}

fn decode_pointer_token(token: &str) -> Result<String, DefinitionError> {
    let mut decoded = String::with_capacity(token.len());
    let mut chars = token.chars();
    while let Some(character) = chars.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match chars.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            _ => return Err(DefinitionError::InvalidPointerEscape),
        }
    }
    Ok(decoded)
}

impl Serialize for JsonPointer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for JsonPointer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

impl ResultType {
    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::String => value.is_string(),
            Self::Number => value.is_number(),
            Self::Integer => value.is_number(),
            Self::Boolean => value.is_boolean(),
            Self::Object => value.is_object(),
            Self::Array => value.is_array(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    FirstNonNull { paths: Vec<JsonPointer> },
    Sum { paths: Vec<JsonPointer> },
    Average { paths: Vec<JsonPointer> },
    Min { paths: Vec<JsonPointer> },
    Max { paths: Vec<JsonPointer> },
    AllPresent { paths: Vec<JsonPointer> },
    AnyPresent { paths: Vec<JsonPointer> },
    CountPresent { paths: Vec<JsonPointer> },
    AllPresentAndEqual { paths: Vec<JsonPointer> },
}

impl Operation {
    pub fn paths(&self) -> &[JsonPointer] {
        match self {
            Self::FirstNonNull { paths }
            | Self::Sum { paths }
            | Self::Average { paths }
            | Self::Min { paths }
            | Self::Max { paths }
            | Self::AllPresent { paths }
            | Self::AnyPresent { paths }
            | Self::CountPresent { paths }
            | Self::AllPresentAndEqual { paths } => paths,
        }
    }

    fn validate(&self, result_type: ResultType) -> Result<(), DefinitionError> {
        let paths = self.paths();
        let minimum = if matches!(self, Self::AllPresentAndEqual { .. }) {
            2
        } else {
            1
        };
        if paths.len() < minimum || paths.len() > MAX_PATHS_PER_DEFINITION {
            return Err(DefinitionError::InvalidArity {
                minimum,
                maximum: MAX_PATHS_PER_DEFINITION,
            });
        }
        let mut unique = HashSet::with_capacity(paths.len());
        if paths.iter().any(|path| !unique.insert(path.as_str())) {
            return Err(DefinitionError::DuplicatePointer);
        }

        let compatible = match self {
            Self::FirstNonNull { .. } => true,
            Self::Sum { .. } | Self::Average { .. } | Self::Min { .. } | Self::Max { .. } => {
                matches!(result_type, ResultType::Number | ResultType::Integer)
            }
            Self::CountPresent { .. } => matches!(result_type, ResultType::Integer),
            Self::AllPresent { .. } | Self::AnyPresent { .. } | Self::AllPresentAndEqual { .. } => {
                matches!(result_type, ResultType::Boolean)
            }
        };
        if !compatible {
            return Err(DefinitionError::IncompatibleResultType);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    key: FieldKey,
    label: String,
    #[serde(default)]
    description: String,
    operation: Operation,
    result_type: ResultType,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default = "default_semantics_version")]
    semantics_version: i16,
}

const fn default_enabled() -> bool {
    true
}

const fn default_semantics_version() -> i16 {
    SEMANTICS_VERSION
}

impl Definition {
    pub fn new(
        key: FieldKey,
        label: impl Into<String>,
        description: impl Into<String>,
        operation: Operation,
        result_type: ResultType,
        enabled: bool,
    ) -> Result<Self, DefinitionError> {
        let definition = Self {
            key,
            label: label.into(),
            description: description.into(),
            operation,
            result_type,
            enabled,
            semantics_version: SEMANTICS_VERSION,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> Result<(), DefinitionError> {
        if self.label.is_empty() || self.label.len() > MAX_LABEL_BYTES {
            return Err(DefinitionError::InvalidLabel);
        }
        if self.description.len() > MAX_DESCRIPTION_BYTES {
            return Err(DefinitionError::InvalidDescription);
        }
        if self.semantics_version != SEMANTICS_VERSION {
            return Err(DefinitionError::UnsupportedSemanticsVersion(
                self.semantics_version,
            ));
        }
        self.operation.validate(self.result_type)
    }

    pub fn key(&self) -> &FieldKey {
        &self.key
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    pub const fn result_type(&self) -> ResultType {
        self.result_type
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub const fn semantics_version(&self) -> i16 {
        self.semantics_version
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DefinitionError {
    #[error("key must match [a-z][a-z0-9_]{{0,63}}")]
    InvalidKey,
    #[error("label must contain between 1 and {MAX_LABEL_BYTES} bytes")]
    InvalidLabel,
    #[error("description must contain at most {MAX_DESCRIPTION_BYTES} bytes")]
    InvalidDescription,
    #[error("invalid JSON Pointer: {0}")]
    InvalidPointer(String),
    #[error("JSON Pointer contains an invalid '~' escape")]
    InvalidPointerEscape,
    #[error("operation contains the same JSON Pointer more than once")]
    DuplicatePointer,
    #[error("operation requires between {minimum} and {maximum} paths")]
    InvalidArity { minimum: usize, maximum: usize },
    #[error("result type is incompatible with the selected operation")]
    IncompatibleResultType,
    #[error("unsupported semantics version {0}")]
    UnsupportedSemanticsVersion(i16),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldError {
    pub code: FieldErrorCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub message: String,
}

impl FieldError {
    fn new(code: FieldErrorCode, path: Option<&JsonPointer>, message: &'static str) -> Self {
        Self {
            code,
            path: path.map(|pointer| pointer.as_str().to_owned()),
            message: message.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldErrorCode {
    InputTooLarge,
    NonNumericOperand,
    NonIntegerResult,
    ResultTypeMismatch,
    NumericOutOfRange,
    EvaluationLimitExceeded,
    ResultTooLarge,
}

impl FieldErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InputTooLarge => "input_too_large",
            Self::NonNumericOperand => "non_numeric_operand",
            Self::NonIntegerResult => "non_integer_result",
            Self::ResultTypeMismatch => "result_type_mismatch",
            Self::NumericOutOfRange => "numeric_out_of_range",
            Self::EvaluationLimitExceeded => "evaluation_limit_exceeded",
            Self::ResultTooLarge => "result_too_large",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub values: BTreeMap<String, Value>,
    pub errors: BTreeMap<String, FieldError>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EvaluationError {
    #[error("at most {maximum} definitions may be evaluated in this scope")]
    TooManyDefinitions { maximum: usize },
}

/// Evaluates enabled definitions independently. Expected data errors are isolated per field.
pub fn evaluate(
    data: &Value,
    definitions: &[Definition],
    maximum_definitions: usize,
    limits: EvaluationLimits,
) -> Result<EvaluationResult, EvaluationError> {
    if definitions.len() > maximum_definitions {
        return Err(EvaluationError::TooManyDefinitions {
            maximum: maximum_definitions,
        });
    }

    let enabled_keys = definitions
        .iter()
        .filter(|definition| definition.enabled())
        .map(|definition| definition.key().as_str().to_owned())
        .collect::<Vec<_>>();
    if serde_json::to_vec(data).map_or(usize::MAX, |bytes| bytes.len()) > limits.max_input_bytes {
        return Ok(limit_result(
            enabled_keys,
            FieldErrorCode::InputTooLarge,
            "Computed field input exceeds the size limit",
        ));
    }
    let mut result = EvaluationResult::default();
    let mut work_units = 0usize;
    for definition in definitions.iter().filter(|definition| definition.enabled()) {
        let key = definition.key().as_str().to_owned();
        let mut budget = FieldBudget {
            visited_nodes: 0,
            max_nodes: limits.max_nodes_per_field,
            scope_work_units: &mut work_units,
            max_scope_work_units: limits.max_work_units,
        };
        let evaluated = evaluate_one(data, definition, &mut budget);
        let (value, error) = match evaluated {
            Ok(value) => (value, None),
            Err(error) => (Value::Null, Some(error)),
        };

        let value_bytes = serde_json::to_vec(&value).map_or(usize::MAX, |bytes| bytes.len());
        if value_bytes > limits.max_field_output_bytes {
            let size_error = FieldError::new(
                FieldErrorCode::ResultTooLarge,
                None,
                "Computed field result exceeds the size limit",
            );
            result.values.insert(key.clone(), Value::Null);
            result.errors.insert(key, size_error);
        } else {
            result.values.insert(key.clone(), value);
            if let Some(error) = error {
                result.errors.insert(key, error);
            }
        }
    }
    if serde_json::to_vec(&result).map_or(usize::MAX, |bytes| bytes.len())
        > limits.max_scope_output_bytes
    {
        return Ok(limit_result(
            enabled_keys,
            FieldErrorCode::ResultTooLarge,
            "Computed scope result exceeds the size limit",
        ));
    }
    Ok(result)
}

fn limit_result(
    keys: Vec<String>,
    code: FieldErrorCode,
    message: &'static str,
) -> EvaluationResult {
    let mut result = EvaluationResult::default();
    for key in keys {
        result.values.insert(key.clone(), Value::Null);
        result
            .errors
            .insert(key, FieldError::new(code, None, message));
    }
    result
}

fn evaluate_one(
    data: &Value,
    definition: &Definition,
    budget: &mut FieldBudget<'_>,
) -> Result<Value, FieldError> {
    let value = match definition.operation() {
        Operation::FirstNonNull { paths } => first_non_null(data, paths, budget)?,
        Operation::Sum { paths } => numeric_aggregate(data, paths, budget, NumericOperation::Sum)?,
        Operation::Average { paths } => {
            numeric_aggregate(data, paths, budget, NumericOperation::Average)?
        }
        Operation::Min { paths } => numeric_aggregate(data, paths, budget, NumericOperation::Min)?,
        Operation::Max { paths } => numeric_aggregate(data, paths, budget, NumericOperation::Max)?,
        Operation::AllPresent { paths } => Value::Bool(all_present(data, paths, budget)?),
        Operation::AnyPresent { paths } => Value::Bool(any_present(data, paths, budget)?),
        Operation::CountPresent { paths } => {
            Value::Number(Number::from(count_present(data, paths, budget)? as u64))
        }
        Operation::AllPresentAndEqual { paths } => {
            Value::Bool(all_present_and_equal(data, paths, budget)?)
        }
    };

    if !value.is_null() && !definition.result_type().accepts(&value) {
        let code = if definition.result_type() == ResultType::Integer && value.is_number() {
            FieldErrorCode::NonIntegerResult
        } else {
            FieldErrorCode::ResultTypeMismatch
        };
        return Err(FieldError::new(
            code,
            None,
            "Computed value does not match the definition result type",
        ));
    }
    if let Value::Number(number) = &value {
        let decimal = parse_decimal(number, None)?;
        if definition.result_type() == ResultType::Integer && !decimal_is_integer(&decimal) {
            return Err(FieldError::new(
                FieldErrorCode::NonIntegerResult,
                None,
                "Computed value is not an integer",
            ));
        }
    }
    Ok(value)
}

fn first_non_null(
    data: &Value,
    paths: &[JsonPointer],
    budget: &mut FieldBudget<'_>,
) -> Result<Value, FieldError> {
    for path in paths {
        match path.resolve(data, budget) {
            Resolution::Present(value) if !value.is_null() => return Ok(value.clone()),
            Resolution::LimitExceeded => return Err(limit_error()),
            Resolution::Missing | Resolution::Present(_) => {}
        }
    }
    Ok(Value::Null)
}

fn is_present(
    data: &Value,
    path: &JsonPointer,
    budget: &mut FieldBudget<'_>,
) -> Result<bool, FieldError> {
    match path.resolve(data, budget) {
        Resolution::Present(value) => Ok(!value.is_null()),
        Resolution::Missing => Ok(false),
        Resolution::LimitExceeded => Err(limit_error()),
    }
}

fn all_present(
    data: &Value,
    paths: &[JsonPointer],
    budget: &mut FieldBudget<'_>,
) -> Result<bool, FieldError> {
    for path in paths {
        if !is_present(data, path, budget)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn any_present(
    data: &Value,
    paths: &[JsonPointer],
    budget: &mut FieldBudget<'_>,
) -> Result<bool, FieldError> {
    for path in paths {
        if is_present(data, path, budget)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn count_present(
    data: &Value,
    paths: &[JsonPointer],
    budget: &mut FieldBudget<'_>,
) -> Result<usize, FieldError> {
    let mut count = 0;
    for path in paths {
        if is_present(data, path, budget)? {
            count += 1;
        }
    }
    Ok(count)
}

fn all_present_and_equal(
    data: &Value,
    paths: &[JsonPointer],
    budget: &mut FieldBudget<'_>,
) -> Result<bool, FieldError> {
    let mut first = None;
    for path in paths {
        let value = match path.resolve(data, budget) {
            Resolution::Present(value) if !value.is_null() => value,
            Resolution::LimitExceeded => return Err(limit_error()),
            Resolution::Missing | Resolution::Present(_) => return Ok(false),
        };
        if let Some(first) = first {
            if !json_values_equal(first, value, budget)? {
                return Ok(false);
            }
        } else {
            first = Some(value);
        }
    }
    Ok(true)
}

fn json_values_equal(
    left: &Value,
    right: &Value,
    budget: &mut FieldBudget<'_>,
) -> Result<bool, FieldError> {
    if !budget.visit() {
        return Err(limit_error());
    }
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => {
            let left = parse_decimal(left, None)?;
            let right = parse_decimal(right, None)?;
            Ok(left == right)
        }
        (Value::Array(left), Value::Array(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !json_values_equal(left, right, budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Value::Object(left), Value::Object(right)) => objects_equal(left, right, budget),
        _ => Ok(left == right),
    }
}

fn objects_equal(
    left: &Map<String, Value>,
    right: &Map<String, Value>,
    budget: &mut FieldBudget<'_>,
) -> Result<bool, FieldError> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (key, left) in left {
        let Some(right) = right.get(key) else {
            return Ok(false);
        };
        if !json_values_equal(left, right, budget)? {
            return Ok(false);
        }
    }
    Ok(true)
}

enum NumericOperation {
    Sum,
    Average,
    Min,
    Max,
}

fn numeric_aggregate(
    data: &Value,
    paths: &[JsonPointer],
    budget: &mut FieldBudget<'_>,
    operation: NumericOperation,
) -> Result<Value, FieldError> {
    let mut values = Vec::with_capacity(paths.len());
    for path in paths {
        match path.resolve(data, budget) {
            Resolution::Present(Value::Number(number)) => {
                values.push(parse_decimal(number, Some(path))?)
            }
            Resolution::Present(Value::Null) | Resolution::Missing => {}
            Resolution::Present(_) => {
                return Err(FieldError::new(
                    FieldErrorCode::NonNumericOperand,
                    Some(path),
                    "Expected a number",
                ));
            }
            Resolution::LimitExceeded => return Err(limit_error()),
        }
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }

    let value = match operation {
        NumericOperation::Sum => values.into_iter().sum(),
        NumericOperation::Average => {
            let count = BigDecimal::from(values.len() as u64);
            let sum: BigDecimal = values.into_iter().sum();
            sum / count
        }
        NumericOperation::Min => values.into_iter().min().expect("non-empty values"),
        NumericOperation::Max => values.into_iter().max().expect("non-empty values"),
    };
    decimal_to_json(value)
}

fn parse_decimal(number: &Number, path: Option<&JsonPointer>) -> Result<BigDecimal, FieldError> {
    let source = number.to_string();
    if source.len() > MAX_DECIMAL_SOURCE_BYTES {
        return Err(FieldError::new(
            FieldErrorCode::NumericOutOfRange,
            path,
            "Number is outside the supported decimal range",
        ));
    }
    let value = BigDecimal::from_str(&source).map_err(|_| {
        FieldError::new(
            FieldErrorCode::NumericOutOfRange,
            path,
            "Number is outside the supported decimal range",
        )
    })?;
    validate_decimal(&value, path)?;
    Ok(value)
}

fn decimal_to_json(value: BigDecimal) -> Result<Value, FieldError> {
    let value = round_significant(value, MAX_DECIMAL_SIGNIFICANT_DIGITS as u64);
    validate_decimal(&value, None)?;
    let normalized = value.normalized().to_string();
    let number = Number::from_str(&normalized).map_err(|_| {
        FieldError::new(
            FieldErrorCode::NumericOutOfRange,
            None,
            "Number cannot be represented as JSON",
        )
    })?;
    Ok(Value::Number(number))
}

fn decimal_is_integer(value: &BigDecimal) -> bool {
    let normalized = value.normalized();
    let (_, scale) = normalized.as_bigint_and_exponent();
    scale <= 0
}

fn validate_decimal(value: &BigDecimal, path: Option<&JsonPointer>) -> Result<(), FieldError> {
    let (digits, exponent) = decimal_shape(value);
    if digits > MAX_DECIMAL_SIGNIFICANT_DIGITS
        || !(MIN_DECIMAL_EXPONENT..=MAX_DECIMAL_EXPONENT).contains(&exponent)
    {
        return Err(FieldError::new(
            FieldErrorCode::NumericOutOfRange,
            path,
            "Number is outside the supported decimal range",
        ));
    }
    Ok(())
}

fn decimal_shape(value: &BigDecimal) -> (usize, i64) {
    let normalized = value.normalized();
    let (integer, scale) = normalized.as_bigint_and_exponent();
    let digits = integer.to_string().trim_start_matches('-').len();
    let exponent = if integer == 0.into() {
        0
    } else {
        digits as i64 - scale - 1
    };
    (digits, exponent)
}

fn round_significant(value: BigDecimal, digits: u64) -> BigDecimal {
    if value == 0 {
        return value;
    }
    let (current_digits, exponent) = decimal_shape(&value);
    if current_digits as u64 <= digits {
        return value;
    }
    let scale = digits as i64 - exponent - 1;
    value.with_scale_round(scale, RoundingMode::HalfEven)
}

fn limit_error() -> FieldError {
    FieldError::new(
        FieldErrorCode::EvaluationLimitExceeded,
        None,
        "Computed field evaluation exceeded its work limit",
    )
}

enum Resolution<'a> {
    Missing,
    Present(&'a Value),
    LimitExceeded,
}

struct FieldBudget<'a> {
    visited_nodes: usize,
    max_nodes: usize,
    scope_work_units: &'a mut usize,
    max_scope_work_units: usize,
}

impl FieldBudget<'_> {
    fn visit(&mut self) -> bool {
        self.visited_nodes = self.visited_nodes.saturating_add(1);
        *self.scope_work_units = self.scope_work_units.saturating_add(1);
        self.visited_nodes <= self.max_nodes && *self.scope_work_units <= self.max_scope_work_units
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    fn pointer(value: &str) -> JsonPointer {
        JsonPointer::new(value).unwrap()
    }

    fn definition(operation: Operation, result_type: ResultType) -> Definition {
        Definition::new(
            FieldKey::new("result").unwrap(),
            "Result",
            "",
            operation,
            result_type,
            true,
        )
        .unwrap()
    }

    fn evaluate_definition(data: Value, definition: Definition) -> EvaluationResult {
        evaluate(
            &data,
            &[definition],
            MAX_SHARED_DEFINITIONS,
            EvaluationLimits::standard(),
        )
        .unwrap()
    }

    #[rstest]
    #[case(json!({}), Value::Null)]
    #[case(json!({"a": null, "b": "fallback"}), json!("fallback"))]
    #[case(json!({"a": false, "b": true}), json!(false))]
    #[case(json!({"a": 0, "b": 2}), json!(0))]
    #[case(json!({"a": "", "b": "fallback"}), json!(""))]
    #[case(json!({"a": [], "b": [1]}), json!([]))]
    fn first_non_null_semantics(#[case] data: Value, #[case] expected: Value) {
        let result = evaluate_definition(
            data,
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/a"), pointer("/b")],
                },
                match expected {
                    Value::Bool(_) => ResultType::Boolean,
                    Value::Number(_) => ResultType::Integer,
                    Value::Array(_) => ResultType::Array,
                    _ => ResultType::String,
                },
            ),
        );
        assert_eq!(result.values["result"], expected);
    }

    #[rstest]
    #[case(Operation::Sum { paths: vec![pointer("/a"), pointer("/b"), pointer("/missing")] }, json!(6))]
    #[case(Operation::Average { paths: vec![pointer("/a"), pointer("/b")] }, json!(3))]
    #[case(Operation::Min { paths: vec![pointer("/a"), pointer("/b")] }, json!(2))]
    #[case(Operation::Max { paths: vec![pointer("/a"), pointer("/b")] }, json!(4))]
    fn numeric_operations(#[case] operation: Operation, #[case] expected: Value) {
        let result = evaluate_definition(
            json!({"a": 2, "b": 4}),
            definition(operation, ResultType::Number),
        );
        assert_eq!(result.values["result"], expected);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn numeric_aggregate_ignores_missing_and_null_but_not_wrong_types() {
        let operation = Operation::Sum {
            paths: vec![pointer("/a"), pointer("/b"), pointer("/c")],
        };
        let result = evaluate_definition(
            json!({"a": null, "c": "2"}),
            definition(operation, ResultType::Number),
        );
        assert_eq!(result.values["result"], Value::Null);
        assert_eq!(
            result.errors["result"].code,
            FieldErrorCode::NonNumericOperand
        );
        assert_eq!(result.errors["result"].path.as_deref(), Some("/c"));
    }

    #[rstest]
    #[case(Operation::AllPresent { paths: vec![pointer("/a"), pointer("/b")] }, json!(false))]
    #[case(Operation::AnyPresent { paths: vec![pointer("/a"), pointer("/b")] }, json!(true))]
    #[case(Operation::CountPresent { paths: vec![pointer("/a"), pointer("/b")] }, json!(1))]
    fn presence_operations(#[case] operation: Operation, #[case] expected: Value) {
        let result_type = if matches!(operation, Operation::CountPresent { .. }) {
            ResultType::Integer
        } else {
            ResultType::Boolean
        };
        let result = evaluate_definition(
            json!({"a": false, "b": null}),
            definition(operation, result_type),
        );
        assert_eq!(result.values["result"], expected);
    }

    #[rstest]
    #[case(json!({"a": 1, "b": 1.0}), true)]
    #[case(json!({"a": "42", "b": 42}), false)]
    #[case(json!({"a": {"x": 1, "y": 2}, "b": {"y": 2, "x": 1}}), true)]
    #[case(json!({"a": [1, 2], "b": [2, 1]}), false)]
    #[case(json!({"a": null, "b": null}), false)]
    #[case(json!({"a": false}), false)]
    fn strict_equality(#[case] data: Value, #[case] expected: bool) {
        let result = evaluate_definition(
            data,
            definition(
                Operation::AllPresentAndEqual {
                    paths: vec![pointer("/a"), pointer("/b")],
                },
                ResultType::Boolean,
            ),
        );
        assert_eq!(result.values["result"], json!(expected));
    }

    #[test]
    fn pointer_escaping_and_array_indexes_work() {
        let result = evaluate_definition(
            json!({"a/b": {"~name": ["yes"]}}),
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/a~1b/~0name/0")],
                },
                ResultType::String,
            ),
        );
        assert_eq!(result.values["result"], json!("yes"));
    }

    #[test]
    fn empty_pointer_resolves_the_document_root() {
        let result = evaluate_definition(
            json!({"answer": 42}),
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("")],
                },
                ResultType::Object,
            ),
        );
        assert_eq!(result.values["result"], json!({"answer": 42}));
    }

    #[test]
    fn array_indexes_with_leading_zero_do_not_resolve() {
        let result = evaluate_definition(
            json!(["zero", "one"]),
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/01")],
                },
                ResultType::String,
            ),
        );
        assert_eq!(result.values["result"], Value::Null);
    }

    #[test]
    fn array_indexes_require_canonical_ascii_digits() {
        let result = evaluate_definition(
            json!(["zero", "one"]),
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/+1")],
                },
                ResultType::String,
            ),
        );
        assert_eq!(result.values["result"], Value::Null);
    }

    #[rstest]
    #[case("root")]
    #[case("/~2")]
    #[case("/items/-")]
    fn invalid_pointers_are_rejected(#[case] value: &str) {
        assert!(JsonPointer::new(value).is_err());
    }

    #[test]
    fn operation_rejects_duplicate_paths() {
        let result = Definition::new(
            FieldKey::new("duplicate").unwrap(),
            "Duplicate",
            "",
            Operation::Sum {
                paths: vec![pointer("/a"), pointer("/a")],
            },
            ResultType::Number,
            true,
        );
        assert_eq!(result.unwrap_err(), DefinitionError::DuplicatePointer);
    }

    #[test]
    fn average_uses_half_even_rounding_at_34_significant_digits() {
        let data: Value =
            serde_json::from_str(r#"{"a":1,"b":0.00000000000000000000000000000000005}"#).unwrap();
        let result = evaluate_definition(
            data,
            definition(
                Operation::Average {
                    paths: vec![pointer("/a"), pointer("/b")],
                },
                ResultType::Number,
            ),
        );
        assert_eq!(result.values["result"].to_string(), "0.5");
    }

    #[test]
    fn result_type_mismatch_is_isolated() {
        let result = evaluate_definition(
            json!({"a": "text"}),
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/a")],
                },
                ResultType::Boolean,
            ),
        );
        assert_eq!(result.values["result"], Value::Null);
        assert_eq!(
            result.errors["result"].code,
            FieldErrorCode::ResultTypeMismatch
        );
    }

    #[test]
    fn traversal_limit_isolated_per_field() {
        let result = evaluate(
            &json!({"a": {"b": 1}}),
            &[definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/a/b")],
                },
                ResultType::Integer,
            )],
            1,
            EvaluationLimits::for_tests(1024, 1, 10, 1024, 1024),
        )
        .unwrap();
        assert_eq!(
            result.errors["result"].code,
            FieldErrorCode::EvaluationLimitExceeded
        );
    }

    #[test]
    fn recursive_equality_obeys_the_traversal_limit() {
        let result = evaluate(
            &json!({"a": [1, 2], "b": [1, 2]}),
            &[definition(
                Operation::AllPresentAndEqual {
                    paths: vec![pointer("/a"), pointer("/b")],
                },
                ResultType::Boolean,
            )],
            1,
            EvaluationLimits::for_tests(1024, 3, 10, 1024, 1024),
        )
        .unwrap();
        assert_eq!(
            result.errors["result"].code,
            FieldErrorCode::EvaluationLimitExceeded
        );
    }

    #[test]
    fn presence_operations_propagate_the_traversal_limit() {
        let result = evaluate(
            &json!({"a": {"b": true}}),
            &[definition(
                Operation::AllPresent {
                    paths: vec![pointer("/a/b")],
                },
                ResultType::Boolean,
            )],
            1,
            EvaluationLimits::for_tests(1024, 1, 10, 1024, 1024),
        )
        .unwrap();
        assert_eq!(
            result.errors["result"].code,
            FieldErrorCode::EvaluationLimitExceeded
        );
    }

    #[test]
    fn oversized_input_isolated_for_every_enabled_field() {
        let result = evaluate(
            &json!({"large": "value"}),
            &[definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/large")],
                },
                ResultType::String,
            )],
            1,
            EvaluationLimits::for_tests(4, 10, 10, 1024, 1024),
        )
        .unwrap();
        assert_eq!(result.values["result"], Value::Null);
        assert_eq!(result.errors["result"].code, FieldErrorCode::InputTooLarge);
    }

    #[test]
    fn scope_output_limit_includes_result_map_structure() {
        let result = evaluate(
            &json!({"value": 1}),
            &[definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/value")],
                },
                ResultType::Integer,
            )],
            1,
            EvaluationLimits::for_tests(1024, 10, 10, 1024, 1),
        )
        .unwrap();
        assert_eq!(result.errors["result"].code, FieldErrorCode::ResultTooLarge);
    }

    #[rstest]
    #[case(
        r#"{"value":0.1234567890123456789012345678901234}"#,
        ResultType::Number,
        "0.1234567890123456789012345678901234"
    )]
    #[case(
        r#"{"value":1234567890123456789012345678901234}"#,
        ResultType::Integer,
        "1234567890123456789012345678901234"
    )]
    fn json_numbers_retain_34_digit_precision(
        #[case] source: &str,
        #[case] result_type: ResultType,
        #[case] expected: &str,
    ) {
        let data = serde_json::from_str(source).unwrap();
        let result = evaluate_definition(
            data,
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/value")],
                },
                result_type,
            ),
        );
        assert_eq!(result.values["result"].to_string(), expected);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn integer_result_type_accepts_the_u64_boundary() {
        let data = json!({"value": u64::MAX});
        let result = evaluate_definition(
            data,
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/value")],
                },
                ResultType::Integer,
            ),
        );
        assert_eq!(result.values["result"], json!(u64::MAX));
        assert!(result.errors.is_empty());
    }

    #[test]
    fn oversized_numeric_source_is_rejected_before_decimal_parsing() {
        let source = format!(
            r#"{{"value":{}}}"#,
            "1".repeat(MAX_DECIMAL_SOURCE_BYTES + 1)
        );
        let data = serde_json::from_str(&source).unwrap();

        let result = evaluate_definition(
            data,
            definition(
                Operation::FirstNonNull {
                    paths: vec![pointer("/value")],
                },
                ResultType::Integer,
            ),
        );

        assert_eq!(
            result.errors["result"].code,
            FieldErrorCode::NumericOutOfRange
        );
    }
}
