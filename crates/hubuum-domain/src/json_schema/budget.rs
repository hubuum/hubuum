//! Conservative admission limits, checked before the JSON Schema engine runs.
//!
//! References are a DAG: each use charges the full referenced subtree, even
//! when the same definition was already examined. This bounds evaluation, not
//! just the input size or the number of distinct definitions.

use std::collections::HashSet;

use serde_json::Value;

use super::JsonSchemaError;

const MAX_DEPTH: usize = 64;
const MAX_SCHEMA_NODES: usize = 4_096;
const MAX_SCHEMA_BYTES: usize = 65_536;
const MAX_INSTANCE_NODES: usize = 16_384;
const MAX_INSTANCE_BYTES: usize = 1_048_576;
const MAX_EXPANDED_WORK: usize = 16_384;
const MAX_INSTANCE_WORK: usize = 16_777_216;

#[derive(Clone, Copy)]
enum Document {
    Schema,
    Instance,
}

impl Document {
    fn error(self, message: impl Into<String>) -> JsonSchemaError {
        match self {
            Self::Schema => JsonSchemaError::invalid_schema(message),
            Self::Instance => JsonSchemaError::invalid_value(message),
        }
    }
}

struct JsonSize {
    nodes: usize,
    bytes: usize,
}

impl JsonSize {
    fn measure(value: &Value, document: Document) -> Result<Self, JsonSchemaError> {
        let (max_nodes, max_bytes) = match document {
            Document::Schema => (MAX_SCHEMA_NODES, MAX_SCHEMA_BYTES),
            Document::Instance => (MAX_INSTANCE_NODES, MAX_INSTANCE_BYTES),
        };
        let mut size = Self { nodes: 0, bytes: 0 };
        let mut pending = vec![(value, 0)];
        while let Some((value, depth)) = pending.pop() {
            size.nodes += 1;
            // Include punctuation and escaping conservatively, without first
            // allocating an unbounded serialized copy of the document.
            size.bytes = size.bytes.saturating_add(8);
            match value {
                Value::String(text) => {
                    size.bytes = size.bytes.saturating_add(text.len().saturating_mul(6))
                }
                Value::Number(number) => {
                    let text = number.to_string();
                    let exponent = text.split_once(['e', 'E']).map(|(_, exponent)| exponent);
                    if text.len() > 128
                        || exponent.is_some_and(|exponent| {
                            exponent
                                .parse::<i32>()
                                .map_or(true, |n| !(-308..=308).contains(&n))
                        })
                    {
                        return Err(document.error("JSON Schema validation supports numbers with at most 128 characters and exponents from -308 to 308; reduce numeric precision or exponent"));
                    }
                    size.bytes = size.bytes.saturating_add(text.len());
                }
                Value::Array(values) => {
                    if values.len() > max_nodes.saturating_sub(size.nodes) {
                        return Err(document.error(format!("JSON Schema validation exceeds {max_nodes} JSON nodes; reduce document size")));
                    }
                    pending.extend(values.iter().map(|value| (value, depth + 1)));
                }
                Value::Object(values) => {
                    if values.len() > max_nodes.saturating_sub(size.nodes) {
                        return Err(document.error(format!("JSON Schema validation exceeds {max_nodes} JSON nodes; reduce document size")));
                    }
                    for (key, value) in values {
                        size.bytes = size.bytes.saturating_add(key.len().saturating_mul(6));
                        pending.push((value, depth + 1));
                    }
                }
                _ => {}
            }
            if depth > MAX_DEPTH || size.nodes > max_nodes || size.bytes > max_bytes {
                return Err(document.error(format!("JSON Schema validation exceeds document limits ({max_nodes} nodes, {max_bytes} estimated encoded bytes, depth {MAX_DEPTH}); reduce document size or nesting")));
            }
        }
        Ok(size)
    }
}

pub(super) fn validate_document_size(schema: &Value) -> Result<(), JsonSchemaError> {
    JsonSize::measure(schema, Document::Schema).map(|_| ())
}

/// Private proof of a finite, supported schema evaluation graph.
pub(super) struct SchemaBudget {
    expanded_work: usize,
    compares_array_pairs: bool,
}

impl SchemaBudget {
    pub(super) fn new(schema: &Value) -> Result<Self, JsonSchemaError> {
        validate_document_size(schema)?;
        let mut inspector = Inspector {
            root: schema,
            active: HashSet::new(),
            compares_array_pairs: false,
        };
        let expanded_work = inspector.schema_work(schema, 0)?;
        Ok(Self {
            expanded_work,
            compares_array_pairs: inspector.compares_array_pairs,
        })
    }

    pub(super) fn check_instance(&self, value: &Value) -> Result<(), JsonSchemaError> {
        let size = JsonSize::measure(value, Document::Instance)?;
        let mut work = self.expanded_work.saturating_mul(size.bytes);
        if self.compares_array_pairs {
            work = work.saturating_mul(size.nodes);
        }
        if work > MAX_INSTANCE_WORK {
            return Err(JsonSchemaError::invalid_value(format!(
                "JSON Schema evaluation exceeds the {MAX_INSTANCE_WORK} work budget; simplify the schema or reduce instance size (especially uniqueItems arrays)"
            )));
        }
        Ok(())
    }
}

struct Inspector<'a> {
    root: &'a Value,
    // Addresses identify borrowed nodes in this immutable document. No pointer
    // is dereferenced or retained after inspection.
    active: HashSet<*const Value>,
    compares_array_pairs: bool,
}

fn checked_work(work: usize) -> Result<usize, JsonSchemaError> {
    if work > MAX_EXPANDED_WORK {
        return Err(JsonSchemaError::invalid_schema(format!(
            "JSON Schema reference/combinator expansion exceeds {MAX_EXPANDED_WORK} work units; remove repeated references or simplify combinators"
        )));
    }
    Ok(work)
}

fn reference_target<'a>(root: &'a Value, pointer: &str) -> Result<&'a Value, JsonSchemaError> {
    let mut prefix = String::new();
    let mut target = root;
    for token in pointer
        .strip_prefix('/')
        .expect("local pointer checked")
        .split('/')
    {
        // A pointer can enter a schema through an otherwise unknown keyword.
        // Its ancestors must not change the reference base behind the cost model.
        if !std::ptr::eq(target, root)
            && target.as_object().is_some_and(|object| {
                object.get("$id").is_some_and(Value::is_string)
                    || object.get("id").is_some_and(Value::is_string)
            })
        {
            return Err(JsonSchemaError::invalid_schema(
                "JSON Schema references cannot cross nested resource IDs; keep one root resource",
            ));
        }
        prefix.push('/');
        prefix.push_str(token);
        target = root.pointer(&prefix).ok_or_else(|| {
            JsonSchemaError::invalid_schema(format!(
                "JSON Schema reference #{pointer} does not resolve within this document"
            ))
        })?;
    }
    Ok(target)
}

impl Inspector<'_> {
    fn schema_work(&mut self, schema: &Value, depth: usize) -> Result<usize, JsonSchemaError> {
        if depth > MAX_DEPTH {
            return Err(JsonSchemaError::invalid_schema(format!(
                "JSON Schema reference expansion exceeds depth {MAX_DEPTH}; shorten reference chains"
            )));
        }
        let key = std::ptr::from_ref(schema);
        if !self.active.insert(key) {
            return Err(JsonSchemaError::invalid_schema(
                "Recursive JSON Schema references are unsupported by the evaluation budget; replace recursion with bounded nesting",
            ));
        }
        let mut work = 1;
        if let Value::Object(object) = schema {
            for (keyword, value) in object {
                let additional = match keyword.as_str() {
                    "$dynamicRef"
                    | "$recursiveRef"
                    | "$dynamicAnchor"
                    | "$recursiveAnchor"
                    | "$anchor"
                    | "unevaluatedProperties"
                    | "unevaluatedItems" => {
                        return Err(JsonSchemaError::invalid_schema(format!(
                            "JSON Schema keyword {keyword} is unsupported by the evaluation budget; use explicit properties/items and acyclic local JSON Pointer references"
                        )));
                    }
                    "$id" | "id" if !std::ptr::eq(schema, self.root) => {
                        return Err(JsonSchemaError::invalid_schema(
                            "Nested JSON Schema resource IDs are unsupported by the evaluation budget; keep one root resource and use local JSON Pointer references",
                        ));
                    }
                    "$ref" => {
                        let reference = value.as_str().ok_or_else(|| {
                            JsonSchemaError::invalid_schema("JSON schema $ref must be a string")
                        })?;
                        let pointer = reference.strip_prefix('#').filter(|pointer| pointer.starts_with('/') && !pointer.contains('%')).ok_or_else(|| JsonSchemaError::invalid_schema("JSON schema $ref must be an acyclic local JSON Pointer such as #/$defs/item; anchors, root references, and percent-encoded references are unsupported"))?;
                        let target = reference_target(self.root, pointer)?;
                        self.schema_work(target, depth + 1)?
                    }
                    "$defs" | "definitions" | "properties" | "patternProperties"
                    | "dependentSchemas" | "dependencies" => {
                        let mut children = 1;
                        if let Value::Object(values) = value {
                            for (name, child) in values {
                                let child_work = if child.is_object() || child.is_boolean() {
                                    self.schema_work(child, depth + 1)?
                                } else {
                                    JsonSize::measure(child, Document::Schema)?.bytes
                                };
                                children = checked_work(children + name.len() + child_work)?;
                            }
                        }
                        children
                    }
                    "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                        let mut children = 1;
                        if let Value::Array(values) = value {
                            for child in values {
                                children =
                                    checked_work(children + self.schema_work(child, depth + 1)?)?;
                            }
                        }
                        // Failed alternatives can be revisited to build errors.
                        if matches!(keyword.as_str(), "anyOf" | "oneOf") {
                            children.saturating_mul(2)
                        } else {
                            children
                        }
                    }
                    "items" if value.is_array() => {
                        let mut children = 1;
                        for child in value.as_array().expect("array checked") {
                            children =
                                checked_work(children + self.schema_work(child, depth + 1)?)?;
                        }
                        children
                    }
                    "items"
                    | "additionalItems"
                    | "additionalProperties"
                    | "propertyNames"
                    | "not"
                    | "if"
                    | "then"
                    | "else" => self.schema_work(value, depth + 1)?,
                    "contains" => self.schema_work(value, depth + 1)?.saturating_mul(2),
                    "uniqueItems" => {
                        self.compares_array_pairs |= value == &Value::Bool(true);
                        1
                    }
                    _ => JsonSize::measure(value, Document::Schema)?.bytes,
                };
                work = checked_work(work + keyword.len() + additional)?;
            }
        } else if !schema.is_boolean() {
            return Err(JsonSchemaError::invalid_schema(
                "JSON Schema subschemas must be objects or booleans",
            ));
        }
        self.active.remove(&key);
        Ok(work)
    }
}
