//! Bounded event message rendering, shared by human-facing event sinks.
use hubuum_event_sinks_common::{EventEnvelope, SinkError};
use hubuum_templates::{
    MissingDataPolicy, TemplateBatch, TemplateExecution, TemplateLimits, prepare_template,
};
use serde_json::Value;

pub const MAX_MESSAGE_BYTES: usize = 1_048_576;

/// A named template with an explicit output bound. Sources never appear in Debug.
pub struct EventTemplate<'a> {
    name: &'a str,
    source: &'a str,
    max_bytes: usize,
}

impl<'a> EventTemplate<'a> {
    pub fn new(name: &'a str, source: &'a str, max_bytes: usize) -> Result<Self, SinkError> {
        if source.trim().is_empty()
            || source.len() > MAX_MESSAGE_BYTES
            || max_bytes == 0
            || max_bytes > MAX_MESSAGE_BYTES
        {
            return Err(SinkError::permanent(format!(
                "Invalid {name}: empty template or invalid size limit"
            )));
        }
        Ok(Self {
            name,
            source,
            max_bytes,
        })
    }

    pub async fn validate(&self) -> Result<(), SinkError> {
        prepare_template(self.source)
            .limits(TemplateLimits::new(16, 50_000))
            .validate()
            .await
            .map_err(|_| SinkError::permanent(format!("Invalid {} template syntax", self.name)))
    }
}

/// Bound the serialized envelope before adding the established `event` alias.
pub fn event_context(envelope: &EventEnvelope, max_bytes: usize) -> Result<Value, SinkError> {
    let event = serde_json::to_value(envelope)
        .map_err(|_| SinkError::permanent("Cannot serialize event template context"))?;
    if event.to_string().len() > max_bytes {
        return Err(SinkError::permanent(
            "Event envelope exceeds its size limit",
        ));
    }
    let mut root = event
        .as_object()
        .cloned()
        .ok_or_else(|| SinkError::permanent("Event context must be an object"))?;
    root.insert("event".into(), event);
    root.insert(
        "occurred_at".into(),
        Value::String(envelope.occurred_at().naive_utc().to_string()),
    );
    Ok(Value::Object(root))
}

pub async fn render(
    envelope: &EventEnvelope,
    templates: &[EventTemplate<'_>],
    max_envelope_bytes: usize,
) -> Result<Vec<String>, SinkError> {
    let context = event_context(envelope, max_envelope_bytes)?;
    let mut batch = TemplateBatch::new(templates.iter().map(|template| template.max_bytes).sum());
    for template in templates {
        batch
            .push(
                TemplateExecution::new(
                    "event-message",
                    template.source,
                    TemplateLimits::new(16, 50_000),
                )
                .keep_trailing_newline(false)
                .missing_data(MissingDataPolicy::Lenient)
                .max_output_bytes(template.max_bytes),
            )
            .map_err(|_| {
                SinkError::permanent(format!("Invalid {} template limits", template.name))
            })?;
    }
    batch
        .render(&context)
        .await
        .map(|outputs| {
            outputs
                .into_iter()
                .map(|output| output.into_parts().0)
                .collect()
        })
        .map_err(|error| {
            match error
                .template_index()
                .and_then(|index| templates.get(index))
            {
                Some(template) => SinkError::permanent(format!(
                    "Could not render {}: check template syntax, input and execution limits",
                    template.name
                )),
                None => SinkError::new(
                    "Event template execution is unavailable or exceeded its process budget",
                ),
            }
        })
}

pub fn default_text_template() -> String {
    concat!("Hubuum: {{ summary }}\n", "Entity: {{ entity_type }}{% if entity_name %} {{ entity_name }}{% elif entity_id %} {{ entity_id }}{% endif %}\n",
        "Action: {{ action }}{% if metadata.task_kind %}\nTask kind: {{ metadata.task_kind }}{% endif %}\n",
        "Occurred: {{ occurred_at }}\nEvent: {{ event_id }}").to_string()
}

pub fn rich_array(rendered: &str, max_items: usize) -> Result<Vec<Value>, SinkError> {
    let value: Value = serde_json::from_str(rendered).map_err(|_| {
        SinkError::permanent(
            "Rich message template must render valid JSON; use tojson for event values",
        )
    })?;
    match value {
        Value::Array(items) if items.len() <= max_items && items.iter().all(Value::is_object) => {
            Ok(items)
        }
        _ => Err(SinkError::permanent(
            "Rich message must be a bounded JSON array of objects",
        )),
    }
}

pub fn validate_text(text: &str, max_chars: usize) -> Result<(), SinkError> {
    if text.trim().is_empty() || text.chars().count() > max_chars {
        return Err(SinkError::permanent(format!(
            "Message text must contain 1 to {max_chars} characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("{}")]
    #[case("[1]")]
    #[case("not json")]
    #[case("[{},{}]")]
    fn rejects_invalid_rich_arrays(#[case] value: &str) {
        assert!(rich_array(value, 1).is_err());
    }

    #[test]
    fn accepts_json_escaped_event_values() {
        assert_eq!(
            rich_array(r#"[{"text":"quote: \" and newline: \n"}]"#, 1).unwrap()[0]["text"],
            "quote: \" and newline: \n"
        );
    }
}
