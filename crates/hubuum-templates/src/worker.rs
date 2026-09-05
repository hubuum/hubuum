//! Worker-side execution. Called only by the dedicated bounded-allocator binary.
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;

use minijinja::{AutoEscape, Environment, UndefinedBehavior, escape_formatter};

use crate::isolation::{MissingDataPolicy, Operation, RenderedTemplate, WorkerRequest};
use crate::{
    MissingValue, SizeLimitedWriter, TemplateAutoEscape, json_value_to_template_value,
    register_curated_helpers,
};

thread_local! {
    static MISSING: RefCell<Vec<MissingValue>> = const { RefCell::new(Vec::new()) };
}
fn record(missing: MissingValue) {
    MISSING.with(|values| {
        let mut values = values.borrow_mut();
        if values.len() < 1024 && !values.contains(&missing) {
            values.push(missing);
        }
    });
}

/// Worker protocol entry point. Embedders must invoke the executable, whose
/// allocator and process lifetime enforce the memory and time boundaries.
pub fn serve_template_worker(heap_peak: fn() -> usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err("template request too large".into());
    }
    let request: WorkerRequest = serde_json::from_slice(&bytes)?;
    let result = render(request)
        .map(|mut output| {
            output.set_peak_heap_bytes(heap_peak());
            output
        })
        .map_err(|error| error.to_string());
    serde_json::to_writer(std::io::stdout().lock(), &result)?;
    Ok(())
}

fn render(request: WorkerRequest<'_>) -> Result<RenderedTemplate, minijinja::Error> {
    let mut environment = Environment::new();
    environment.set_keep_trailing_newline(request.keep_trailing_newline);
    environment.set_recursion_limit(request.limits.recursion_limit());
    environment.set_fuel(Some(request.limits.fuel()));
    environment.set_undefined_behavior(match request.missing_data {
        MissingDataPolicy::Lenient => UndefinedBehavior::Lenient,
        MissingDataPolicy::Strict => UndefinedBehavior::Strict,
        MissingDataPolicy::Omit | MissingDataPolicy::Null => UndefinedBehavior::Chainable,
    });
    environment.set_auto_escape_callback(move |_| match request.auto_escape {
        TemplateAutoEscape::None => AutoEscape::None,
        TemplateAutoEscape::Html => AutoEscape::Html,
    });
    environment.set_formatter(move |out, state, value| {
        if matches!(
            request.missing_data,
            MissingDataPolicy::Omit | MissingDataPolicy::Null
        ) && (value.is_undefined() || value.is_none())
        {
            if value.is_undefined() {
                record(MissingValue::new(state.name(), None));
            }
            if matches!(request.missing_data, MissingDataPolicy::Null) {
                out.write_str("null")?;
            }
            Ok(())
        } else {
            escape_formatter(out, state, value)
        }
    });
    let mut sources: HashMap<_, _> = request.sources.into_owned().into_iter().collect();
    sources.insert(request.name.to_string(), request.source.to_string());
    environment.set_loader(move |name| {
        if name.contains('/') || name.contains("::") {
            return Ok(None);
        }
        Ok(sources.get(name).cloned())
    });
    register_curated_helpers(&mut environment, Some(record));
    environment.add_template_owned(request.name.to_string(), request.source.into_owned())?;
    if matches!(request.operation, Operation::Syntax) {
        return Ok(RenderedTemplate::new(String::new(), vec![]));
    }
    let mut writer = SizeLimitedWriter::new(request.max_output_bytes);
    let rendered = environment
        .get_template(&request.name)?
        .render_captured_to(json_value_to_template_value(&request.context), &mut writer);
    if writer.exceeded() {
        return Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "template output limit exceeded",
        ));
    }
    rendered?;
    let output = writer.into_string().map_err(|_| {
        minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "invalid template output",
        )
    })?;
    Ok(RenderedTemplate::new(
        output,
        MISSING.with(|values| values.take()),
    ))
}
