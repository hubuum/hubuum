//! Worker-side execution. Called only by the dedicated bounded-allocator binary.
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;

use hubuum_budget::{ByteBudget, ByteLimit};
use minijinja::syntax::SyntaxConfig;
use minijinja::value::Value;
use minijinja::{AutoEscape, Environment, UndefinedBehavior, escape_formatter};

use crate::isolation::{
    MAX_BATCH_TEMPLATES, MAX_OUTPUT_BYTES, MAX_REQUEST_BYTES, MissingDataPolicy, Operation,
    RenderedTemplate, WorkerFailure, WorkerRequest, WorkerTemplate,
};
use crate::{
    MissingValue, TemplateAutoEscape, json_value_to_template_value, register_curated_helpers,
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
        .take(MAX_REQUEST_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err("template request too large".into());
    }
    let request: WorkerRequest = serde_json::from_slice(&bytes)?;
    if request.templates.is_empty()
        || request.templates.len() > MAX_BATCH_TEMPLATES
        || request.max_output_bytes > MAX_OUTPUT_BYTES
    {
        return Err("invalid template batch limits".into());
    }
    let result = render_batch(request).map(|mut outputs| {
        let peak = heap_peak();
        for output in &mut outputs {
            output.set_peak_heap_bytes(peak);
        }
        outputs
    });
    serde_json::to_writer(std::io::stdout().lock(), &result)?;
    Ok(())
}

fn render_batch(request: WorkerRequest<'_>) -> Result<Vec<RenderedTemplate>, WorkerFailure> {
    // Convert the shared context once. MiniJinja values share immutable context
    // storage; each render still receives a separate environment and state.
    let context = json_value_to_template_value(&request.context);
    let mut output_budget = ByteLimit::new(request.max_output_bytes).start();
    let mut remaining_warnings = 1024;
    let mut outputs = Vec::with_capacity(request.templates.len());
    for (template_index, template) in request.templates.into_iter().enumerate() {
        let mut output = render(template, &request.operation, &context, &mut output_budget)
            .map_err(|error| WorkerFailure {
                template_index,
                message: error.to_string(),
            })?;
        output.truncate_missing_values(remaining_warnings);
        remaining_warnings -= output.missing_value_count();
        outputs.push(output);
    }
    Ok(outputs)
}

fn render(
    request: WorkerTemplate<'_>,
    operation: &Operation,
    context: &Value,
    output_budget: &mut ByteBudget,
) -> Result<RenderedTemplate, minijinja::Error> {
    MISSING.with(|values| values.borrow_mut().clear());
    let mut environment = Environment::new();
    environment.set_syntax(
        SyntaxConfig::builder()
            .keep_trailing_newline(request.keep_trailing_newline)
            .build()?,
    );
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
    if matches!(operation, Operation::Syntax) {
        return Ok(RenderedTemplate::new(String::new(), vec![]));
    }
    let buffer = Vec::with_capacity(
        request
            .max_output_bytes
            .min(output_budget.remaining())
            .min(1024),
    );
    let mut writer = output_budget.writer(buffer, ByteLimit::new(request.max_output_bytes));
    let rendered = environment
        .get_template(&request.name)?
        .render_captured_to(context.clone(), &mut writer);
    if writer.budget().exceeded() {
        return Err(minijinja::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            if writer.parent_exceeded() {
                "template batch output limit exceeded"
            } else {
                "template output limit exceeded"
            },
        ));
    }
    rendered?;
    let output = String::from_utf8(writer.into_inner()).map_err(|_| {
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
