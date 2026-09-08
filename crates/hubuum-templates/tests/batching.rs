use hubuum_templates::{
    MissingDataPolicy, TemplateAutoEscape, TemplateBatch, TemplateExecution, TemplateLimits,
};
use rstest::rstest;
use serde_json::json;

fn template<'a>(name: &'a str, source: &'a str) -> TemplateExecution<'a> {
    TemplateExecution::new(name, source, TemplateLimits::new(64, 50_000))
}

#[tokio::test]
async fn entries_keep_their_own_include_sources_and_rendering_settings() {
    let first = vec![("fragment".into(), "{{ value }}\n".into())];
    let second = vec![("fragment".into(), "second:{{ value }}\n".into())];
    let mut batch = TemplateBatch::new(128);
    batch
        .push(
            template("root", "{% include 'fragment' %}")
                .sources(&first)
                .auto_escape(TemplateAutoEscape::Html),
        )
        .unwrap();
    batch
        .push(
            template("root", "{% include 'fragment' %}")
                .sources(&second)
                .keep_trailing_newline(false),
        )
        .unwrap();

    let outputs = batch.render(&json!({ "value": "<tag>" })).await.unwrap();

    assert_eq!(
        outputs
            .into_iter()
            .map(|item| item.into_parts().0)
            .collect::<Vec<_>>(),
        ["&lt;tag&gt;\n", "second:<tag>"]
    );
}

#[tokio::test]
async fn a_large_shared_context_is_encoded_only_once() {
    let context = json!({ "payload": "x".repeat(9 * 1024 * 1024) });
    let mut batch = TemplateBatch::new(128);
    for _ in 0..2 {
        batch
            .push(template("length", "{{ payload | length }}"))
            .unwrap();
    }

    let outputs = batch.render(&context).await.unwrap();

    assert_eq!(
        outputs
            .into_iter()
            .map(|item| item.into_parts().0)
            .collect::<Vec<_>>(),
        ["9437184", "9437184"]
    );
}

#[rstest]
#[case::individual(128, 2, "template output limit")]
#[case::aggregate(5, 128, "template batch output limit")]
#[tokio::test]
async fn output_limits_reject_the_whole_batch_at_the_failing_entry(
    #[case] total_limit: usize,
    #[case] entry_limit: usize,
    #[case] message: &str,
) {
    let mut batch = TemplateBatch::new(total_limit);
    batch.push(template("first", "abc")).unwrap();
    batch
        .push(template("second", "éé").max_output_bytes(entry_limit))
        .unwrap();

    let error = batch
        .render(&json!({}))
        .await
        .err()
        .expect("batch must fail");

    assert!(error.to_string().contains(message), "{error}");
    assert_eq!(error.template_index(), Some(1));
}

#[tokio::test]
async fn syntax_batches_compile_without_executing_and_identify_invalid_entries() {
    let mut batch = TemplateBatch::new(0);
    batch.push(template("first", "{{ 1 / 0 }}")).unwrap();
    batch.push(template("second", "{% if %}")).unwrap();

    let error = batch.validate().await.unwrap_err();

    assert_eq!(error.template_index(), Some(1));
}

#[tokio::test]
async fn every_entry_receives_its_own_fuel_allowance() {
    let mut batch = TemplateBatch::new(128);
    for _ in 0..2 {
        batch
            .push(TemplateExecution::new(
                "loop",
                "{% for i in range(10) %}{{ i }}{% endfor %}",
                TemplateLimits::new(64, 100),
            ))
            .unwrap();
    }

    let outputs = batch.render(&json!({})).await.unwrap();

    assert_eq!(
        outputs
            .into_iter()
            .map(|item| item.into_parts().0)
            .collect::<Vec<_>>(),
        ["0123456789", "0123456789"]
    );
}

#[tokio::test]
async fn missing_value_warnings_do_not_leak_into_later_entries() {
    let mut batch = TemplateBatch::new(128);
    batch
        .push(template("same", "{{ absent }}").missing_data(MissingDataPolicy::Omit))
        .unwrap();
    batch
        .push(template("same", "healthy").missing_data(MissingDataPolicy::Omit))
        .unwrap();
    let mut outputs = batch.render(&json!({})).await.unwrap().into_iter();
    assert!(!outputs.next().unwrap().into_parts().1.is_empty());

    assert!(outputs.next().unwrap().into_parts().1.is_empty());
}

#[tokio::test]
async fn a_lenient_entry_does_not_relax_a_later_strict_entry() {
    let mut batch = TemplateBatch::new(128);
    batch
        .push(template("same", "{{ absent }}").missing_data(MissingDataPolicy::Lenient))
        .unwrap();
    batch.push(template("same", "{{ absent }}")).unwrap();

    let error = batch
        .render(&json!({}))
        .await
        .err()
        .expect("strict entry must fail");

    assert_eq!(error.template_index(), Some(1));
}

#[tokio::test]
async fn local_variables_do_not_leak_into_later_entries() {
    let mut batch = TemplateBatch::new(128);
    batch
        .push(template("same", "{% set shared = 'private' %}first"))
        .unwrap();
    batch
        .push(template("same", "{{ shared | default('clean') }}"))
        .unwrap();

    let output = batch.render(&json!({})).await.unwrap().pop().unwrap();

    assert_eq!(output.into_parts().0, "clean");
}

#[rstest]
#[case::maximum(130, true)]
#[case::excessive(131, false)]
#[tokio::test]
async fn batch_entry_count_is_bounded(#[case] count: usize, #[case] accepted: bool) {
    let mut batch = TemplateBatch::new(128);
    let result: Result<(), _> = (0..count).try_for_each(|_| batch.push(template("entry", "x")));
    let result = match result {
        Ok(()) => batch.validate().await,
        Err(error) => Err(error),
    };

    assert_eq!(result.is_ok(), accepted);
}

#[test]
fn the_source_limit_covers_all_batch_entries() {
    let source = "x".repeat(9 * 1024 * 1024);
    let mut batch = TemplateBatch::new(128);
    batch.push(template("first", &source)).unwrap();

    let error = batch.push(template("second", &source)).unwrap_err();

    assert!(error.to_string().contains("input budget"));
}

#[tokio::test]
async fn the_encoded_input_limit_includes_the_shared_context() {
    let mut batch = TemplateBatch::new(128);
    batch.push(template("first", "hello")).unwrap();
    let context = json!({ "large": "x".repeat(16 * 1024 * 1024) });

    let error = batch
        .render(&context)
        .await
        .err()
        .expect("input must be bounded");

    assert!(error.to_string().contains("input budget"));
}

#[tokio::test]
async fn empty_batches_are_rejected() {
    let error = TemplateBatch::new(128)
        .render(&json!({}))
        .await
        .err()
        .unwrap();

    assert!(error.to_string().contains("must not be empty"));
}

#[tokio::test]
async fn heap_failure_in_a_later_entry_is_contained_and_allows_recovery() {
    let mut batch = TemplateBatch::new(128);
    batch.push(template("first", "healthy")).unwrap();
    batch
        .push(template(
            "second",
            "{% set a = 'x' * 60000000 %}{% set b = a ~ a %}{{ b | length }}",
        ))
        .unwrap();
    let error = batch
        .render(&json!({}))
        .await
        .err()
        .expect("heap budget must fail");
    assert!(error.to_string().contains("heap budget"), "{error}");

    let output = template("recovery", "healthy")
        .render(&json!({}))
        .await
        .unwrap();

    assert_eq!(output.into_parts().0, "healthy");
}
