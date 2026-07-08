#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocExampleBlock {
    pub label: String,
    pub language: String,
    pub body: String,
}

pub fn extract_labeled_blocks(markdown: &str) -> Result<Vec<DocExampleBlock>, String> {
    let mut blocks = Vec::new();
    let mut pending_label = None;
    let mut lines = markdown.lines();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if let Some(label) = trimmed
            .strip_prefix("<!-- doc-example:")
            .and_then(|rest| rest.strip_suffix("-->"))
        {
            let label = label.trim();
            if label.is_empty() {
                return Err("doc-example label must not be empty".to_string());
            }
            if pending_label.replace(label.to_string()).is_some() {
                return Err(format!(
                    "doc-example label {label:?} appeared before the previous label had a block"
                ));
            }
            continue;
        }

        if let Some(fence_info) = trimmed.strip_prefix("```") {
            let Some(label) = pending_label.take() else {
                continue;
            };
            let language = fence_info
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string();
            if language.is_empty() {
                return Err(format!(
                    "doc-example block {label:?} must declare a fenced code language"
                ));
            }

            let mut body = String::new();
            let mut closed = false;
            for block_line in lines.by_ref() {
                if block_line.trim() == "```" {
                    closed = true;
                    break;
                }
                body.push_str(block_line);
                body.push('\n');
            }
            if !closed {
                return Err(format!(
                    "doc-example block {label:?} is missing a closing fence"
                ));
            }
            blocks.push(DocExampleBlock {
                label,
                language,
                body,
            });
        }
    }

    if let Some(label) = pending_label {
        return Err(format!(
            "doc-example label {label:?} was not followed by a fenced code block"
        ));
    }

    Ok(blocks)
}

pub fn required_labeled_block(markdown: &str, label: &str) -> Result<DocExampleBlock, String> {
    extract_labeled_blocks(markdown)?
        .into_iter()
        .find(|block| block.label == label)
        .ok_or_else(|| format!("missing doc-example block {label:?}"))
}

#[cfg(test)]
mod tests {
    use super::{extract_labeled_blocks, required_labeled_block};

    #[test]
    fn extracts_explicitly_labeled_fenced_blocks() {
        let markdown = r#"
Before

<!-- doc-example: guide/example/template -->
```text
hello
```

```text
ignored
```
"#;

        let blocks = extract_labeled_blocks(markdown).unwrap();

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].label, "guide/example/template");
        assert_eq!(blocks[0].language, "text");
        assert_eq!(blocks[0].body, "hello\n");
    }

    #[test]
    fn exports_missing_required_blocks_clearly() {
        let error = required_labeled_block("", "guide/missing").unwrap_err();

        assert!(error.contains("guide/missing"));
    }
}
