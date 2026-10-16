//! PostgreSQL's implementation of the portable regex compiler contract.

use std::convert::Infallible;
use std::fmt::Write;

use hubuum_regex::{Assertion, Compiler, Expression, ExpressionKind, Pattern};

pub struct PostgresRegexCompiler;

/// A native ARE pattern. Execute against UTF-8 text with `COLLATE "C"` so the
/// explicit scalar ranges retain their portable ordering and case sensitivity.
pub struct PostgresRegex {
    source: String,
}

impl PostgresRegex {
    pub fn as_str(&self) -> &str {
        &self.source
    }
    pub fn into_string(self) -> String {
        self.source
    }
}

impl Compiler for PostgresRegexCompiler {
    type Output = PostgresRegex;
    type Error = Infallible;

    fn compile(&self, pattern: &Pattern) -> Result<Self::Output, Self::Error> {
        let mut source = String::new();
        emit(pattern.expression(), &mut source);
        Ok(PostgresRegex { source })
    }
}

fn scalar(value: char, output: &mut String) {
    // Fixed-width escapes also work inside bracket expressions and cannot
    // introduce SQL, flags, ranges, backreferences, or native regex syntax.
    write!(output, "\\U{:08x}", u32::from(value)).unwrap();
}

fn emit(expression: &Expression, output: &mut String) {
    match expression.kind() {
        ExpressionKind::Empty => output.push_str("(?:)"),
        ExpressionKind::Never => output.push_str("(?!)"),
        ExpressionKind::Literal(text) => {
            for value in text.chars() {
                scalar(value, output);
            }
        }
        ExpressionKind::Class(ranges) => {
            output.push('[');
            for range in ranges {
                scalar(range.start(), output);
                if range.start() != range.end() {
                    output.push('-');
                    scalar(range.end(), output);
                }
            }
            output.push(']');
        }
        ExpressionKind::Assertion(assertion) => output.push_str(match assertion {
            Assertion::Start => r"\A",
            Assertion::End => r"\Z",
            Assertion::StartLine => r"(?:\A|(?<=\n))",
            Assertion::EndLine => r"(?:\Z|(?=\n))",
        }),
        ExpressionKind::Repetition {
            expression,
            minimum,
            maximum,
        } => {
            output.push_str("(?:");
            emit(expression, output);
            output.push(')');
            match maximum {
                Some(maximum) => write!(output, "{{{minimum},{maximum}}}").unwrap(),
                None => write!(output, "{{{minimum},}}").unwrap(),
            }
        }
        ExpressionKind::Sequence(children) | ExpressionKind::Alternation(children) => {
            output.push_str("(?:");
            for (index, child) in children.iter().enumerate() {
                if index > 0 && matches!(expression.kind(), ExpressionKind::Alternation(_)) {
                    output.push('|');
                }
                emit(child, output);
            }
            output.push(')');
        }
    }
}
