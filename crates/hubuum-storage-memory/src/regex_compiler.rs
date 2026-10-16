//! Memory's implementation of the portable regex compiler contract.

use std::fmt::{self, Write};

use hubuum_regex::{Assertion, Compiler, Expression, ExpressionKind, Pattern};
use regex::Regex;

pub struct MemoryRegexCompiler;

pub struct MemoryRegex(Regex);

impl MemoryRegex {
    pub fn is_match(&self, text: &str) -> bool {
        self.0.is_match(text)
    }
}

#[derive(Debug)]
pub struct RegexCompilationError(String);

impl fmt::Display for RegexCompilationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RegexCompilationError {}

impl Compiler for MemoryRegexCompiler {
    type Output = MemoryRegex;
    type Error = RegexCompilationError;

    fn compile(&self, pattern: &Pattern) -> Result<Self::Output, Self::Error> {
        let mut source = String::new();
        emit(pattern.expression(), &mut source);
        Regex::new(&source)
            .map(MemoryRegex)
            .map_err(|error| RegexCompilationError(error.to_string()))
    }
}

fn scalar(value: char, output: &mut String) {
    write!(output, "\\x{{{:x}}}", u32::from(value)).unwrap();
}

fn emit(expression: &Expression, output: &mut String) {
    match expression.kind() {
        ExpressionKind::Empty => output.push_str("(?:)"),
        ExpressionKind::Never => output.push_str("[a&&b]"),
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
            Assertion::End => r"\z",
            Assertion::StartLine => "(?m:^)",
            Assertion::EndLine => "(?m:$)",
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

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_regex::conformance;

    #[test]
    fn portable_regex_conformance() {
        for case in conformance::cases() {
            let pattern = Pattern::new(case.pattern()).unwrap();
            let regex = MemoryRegexCompiler.compile(&pattern).unwrap();
            assert_eq!(regex.is_match(case.text()), case.matches(), "{case:?}");
        }
    }
}
