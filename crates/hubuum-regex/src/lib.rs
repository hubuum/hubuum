//! Bounded regular expressions with one meaning for every storage backend.
//!
//! Parsing resolves flags, Unicode properties and case folding into explicit
//! scalar ranges. Adapters compile this immutable representation using their
//! own engines. This crate has no storage, SQL, runtime or matching dependency.

pub mod conformance;

use std::fmt;
use std::sync::Arc;

use regex_syntax::ast::parse::ParserBuilder;
use regex_syntax::ast::{self, AssertionKind, Ast, Flag, RepetitionKind, RepetitionRange, Visitor};
use regex_syntax::hir::translate::TranslatorBuilder;
use regex_syntax::hir::{Class, Hir, HirKind, Look};

/// Maximum UTF-8 bytes in a source pattern.
pub const MAX_PATTERN_BYTES: usize = 1_024;
/// Maximum syntactic nesting depth.
pub const MAX_NESTING_DEPTH: u32 = 16;
/// Largest explicit repetition bound.
pub const MAX_REPETITION: u32 = 255;
/// Maximum expanded expression cost, including character ranges and repetitions.
pub const MAX_COMPLEXITY: usize = 4_096;

/// A validated expression. Construction is the only fallible language boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pattern {
    expression: Arc<Expression>,
}

impl Pattern {
    pub fn new(source: &str) -> Result<Self, Error> {
        if source.len() > MAX_PATTERN_BYTES {
            return Err(Error::new(format!(
                "pattern exceeds {MAX_PATTERN_BYTES} bytes"
            )));
        }
        let ast = ParserBuilder::new()
            .nest_limit(MAX_NESTING_DEPTH)
            .build()
            .parse(source)
            .map_err(|error| Error::new(error.to_string()))?;
        ast::visit(&ast, LanguageValidator)?;
        let hir = TranslatorBuilder::new()
            .build()
            .translate(source, &ast)
            .map_err(|error| Error::new(error.to_string()))?;
        let (expression, _) = normalize(&hir)?;
        Ok(Self {
            expression: Arc::new(expression),
        })
    }

    pub fn expression(&self) -> &Expression {
        &self.expression
    }
}

/// Adapter-owned compilation. Implementations must preserve the entire language.
///
/// The output may be a native pattern, executable matcher, or another private
/// program. Compilation never authorizes fetching rows for client-side filtering.
pub trait Compiler {
    type Output;
    type Error;

    fn compile(&self, pattern: &Pattern) -> Result<Self::Output, Self::Error>;
}

/// An immutable node of a validated pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expression {
    kind: ExpressionKind,
}

impl Expression {
    pub fn kind(&self) -> &ExpressionKind {
        &self.kind
    }
}

/// Closed, engine-independent vocabulary. Captures and matching preference do
/// not survive normalization: this contract exposes only boolean search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpressionKind {
    Empty,
    Never,
    Literal(String),
    Class(Vec<CharacterRange>),
    Assertion(Assertion),
    Repetition {
        expression: Box<Expression>,
        minimum: u32,
        maximum: Option<u32>,
    },
    Sequence(Vec<Expression>),
    Alternation(Vec<Expression>),
}

/// Inclusive Unicode scalar range. NUL and surrogate code points are excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CharacterRange {
    start: char,
    end: char,
}

impl CharacterRange {
    pub const fn start(self) -> char {
        self.start
    }

    pub const fn end(self) -> char {
        self.end
    }
}

/// Line anchors recognize LF only. Text anchors never ignore a final newline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Assertion {
    Start,
    End,
    StartLine,
    EndLine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid portable regular expression: {}", self.message)
    }
}

impl std::error::Error for Error {}

struct LanguageValidator;

impl Visitor for LanguageValidator {
    type Output = ();
    type Err = Error;

    fn finish(self) -> Result<(), Error> {
        Ok(())
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Error> {
        match ast {
            Ast::Assertion(assertion)
                if !matches!(
                    assertion.kind,
                    AssertionKind::StartLine
                        | AssertionKind::EndLine
                        | AssertionKind::StartText
                        | AssertionKind::EndText
                ) =>
            {
                return Err(Error::new("word boundaries are not supported"));
            }
            Ast::Literal(literal) if literal.c == '\0' => {
                return Err(Error::new("NUL literals are not supported"));
            }
            Ast::Repetition(repetition) => {
                let largest = match repetition.op.kind {
                    RepetitionKind::Range(
                        RepetitionRange::Exactly(n) | RepetitionRange::AtLeast(n),
                    ) => n,
                    RepetitionKind::Range(RepetitionRange::Bounded(_, n)) => n,
                    _ => 1,
                };
                if largest > MAX_REPETITION {
                    return Err(Error::new(format!(
                        "repetition bounds must not exceed {MAX_REPETITION}"
                    )));
                }
            }
            _ => {}
        }
        let flags = match ast {
            Ast::Flags(flags) => Some(&flags.flags),
            Ast::Group(group) => group.flags(),
            _ => None,
        };
        if flags.is_some_and(|flags| flags.flag_state(Flag::CRLF) == Some(true)) {
            return Err(Error::new("CRLF anchor mode is not supported"));
        }
        Ok(())
    }
}

fn normalize(hir: &Hir) -> Result<(Expression, usize), Error> {
    let (kind, cost) = match hir.kind() {
        HirKind::Empty => (ExpressionKind::Empty, 1),
        HirKind::Literal(literal) => {
            let text = std::str::from_utf8(&literal.0)
                .map_err(|_| Error::new("byte patterns are not supported"))?;
            if text.contains('\0') {
                return Err(Error::new("NUL literals are not supported"));
            }
            (
                ExpressionKind::Literal(text.to_owned()),
                text.chars().count(),
            )
        }
        HirKind::Class(class) => {
            let mut ranges = Vec::new();
            match class {
                Class::Unicode(class) => {
                    for range in class.ranges() {
                        push_range(&mut ranges, range.start(), range.end());
                    }
                }
                Class::Bytes(class) => {
                    for range in class.ranges() {
                        if !range.end().is_ascii() {
                            return Err(Error::new("byte patterns are not supported"));
                        }
                        push_range(
                            &mut ranges,
                            char::from(range.start()),
                            char::from(range.end()),
                        );
                    }
                }
            }
            if ranges.is_empty() {
                (ExpressionKind::Never, 1)
            } else {
                let cost = ranges.len();
                (ExpressionKind::Class(ranges), cost)
            }
        }
        HirKind::Look(look) => {
            let assertion = match look {
                Look::Start => Assertion::Start,
                Look::End => Assertion::End,
                Look::StartLF => Assertion::StartLine,
                Look::EndLF => Assertion::EndLine,
                _ => {
                    return Err(Error::new(
                        "word boundaries and CRLF anchors are not supported",
                    ));
                }
            };
            (ExpressionKind::Assertion(assertion), 1)
        }
        HirKind::Capture(capture) => return normalize(&capture.sub),
        HirKind::Repetition(repetition) => {
            if repetition.min > MAX_REPETITION
                || repetition.max.is_some_and(|max| max > MAX_REPETITION)
            {
                return Err(Error::new(format!(
                    "repetition bounds must not exceed {MAX_REPETITION}"
                )));
            }
            let (expression, cost) = normalize(&repetition.sub)?;
            let copies = repetition.max.unwrap_or(repetition.min + 1).max(1) as usize;
            (
                ExpressionKind::Repetition {
                    expression: Box::new(expression),
                    minimum: repetition.min,
                    maximum: repetition.max,
                },
                cost.saturating_mul(copies).saturating_add(1),
            )
        }
        HirKind::Concat(children) | HirKind::Alternation(children) => {
            let mut expressions = Vec::with_capacity(children.len());
            let mut cost = 1usize;
            for child in children {
                let (expression, child_cost) = normalize(child)?;
                cost = cost.saturating_add(child_cost);
                check_cost(cost)?;
                expressions.push(expression);
            }
            let kind = if matches!(hir.kind(), HirKind::Concat(_)) {
                ExpressionKind::Sequence(expressions)
            } else {
                ExpressionKind::Alternation(expressions)
            };
            (kind, cost)
        }
    };
    check_cost(cost)?;
    Ok((Expression { kind }, cost))
}

fn check_cost(cost: usize) -> Result<(), Error> {
    if cost > MAX_COMPLEXITY {
        Err(Error::new(format!(
            "expanded complexity exceeds {MAX_COMPLEXITY}"
        )))
    } else {
        Ok(())
    }
}

fn push_range(ranges: &mut Vec<CharacterRange>, start: char, end: char) {
    // Stored text is a sequence of non-NUL Unicode scalars. Splitting around
    // surrogates makes that domain explicit even for engines with other ranges.
    if end == '\0' {
        return;
    }
    let start = start.max('\u{1}');
    if start <= '\u{d7ff}' && end >= '\u{e000}' {
        ranges.push(CharacterRange {
            start,
            end: '\u{d7ff}',
        });
        ranges.push(CharacterRange {
            start: '\u{e000}',
            end,
        });
    } else {
        ranges.push(CharacterRange { start, end });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn conformance_patterns_are_valid() {
        for case in conformance::cases() {
            Pattern::new(case.pattern())
                .unwrap_or_else(|error| panic!("{}: {error}", case.pattern()));
        }
    }

    #[test]
    fn unsupported_patterns_are_rejected() {
        for pattern in conformance::invalid_patterns() {
            assert!(Pattern::new(pattern).is_err(), "{pattern}");
        }
    }

    #[rstest]
    #[case::bytes("a".repeat(MAX_PATTERN_BYTES + 1))]
    #[case::depth(format!("{}a{}", "(".repeat(32), ")".repeat(32)))]
    #[case::expansion("(?:a{200}b){200}".to_string())]
    fn resource_limits_are_enforced(#[case] source: String) {
        assert!(Pattern::new(&source).is_err());
    }
}
