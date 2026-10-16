//! Engine-independent acceptance and boolean matching cases for adapter tests.
//! Every compiler must pass this corpus; adapters should add native execution
//! and generated differential tests without weakening these expectations.

#[derive(Clone, Copy, Debug)]
pub struct Case {
    pattern: &'static str,
    text: &'static str,
    matches: bool,
}

impl Case {
    pub const fn pattern(self) -> &'static str {
        self.pattern
    }
    pub const fn text(self) -> &'static str {
        self.text
    }
    pub const fn matches(self) -> bool {
        self.matches
    }
}

macro_rules! cases {
    ($(($pattern:expr, $text:expr, $matches:expr)),* $(,)?) => {
        &[$(Case { pattern: $pattern, text: $text, matches: $matches }),*]
    };
}

pub fn cases() -> &'static [Case] {
    cases![
        ("", "router", true),
        ("out", "router", true),
        ("^router$", "a router", false),
        ("ROUTER", "router", false),
        ("(?i)^ROUTER$", "router", true),
        ("^røuter$", "røuter", true),
        ("router.$", "router\n", false),
        ("(?s)router.$", "router\n", true),
        ("^router$", "router\n", false),
        ("(?m)^router$", "switch\nrouter\nswitch", true),
        ("(?m)^router$", "router\r\n", false),
        ("(?m)^router$", "router\r", false),
        (r"\Arouter\z", "router\n", false),
        ("(?m)^$", "\n", true),
        ("(?m)^[^a]$", "\n", true),
        ("(?s:a.)b.", "a\nb\n", false),
        ("(?s:a.)b.", "a\nbc", true),
        ("(?i:a)b", "AB", false),
        ("(?i:a)b", "Ab", true),
        (r"^\p{Greek}+$", "αβ", true),
        (r"^\p{Greek}+$", "αb", false),
        (r"^\d+$", "١٢", true),
        (r"^(?-u:\d)+$", "١٢", false),
        (r"^\w+$", "røuter_1", true),
        (r"^\W+$", "!💡", true),
        (r"^\s$", "\u{a0}", true),
        (r"^[[:alpha:]]+$", "αβ", false),
        ("(?i)^k$", "K", true),
        ("(?i)^σ$", "ς", true),
        ("(?i)^i$", "İ", false),
        ("(?i)^ss$", "ß", false),
        ("^é$", "e\u{301}", false),
        ("^[a-z]+$", "é", false),
        ("^[α-ω]+$", "αω", true),
        ("^[💡-💩]$", "💧", true),
        (r"^[\x{1}-\x{10ffff}]$", "💧", true),
        (r"^[\x{1}-\x{10ffff}]$", "\u{1}", true),
        (r"^[a-z&&[^aeiou]]+$", "bcdf", true),
        (r"^[a-z&&[^aeiou]]+$", "beef", false),
        ("[a&&b]", "a", false),
        ("[^a]", "\n", true),
        (
            r"^\[a\]\(b\)\{c\}\.\*\+\?\|\^\$\\$",
            "[a](b){c}.*+?|^$\\",
            true
        ),
        (r#"^['";=-]+$"#, "';=-", true),
        ("^(foo|bar)+$", "foobarfoo", true),
        ("^(foo|bar)+$", "foobaz", false),
        ("^a{2,3}$", "aaa", true),
        ("^a{2,3}$", "aaaa", false),
        ("^a{2,}$", "aaaa", true),
        ("^a{255}$", "a", false),
        ("(?:a{62}b){62}", "a", false),
        ("^a*?$", "aaa", true),
        ("^(?:a?)*$", "aaa", true),
        ("^(?:a|)$", "", true),
        ("a{0}", "b", true),
        ("(?:^){2}a", "a", true),
        (r"(?x) ^ a [\x20] b $ # comment", "a b", true),
    ]
}

pub fn invalid_patterns() -> &'static [&'static str] {
    &[
        "[",
        "(",
        "a{256}",
        "a{1,256}",
        "a{1000000000}",
        "(?:a{256}){0}",
        r"(?:\b){0}",
        r"^(router)\1$",
        r"router(?=$)",
        r"(?<=a)b",
        r"\yrouter\y",
        r"\brouter\b",
        r"(?-u:\b)router",
        "(?Rm)^a$",
        r"(?-u:.)",
        "\0",
        r"\x00",
        r"\u0000",
    ]
}
