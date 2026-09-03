use std::collections::BTreeMap;

use hubuum_scale_core::{Result, invalid_data};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricSnapshot {
    values: BTreeMap<MetricKey, f64>,
}

impl MetricSnapshot {
    pub fn parse(text: &str) -> Result<Self> {
        let mut values = BTreeMap::new();
        for (index, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (identifier, value) = line
                .split_once(char::is_whitespace)
                .ok_or_else(|| invalid_data(format!("metrics line {} has no value", index + 1)))?;
            let value = value
                .trim()
                .parse::<f64>()
                .map_err(|error| invalid_data(format!("invalid metrics value: {error}")))?;
            let (name, labels) = parse_identifier(identifier)?;
            if values.insert(MetricKey { name, labels }, value).is_some() {
                return Err(invalid_data(format!(
                    "metrics line {} duplicates a series",
                    index + 1
                )));
            }
        }
        Ok(Self { values })
    }

    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        let labels = labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        self.values
            .get(&MetricKey {
                name: name.to_string(),
                labels,
            })
            .copied()
            .unwrap_or_default()
    }

    pub fn total_counter_delta(&self, later: &Self, name: &str) -> Result<f64> {
        let before = self
            .values
            .iter()
            .filter(|(key, _)| key.name == name)
            .map(|(_, value)| *value)
            .sum::<f64>();
        let after = later
            .values
            .iter()
            .filter(|(key, _)| key.name == name)
            .map(|(_, value)| *value)
            .sum::<f64>();
        if !before.is_finite() || !after.is_finite() || after < before {
            return Err(invalid_data(format!(
                "counter '{name}' moved backwards or was non-finite ({before} -> {after})"
            )));
        }
        Ok(after - before)
    }
}

fn parse_identifier(identifier: &str) -> Result<(String, BTreeMap<String, String>)> {
    let Some(open) = identifier.find('{') else {
        return Ok((identifier.to_string(), BTreeMap::new()));
    };
    let close = identifier
        .strip_suffix('}')
        .ok_or_else(|| invalid_data("metric labels are not closed"))?
        .len();
    Ok((
        identifier[..open].to_string(),
        parse_labels(&identifier[open + 1..close])?,
    ))
}

fn parse_labels(mut input: &str) -> Result<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    while !input.trim().is_empty() {
        input = input.trim_start();
        let equals = input
            .find('=')
            .ok_or_else(|| invalid_data("metric label has no equals sign"))?;
        let key = input[..equals].trim();
        input = &input[equals + 1..];
        let rest = input
            .strip_prefix('"')
            .ok_or_else(|| invalid_data("metric label value is not quoted"))?;
        let (value, consumed) = parse_quoted(rest)?;
        if labels.insert(key.to_string(), value).is_some() {
            return Err(invalid_data(format!("duplicate metric label '{key}'")));
        }
        input = rest[consumed..].trim_start();
        if let Some(rest) = input.strip_prefix(',') {
            input = rest;
        } else if !input.is_empty() {
            return Err(invalid_data("metric labels are not comma-separated"));
        }
    }
    Ok(labels)
}

fn parse_quoted(input: &str) -> Result<(String, usize)> {
    let mut value = String::new();
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            value.push(match character {
                'n' => '\n',
                '\\' => '\\',
                '"' => '"',
                other => other,
            });
            escaped = false;
        } else {
            match character {
                '\\' => escaped = true,
                '"' => return Ok((value, index + character.len_utf8())),
                other => value.push(other),
            }
        }
    }
    Err(invalid_data("metric label value is not closed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_sums_labeled_metrics() {
        let before = MetricSnapshot::parse("counter{kind=\"a\"} 1\ncounter{kind=\"b\"} 2").unwrap();
        let after = MetricSnapshot::parse("counter{kind=\"a\"} 3\ncounter{kind=\"b\"} 5").unwrap();

        assert_eq!(before.total_counter_delta(&after, "counter").unwrap(), 5.0);
    }
}
